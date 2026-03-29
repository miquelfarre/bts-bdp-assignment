"""
Aircraft Data Pipeline DAG

Downloads aircraft tracking data from ADS-B Exchange, enriches it with aircraft
metadata, and loads it into a SQLite database accessible by the FastAPI app.

Pipeline stages:
  1. download_to_bronze  — fetch JSON files → MinIO bronze bucket
  2. process_to_silver   — parse + enrich → MinIO silver bucket (Parquet)
  3. load_to_database    — silver Parquet → SQLite (shared with the API)
"""

import io
import json
import logging
import os
import sqlite3
from datetime import datetime

import boto3
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — all tuneable via environment variables so the container can
# override them without changing the DAG source.
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

SOURCE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
AIRCRAFT_DB_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
FUEL_URL = (
    "https://raw.githubusercontent.com/martsec/flight_co2_analysis/"
    "main/data/aircraft_type_fuel_consumption_rates.json"
)

DAY = "2023-11-01"
# Number of hourly snapshot files to download (max 24 for the full day).
# Keeping this small speeds up the pipeline; increase for more coverage.
FILE_LIMIT = int(os.getenv("BDI_S8_FILE_LIMIT", "5"))

# The SQLite database path *inside the Airflow container*.
# Mount your host data directory to this path so the FastAPI app can read it:
#   volumes:
#     - ./data:/opt/airflow/data
# and set BDI_LOCAL_DIR=data (relative to the project root) on the API side.
DB_PATH = os.getenv("BDI_S8_DB_PATH", "/opt/airflow/data/s8/aircraft.db")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _ensure_buckets(s3_client):
    for bucket in (BRONZE_BUCKET, SILVER_BUCKET):
        try:
            s3_client.head_bucket(Bucket=bucket)
        except Exception:
            s3_client.create_bucket(Bucket=bucket)


# ---------------------------------------------------------------------------
# Task 1 — Download raw JSON files to MinIO bronze
# ---------------------------------------------------------------------------

def download_to_bronze(**_):
    s3 = _s3()
    _ensure_buckets(s3)

    filenames = [f"{hour:02d}0000Z.json.gz" for hour in range(24)]
    downloaded = 0

    for filename in filenames:
        if downloaded >= FILE_LIMIT:
            break
        url = f"{SOURCE_URL}{filename}"
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code != 200:
                continue
            key = f"readsb-hist/2023/11/01/{filename}"
            s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=resp.content)
            log.info("Stored %s → bronze/%s", filename, key)
            downloaded += 1
        except Exception as exc:
            log.warning("Failed to download %s: %s", filename, exc)

    log.info("Downloaded %d file(s) to bronze", downloaded)


# ---------------------------------------------------------------------------
# Task 2 — Parse bronze files, enrich, store Parquet to MinIO silver
# ---------------------------------------------------------------------------

def process_to_silver(**_):  # noqa: C901
    s3 = _s3()

    # ------------------------------------------------------------------
    # 2a. Read all bronze files
    # ------------------------------------------------------------------
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="readsb-hist/2023/11/01/")
    objects = [o for o in resp.get("Contents", []) if o["Key"].endswith(".json.gz")]

    if not objects:
        log.warning("No bronze files found — skipping silver processing")
        return

    aircraft_rows = []
    obs_rows = []

    for obj in objects:
        raw = s3.get_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])["Body"].read()

        # The files use .json.gz extension but are stored uncompressed; try both.
        try:
            content = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            import gzip
            content = json.loads(gzip.decompress(raw))

        for ac in content.get("aircraft", []):
            icao = (ac.get("hex") or "").lower().strip()
            if not icao:
                continue
            aircraft_rows.append(
                {
                    "icao": icao,
                    "registration": ac.get("r") or None,
                    "type": ac.get("t") or None,
                }
            )
            obs_rows.append({"icao": icao, "day": DAY})

    if not aircraft_rows:
        log.warning("No aircraft records found in bronze files")
        return

    # ------------------------------------------------------------------
    # 2b. Deduplicate aircraft
    # ------------------------------------------------------------------
    df_ac = (
        pd.DataFrame(aircraft_rows)
        .groupby("icao", as_index=False)
        .agg({"registration": "first", "type": "first"})
    )

    # ------------------------------------------------------------------
    # 2c. Observation counts per (icao, day)
    # ------------------------------------------------------------------
    df_obs = (
        pd.DataFrame(obs_rows)
        .groupby(["icao", "day"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )

    # ------------------------------------------------------------------
    # 2d. Enrich with aircraft database (OpenSky)
    # ------------------------------------------------------------------
    try:
        db_resp = requests.get(AIRCRAFT_DB_URL, timeout=120)
        db_resp.raise_for_status()
        df_db = pd.read_csv(
            io.StringIO(db_resp.text),
            dtype=str,
            low_memory=False,
            on_bad_lines="skip",
        )
        df_db.columns = df_db.columns.str.strip().str.lower()
        df_db["icao24"] = df_db["icao24"].str.lower().str.strip()

        # Identify available columns
        col_map = {}
        for candidate in ("manufacturername", "manufacturericao"):
            if candidate in df_db.columns:
                col_map[candidate] = "manufacturer"
                break
        if "model" in df_db.columns:
            col_map["model"] = "model"
        if "owner" in df_db.columns:
            col_map["owner"] = "owner"
        elif "operatorcallsign" in df_db.columns:
            col_map["operatorcallsign"] = "owner"

        select_cols = ["icao24"] + list(col_map.keys())
        df_merge = df_db[select_cols].rename(columns={**col_map, "icao24": "icao"})
        df_ac = df_ac.merge(df_merge, on="icao", how="left")
    except Exception as exc:
        log.warning("Could not enrich with aircraft DB: %s", exc)
        for col in ("owner", "manufacturer", "model"):
            if col not in df_ac.columns:
                df_ac[col] = None

    # Ensure required columns exist
    for col in ("registration", "type", "owner", "manufacturer", "model"):
        if col not in df_ac.columns:
            df_ac[col] = None

    # ------------------------------------------------------------------
    # 2e. Write Parquet files to silver bucket
    # ------------------------------------------------------------------
    def _upload_parquet(df: pd.DataFrame, key: str):
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=buf.getvalue())
        log.info("Uploaded %d rows → silver/%s", len(df), key)

    _upload_parquet(df_ac, "aircraft/day=20231101/aircraft.parquet")
    _upload_parquet(df_obs, "observations/day=20231101/observations.parquet")


# ---------------------------------------------------------------------------
# Task 3 — Load silver Parquet into SQLite
# ---------------------------------------------------------------------------

def load_to_database(**_):
    s3 = _s3()

    # ------------------------------------------------------------------
    # 3a. Read aircraft Parquet from silver
    # ------------------------------------------------------------------
    try:
        data = s3.get_object(
            Bucket=SILVER_BUCKET, Key="aircraft/day=20231101/aircraft.parquet"
        )["Body"].read()
        df_ac = pd.read_parquet(io.BytesIO(data))
    except Exception as exc:
        log.error("Cannot read aircraft parquet from silver: %s", exc)
        return

    # ------------------------------------------------------------------
    # 3b. Read observations Parquet from silver
    # ------------------------------------------------------------------
    try:
        data = s3.get_object(
            Bucket=SILVER_BUCKET, Key="observations/day=20231101/observations.parquet"
        )["Body"].read()
        df_obs = pd.read_parquet(io.BytesIO(data))
    except Exception as exc:
        log.warning("Cannot read observations parquet: %s — writing empty table", exc)
        df_obs = pd.DataFrame(columns=["icao", "day", "count"])

    # ------------------------------------------------------------------
    # 3c. Write to SQLite
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        df_ac.to_sql("aircraft", conn, if_exists="replace", index=False)
        df_obs.to_sql("observations", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_aircraft_icao ON aircraft(icao)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_obs_icao_day ON observations(icao, day)")
        conn.commit()
        log.info(
            "Loaded %d aircraft and %d observations into %s",
            len(df_ac),
            len(df_obs),
            DB_PATH,
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="aircraft_pipeline",
    description="Download ADS-B data, enrich, and load into DB",
    schedule_interval=None,  # triggered manually or via external trigger
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=["bdi", "s8"],
) as dag:

    t1 = PythonOperator(
        task_id="download_to_bronze",
        python_callable=download_to_bronze,
    )

    t2 = PythonOperator(
        task_id="process_to_silver",
        python_callable=process_to_silver,
    )

    t3 = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
    )

    t1 >> t2 >> t3
