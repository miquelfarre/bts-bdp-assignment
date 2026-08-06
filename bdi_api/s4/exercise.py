import json
import os
from typing import Annotated

import boto3
import duckdb
import requests
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


def _generate_file_names() -> list[str]:
    """Generate all possible file names for a day in ascending order."""
    files = []
    for h in range(24):
        for m in range(60):
            for s in range(0, 60, 5):
                files.append(f"{h:02d}{m:02d}{s:02d}Z.json.gz")
    return files


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    s3 = boto3.client("s3")

    # Clean existing S3 data
    try:
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
        if "Contents" in response:
            for obj in response["Contents"]:
                s3.delete_object(Bucket=s3_bucket, Key=obj["Key"])
    except Exception:
        pass

    file_names = _generate_file_names()

    for fname in file_names[:file_limit]:
        url = base_url + fname
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            s3_key = s3_prefix_path + fname
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=resp.content)

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s1.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    prepared_dir = settings.prepared_dir
    db_path = os.path.join(prepared_dir, "aircraft.duckdb")

    os.makedirs(prepared_dir, exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)

    s3 = boto3.client("s3")

    # List and download all files from S3
    records = []
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
    if "Contents" not in response:
        return "OK"

    for obj in sorted(response["Contents"], key=lambda x: x["Key"]):
        s3_obj = s3.get_object(Bucket=s3_bucket, Key=obj["Key"])
        content = s3_obj["Body"].read()
        data = json.loads(content)

        timestamp = data.get("now", 0)
        for ac in data.get("aircraft", []):
            if "lat" not in ac or "lon" not in ac:
                continue
            records.append({
                "icao": ac.get("hex", ""),
                "registration": ac.get("r"),
                "type": ac.get("t"),
                "lat": ac["lat"],
                "lon": ac["lon"],
                "alt_baro": ac.get("alt_baro") if isinstance(ac.get("alt_baro"), (int, float)) else None,
                "ground_speed": ac.get("gs"),
                "timestamp": timestamp,
                "emergency": ac.get("emergency", "none"),
            })

    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS aircraft_positions (
            icao VARCHAR,
            registration VARCHAR,
            type VARCHAR,
            lat DOUBLE,
            lon DOUBLE,
            alt_baro DOUBLE,
            ground_speed DOUBLE,
            timestamp DOUBLE,
            emergency VARCHAR
        )
    """)

    if records:
        con.executemany(
            "INSERT INTO aircraft_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r["icao"], r["registration"], r["type"],
                    r["lat"], r["lon"], r["alt_baro"],
                    r["ground_speed"], r["timestamp"], r["emergency"],
                )
                for r in records
            ],
        )

    con.close()
    return "OK"
