import json
import os
import shutil
from typing import Annotated

import duckdb
import requests
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)

DB_PATH = os.path.join(settings.prepared_dir, "aircraft.duckdb")


def _generate_file_names() -> list[str]:
    """Generate all possible file names for a day in ascending order."""
    files = []
    for h in range(24):
        for m in range(60):
            for s in range(0, 60, 5):
                files.append(f"{h:02d}{m:02d}{s:02d}Z.json.gz")
    return files


@s1.post("/aircraft/download")
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
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to add it to `requirements.txt`
    so it can be installed using `pip install -r requirements.txt`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    # Clean download folder
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)

    file_names = _generate_file_names()

    for fname in file_names[:file_limit]:
        url = base_url + fname
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            file_path = os.path.join(download_dir, fname)
            with open(file_path, "wb") as f:
                f.write(resp.content)

    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to add it to `requirements.txt`
    so it can be installed using `pip install -r requirements.txt`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    os.makedirs(settings.prepared_dir, exist_ok=True)

    # Clean prepared folder
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    records = []
    for fname in sorted(os.listdir(raw_dir)):
        fpath = os.path.join(raw_dir, fname)
        with open(fpath) as f:
            data = json.load(f)

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

    con = duckdb.connect(DB_PATH)
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


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    offset = page * num_results
    result = con.execute(
        """
        SELECT DISTINCT icao, registration, type
        FROM aircraft_positions
        ORDER BY icao ASC
        LIMIT ? OFFSET ?
        """,
        [num_results, offset],
    ).fetchall()
    con.close()

    return [{"icao": r[0], "registration": r[1], "type": r[2]} for r in result]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    offset = page * num_results
    result = con.execute(
        """
        SELECT timestamp, lat, lon
        FROM aircraft_positions
        WHERE icao = ?
        ORDER BY timestamp ASC
        LIMIT ? OFFSET ?
        """,
        [icao, num_results, offset],
    ).fetchall()
    con.close()

    return [{"timestamp": r[0], "lat": r[1], "lon": r[2]} for r in result]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    result = con.execute(
        """
        SELECT
            MAX(alt_baro) as max_alt,
            MAX(ground_speed) as max_gs,
            BOOL_OR(emergency IS NOT NULL AND emergency != 'none') as had_emergency
        FROM aircraft_positions
        WHERE icao = ?
        """,
        [icao],
    ).fetchone()
    con.close()

    return {
        "max_altitude_baro": result[0] if result else None,
        "max_ground_speed": result[1] if result else None,
        "had_emergency": bool(result[2]) if result else False,
    }
