import os
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

    # Create download directory
    os.makedirs(download_dir, exist_ok=True)

    # Clean existing files
    for filename in os.listdir(download_dir):
        file_path = os.path.join(download_dir, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except (PermissionError, OSError):
                pass

    # Generate list of expected filenames (hourly files for 2023-11-01)
    # Format: 000000Z.json.gz, 010000Z.json.gz, etc.
    filenames = [f"{hour:02d}0000Z.json.gz" for hour in range(24)]

    # Download files up to limit
    files_downloaded = 0
    for filename in filenames:
        if files_downloaded >= file_limit:
            break

        file_url = f"{settings.source_url}/2023/11/01/{filename}"

        try:
            response = requests.get(file_url, timeout=30)
            if response.status_code == 200:
                file_path = os.path.join(download_dir, filename)
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                files_downloaded += 1
        except Exception:
            # Skip files that don't exist or can't be downloaded
            continue

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
    prepared_dir = settings.prepared_dir

    # Create prepared directory
    os.makedirs(prepared_dir, exist_ok=True)

    # Database path
    db_path = os.path.join(prepared_dir, "aircraft.db")

    # Remove existing database
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except (PermissionError, OSError):
            pass

    # Process files
    if not os.path.exists(raw_dir):
        return "OK"

    # Get list of JSON files
    json_files = [os.path.join(raw_dir, f) for f in sorted(os.listdir(raw_dir)) if f.endswith('.json.gz')]

    if not json_files:
        return "OK"

    # Connect to database
    conn = duckdb.connect(db_path)

    try:
        # Create a glob pattern for all JSON files
        file_pattern = os.path.join(raw_dir, "*.json.gz").replace('\\', '/')

        # Read all JSON files directly with DuckDB and unnest aircraft array
        # Use struct_extract to handle fields
        conn.execute("""
            CREATE TEMP TABLE raw_data_expanded AS
            SELECT
                json.now as doc_timestamp,
                struct_extract(aircraft, 'hex') as hex,
                struct_extract(aircraft, 'r') as r,
                struct_extract(aircraft, 't') as t,
                struct_extract(aircraft, 'lat') as lat,
                struct_extract(aircraft, 'lon') as lon,
                struct_extract(aircraft, 'alt_baro') as alt_baro,
                struct_extract(aircraft, 'gs') as gs,
                CAST(NULL AS VARCHAR) as emergency
            FROM (
                SELECT json, unnest(json.aircraft) as aircraft
                FROM read_json(?, format='auto', compression='uncompressed', maximum_object_size=10000000) as json
            )
        """, [file_pattern])

        # Create aircraft table with unique aircraft
        conn.execute("""
            CREATE TABLE aircraft AS
            SELECT
                hex as icao,
                MAX(r) FILTER (WHERE r IS NOT NULL) as registration,
                MAX(t) FILTER (WHERE t IS NOT NULL) as type
            FROM raw_data_expanded
            WHERE hex IS NOT NULL
            GROUP BY hex
            ORDER BY hex
        """)

        # Create positions table
        # TRY_CAST will automatically return NULL for "ground" and other non-numeric values
        conn.execute("""
            CREATE TABLE positions AS
            SELECT
                hex as icao,
                doc_timestamp as timestamp,
                lat,
                lon,
                TRY_CAST(alt_baro AS INTEGER) as altitude_baro,
                gs as ground_speed,
                emergency
            FROM raw_data_expanded
            WHERE hex IS NOT NULL
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND doc_timestamp IS NOT NULL
            ORDER BY hex, doc_timestamp
        """)

        # Create indexes for fast queries
        conn.execute("CREATE INDEX idx_aircraft_icao ON aircraft(icao)")
        conn.execute("CREATE INDEX idx_positions_icao ON positions(icao)")
        conn.execute("CREATE INDEX idx_positions_timestamp ON positions(timestamp)")

    except Exception:
        conn.close()
        raise

    conn.close()
    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    db_path = os.path.join(settings.prepared_dir, "aircraft.db")

    if not os.path.exists(db_path):
        return []

    try:
        conn = duckdb.connect(db_path, read_only=True)
        offset = page * num_results

        result = conn.execute("""
            SELECT icao, registration, type
            FROM aircraft
            WHERE icao IS NOT NULL
            ORDER BY icao ASC
            LIMIT ? OFFSET ?
        """, [num_results, offset]).fetchall()

        conn.close()

        return [
            {"icao": row[0], "registration": row[1], "type": row[2]}
            for row in result
        ]
    except Exception:
        return []


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    db_path = os.path.join(settings.prepared_dir, "aircraft.db")

    if not os.path.exists(db_path):
        return []

    try:
        conn = duckdb.connect(db_path, read_only=True)
        offset = page * num_results

        result = conn.execute("""
            SELECT timestamp, lat, lon
            FROM positions
            WHERE icao = ?
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            ORDER BY timestamp ASC
            LIMIT ? OFFSET ?
        """, [icao, num_results, offset]).fetchall()

        conn.close()

        return [
            {"timestamp": row[0], "lat": row[1], "lon": row[2]}
            for row in result
        ]
    except Exception:
        return []


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    db_path = os.path.join(settings.prepared_dir, "aircraft.db")

    if not os.path.exists(db_path):
        return {"max_altitude_baro": None, "max_ground_speed": None, "had_emergency": False}

    try:
        conn = duckdb.connect(db_path, read_only=True)

        result = conn.execute("""
            SELECT
                MAX(altitude_baro) as max_altitude_baro,
                MAX(ground_speed) as max_ground_speed,
                BOOL_OR(emergency IS NOT NULL AND emergency != '') as had_emergency
            FROM positions
            WHERE icao = ?
        """, [icao]).fetchone()

        conn.close()

        if result:
            return {
                "max_altitude_baro": result[0],
                "max_ground_speed": result[1],
                "had_emergency": bool(result[2]) if result[2] is not None else False
            }
    except Exception:
        pass

    return {"max_altitude_baro": None, "max_ground_speed": None, "had_emergency": False}
