import os
import tempfile
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


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""Limits the number of files to download.
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
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Clean existing files in S3 prefix
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                s3_client.delete_objects(Bucket=s3_bucket, Delete={'Objects': objects_to_delete})
    except Exception:
        pass

    # Generate list of expected filenames (hourly files for 2023-11-01)
    filenames = [f"{hour:02d}0000Z.json.gz" for hour in range(24)]

    # Download files and upload to S3
    files_downloaded = 0
    for filename in filenames:
        if files_downloaded >= file_limit:
            break

        file_url = f"{settings.source_url}/2023/11/01/{filename}"

        try:
            response = requests.get(file_url, timeout=30)
            if response.status_code == 200:
                # Upload to S3
                s3_key = s3_prefix + filename
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=response.content
                )
                files_downloaded += 1
        except Exception:
            continue

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s1.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"
    prepared_dir = settings.prepared_dir

    # Initialize S3 client
    s3_client = boto3.client('s3')

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

    # List all objects in S3 with the prefix
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    except Exception:
        return "OK"

    if 'Contents' not in response:
        return "OK"

    # Download files from S3 to a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download each file from S3
        for obj in response['Contents']:
            s3_key = obj['Key']

            # Skip if it's the directory itself
            if s3_key == s3_prefix or not s3_key.endswith('.json.gz'):
                continue

            file_name = os.path.basename(s3_key)
            local_file_path = os.path.join(temp_dir, file_name)

            try:
                s3_client.download_file(s3_bucket, s3_key, local_file_path)
            except Exception:
                continue

        # Check if we have any files
        json_files = [f for f in os.listdir(temp_dir) if f.endswith('.json.gz')]
        if not json_files:
            return "OK"

        # Connect to database and process files using DuckDB
        conn = duckdb.connect(db_path)

        try:
            # Use DuckDB's native JSON reading - MUCH faster than Python parsing
            file_pattern = os.path.join(temp_dir, "*.json.gz").replace('\\', '/')

            # Read all JSON files directly with DuckDB and unnest aircraft array
            # Files have .gz extension but are NOT compressed
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
