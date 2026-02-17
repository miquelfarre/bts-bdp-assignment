import boto3
import requests
import time
import polars as pl
import json
import os
import gzip
from typing import Annotated

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
    s3_prefix = "raw/day=20231101/"
    
    # connection to bucket
    s3 = boto3.client("s3")

    # clean the download s3 folder
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    if "Contents" in response:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3.delete_objects(Bucket=s3_bucket, Delete={"Objects": objects_to_delete})


    # making the request
    downloaded = 0
    i = 0
    retries = 0


    while downloaded < file_limit and retries < settings.MAX_RETRIES:
        seconds = i * 5
        timestamp = f"{seconds:06d}Z"

        url = f"{base_url}{timestamp}.json.gz"
        s3_key = f"{s3_prefix}{timestamp}.json"  # Save as .json since it's not actually gzipped

        try:
            with requests.get(url, stream=True, timeout=10) as r:
                # when file is not found
                if r.status_code == 404:
                    retries += 1
                    print(f"Skipped {timestamp} (404)")
                    i += 1
                    continue
                
                r.raise_for_status() # catches other errors

                s3.upload_fileobj(
                    r.raw,
                    s3_bucket,
                    s3_key,
                    ExtraArgs={
                        "ContentType": "application/json"
                    }
                )

            downloaded += 1
            retries = 0
            print(f"Downloaded {timestamp} ({downloaded}/{file_limit})")

        except requests.RequestException:
            print(f"Skipped {timestamp}")

        i += 1
        time.sleep(0.2)
    
    if retries >= settings.MAX_RETRIES:
        print("Forced stopped because no more files were found.")

    return json.dumps({
        "downloaded": downloaded,
        "attempts": i,
        "stopped_early": retries >= settings.MAX_RETRIES
    })


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s1.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"
    prepared_dir = settings.prepared_dir
    
    os.makedirs(prepared_dir, exist_ok=True)
    
    # Clean prepared folder
    for file in os.listdir(prepared_dir):
        file_path = os.path.join(prepared_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    
    s3 = boto3.client("s3")
    
    # List all files in S3
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    
    if "Contents" not in response:
        return "No files found in S3"
    
    all_aircraft = []
    file_count = 0
    
    for obj in response["Contents"]:
        s3_key = obj["Key"]
        
        # Skip if it's just the folder prefix
        if s3_key.endswith("/"):
            continue
        
        print("Adding", s3_key)
        s3_response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        raw_bytes = s3_response["Body"].read()

        content = gzip.decompress(raw_bytes).decode("utf-8")
        data = json.loads(content)
        
        timestamp = data["now"]
        
        for aircraft in data.get("aircraft", []):
            row = {"timestamp": timestamp}
            for col in settings.business_columns:
                value = aircraft.get(col)
                if col == "flight" and value is not None:
                    value = value.strip()
                if col == "emergency":
                    value = None if value == "none" else value
                if col == "alt_baro":
                    value = None if value == "ground" else value
                row[col] = value
            all_aircraft.append(row)
        
        file_count += 1
    
    df = pl.DataFrame(all_aircraft, schema=settings.business_schema)
    
    # Write to parquet
    output_path = os.path.join(prepared_dir, settings.parquet_name)
    df.write_parquet(output_path)
    
    return f"Prepared {len(df)} records from {file_count} files"
