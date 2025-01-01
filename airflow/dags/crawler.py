import os
import re
import requests
import urllib.request
import pandas as pd

# from pathlib import Path
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud.exceptions import NotFound, Forbidden
# from google.cloud.storage import Client, transfer_manager


def get_latest_date(bucket_name, **kwargs):
    """Get the latest updated data from the GCS bucket"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google/credentials/serviceKeyGoogle.json"
    storage_client = storage.Client()
    try:
        bucket = storage_client.get_bucket(bucket_name)
        max_month = 0
        blobs = storage_client.list_blobs(bucket_name)

        # Note: The call returns a response only when the iterator is consumed
        for blob in blobs:
            match = re.search(r"yellow_tripdata_(\d{4})-(\d{2})", blob.name)
            if match:
                month = int(match.group(2))
                max_month = month if month > max_month else max_month

        print(f"Latest update month: {max_month:02d}")
        return max_month
    except NotFound:
        print(f"Bucket {bucket_name} not found")
        print(f"Creating bucket {bucket_name}")
        bucket = storage_client.create_bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        new_bucket = storage_client.create_bucket(bucket, location="us")
        print(f"Bucket {bucket_name} created")
        print(f"Location: {new_bucket.location}")
        print(f"Storage Class: {new_bucket.storage_class}")
        print("Latest updated month: 00")
        return 0
    except Forbidden as e:
        print(f"You don't have permission to create a bucket: {e}")
        return None


def crawl_new_data(year, **kwargs):
    latest_month = get_latest_date(f"uber-{year}-154055")
    os.makedirs("/tmp/yellow/", mode=0o777, exist_ok=True)

    page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    response = requests.get(page_url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', title='Yellow Taxi Trip Records')

        new_data_existed = False

        for link in links:
            link_url = link.get("href")
            if match := re.search(r"yellow_tripdata_(\d{4})-(\d{2})", link_url):
                link_year = int(match.group(1))
                link_month = int(match.group(2))
                if link_year == year and link_month > latest_month:
                    new_data_existed = True

                    file_name = os.path.join(
                        "/tmp/yellow/", link_url.split("/")[-1])
                    print(f"Downloading {link_url.split("/")[-1]}...")
                    urllib.request.urlretrieve(
                        url=link_url, filename=file_name)
                    preprocess(file_name)
                    print(f"Downloaded {link_url.split('/')[-1]} successfully")

                    upload_blob(f"uber-{year}-154055",
                                file_name, file_name.split('/')[-1])
                    upload_blob("new-data-154055",
                                file_name, file_name.split('/')[-1])
        if not new_data_existed:
            print("No new data found")
        return new_data_existed
    else:
        print(f"Error: {response.status_code}")
        return False


def preprocess(file_path, **kwargs):
    try:
        df = pd.read_parquet(file_path)
        columns = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "total_amount",
        ]

        df_filtered = df[columns]
        df_filtered = df_filtered.dropna()

        # Cast columns to appropriate types
        df_filtered["VendorID"] = df_filtered["VendorID"] \
            .astype("int64")
        df_filtered["passenger_count"] = df_filtered["passenger_count"] \
            .astype("int64")
        df_filtered["trip_distance"] = df_filtered["trip_distance"] \
            .astype("float64")
        df_filtered["RatecodeID"] = df_filtered["RatecodeID"] \
            .astype("int64")
        df_filtered["PULocationID"] = df_filtered["PULocationID"] \
            .astype("int64")
        df_filtered["DOLocationID"] = df_filtered["DOLocationID"] \
            .astype("int64")
        df_filtered["payment_type"] = df_filtered["payment_type"] \
            .astype("int64")
        df_filtered["fare_amount"] = df_filtered["fare_amount"] \
            .astype("float64")
        df_filtered["extra"] = df_filtered["extra"] \
            .astype("float64")
        df_filtered["mta_tax"] = df_filtered["mta_tax"] \
            .astype("float64")
        df_filtered["tip_amount"] = df_filtered["tip_amount"] \
            .astype("float64")
        df_filtered["tolls_amount"] = df_filtered["tolls_amount"] \
            .astype("float64")
        df_filtered["total_amount"] = df_filtered["total_amount"] \
            .astype("float64")

        df_filtered.to_parquet(file_path, index=False)
        print(f"{file_path} preprocessed successfully")
    except Exception as e:
        print(f"Failed to processed {file_path}: {e}")


def upload_blob(bucket_name, source_file_name, destination_blob_name, **kwargs):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    print(f'Uploading {source_file_name}...')

    try:
        blob.upload_from_filename(
            filename=source_file_name,
            if_generation_match=generation_match_precondition,
            timeout=6000  # avoid TimeoutError
        )
    except:
        print(f"{source_file_name} already uploaded")

    print(
        f"File {source_file_name} uploaded to bucket {bucket_name}"
    )

# information
# https://cloud.google.com/storage/docs/uploading-objects
# https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.transfer_manager
# https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob#google_cloud_storage_blob_Blob_upload_from_filename
