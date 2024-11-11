import os
import re
import requests
import urllib.request
import pandas as pd

from pathlib import Path
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud.storage import Client, transfer_manager


def download_yellow_files():
    page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    download_dir = './yellow/'
    os.makedirs(download_dir, exist_ok=True)
    response = requests.get(page_url)

    if response.status_code == 200:
        # count = 0
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', title='Yellow Taxi Trip Records')

        for link in links:
            # test with one parquet file
            # if count == 1:
            #     break
            link_url = link.get('href')

            if match := re.search(r'yellow_tripdata_(\d{4})-', link_url):
                year = int(match.group(1))
                if year > 2010:
                    file_name = os.path.join(
                        download_dir,
                        link_url.split('/')[-1]
                    )
                    print(f'downloading {link_url.split('/')[-1]}...')
                    urllib.request.urlretrieve(
                        url=link_url,
                        filename=file_name
                    )
                    # count += 1
                    print(f'downloaded {link_url.split('/')[-1]}')
                    preprocess(file_name)
        return True
    else:
        print(f"Error: {response.status_code}")
        return False


def preprocess(file_path):
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


def download_green_files():
    page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    download_dir = './green/'
    os.makedirs(download_dir, exist_ok=True)
    response = requests.get(page_url)

    if response.status_code == 200:
        # count = 0
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', title='Green Taxi Trip Records')

        for link in links:
            # test with one parquet file
            # if count == 1:
            #     break
            link_url = link.get('href')

            if match := re.search(r'green_tripdata_(\d{4})-', link_url):
                year = int(match.group(1))
                if year > 2010:
                    file_name = os.path.join(
                        download_dir,
                        link_url.split('/')[-1]
                    )
                    print(f'downloading {link_url.split('/')[-1]}...')
                    urllib.request.urlretrieve(
                        url=link_url,
                        filename=file_name
                    )
                    # count += 1
                    print(f'downloaded {link_url.split('/')[-1]}')
        return True
    else:
        print(f"Error: {response.status_code}")
        return False


def upload_files(bucket_name, source_directory):
    file_paths = []
    for root, _, files in os.walk(source_directory):
        for file in files:
            file_paths.append(os.path.join(root, file).replace("\\", "/"))
    for file_path in file_paths:
        upload_blob(
            bucket_name=bucket_name,
            source_file_name=file_path,
            destination_blob_name=file_path.split('/')[-1]
        )


def upload_directory_with_transfer_manager(bucket_name, source_directory, workers=8):
    """Upload every file in a directory, including all files in subdirectories.

    Each blob name is derived from the filename, not including the `directory`
    parameter itself. For complete control of the blob name for each file (and
    other aspects of individual blob metadata), use
    transfer_manager.upload_many() instead.
    """

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The directory on your computer to upload. Files in the directory and its
    # subdirectories will be uploaded. An empty string means "the current
    # working directory".
    # source_directory=""

    # The maximum number of processes to use for the operation. The performance
    # impact of this value depends on the use case, but smaller files usually
    # benefit from a higher number of processes. Each additional process occupies
    # some CPU and memory resources until finished. Threads can be used instead
    # of processes by passing `worker_type=transfer_manager.THREAD`.
    # workers=8

    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../serviceKeyGoogle.json'

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    # Generate a list of paths (in string form) relative to the `directory`.
    # This can be done in a single list comprehension, but is expanded into
    # multiple lines here for clarity.

    # First, recursively get all files in `directory` as Path objects.
    directory_as_path_obj = Path(source_directory)
    paths = directory_as_path_obj.rglob("*")

    # Filter so the list only includes files, not directories themselves.
    file_paths = [path for path in paths if path.is_file()]

    # These paths are relative to the current working directory. Next, make them
    # relative to `directory`
    relative_paths = [
        path.relative_to(source_directory) for path in file_paths
    ]

    # Finally, convert them all to strings.
    string_paths = [str(path) for path in relative_paths]

    print("Found {} files.".format(len(string_paths)))

    # Start the upload.
    print(f"Uploading files to bucket {bucket_name}...")
    results = transfer_manager.upload_many_from_filenames(
        bucket, string_paths,
        source_directory=source_directory,
        max_workers=workers,
        skip_if_exists=True,
        deadline=None  # avoid TimeError
    )

    for name, result in zip(string_paths, results):
        # The results list is either `None` or an exception for each filename in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))


def upload_blob(bucket_name, source_file_name, destination_blob_name):
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
