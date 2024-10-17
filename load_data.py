import requests
import os
import urllib.request

from bs4 import BeautifulSoup


def download_files():
    page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    download_dir = './data/'
    os.makedirs(download_dir, exist_ok=True)
    response = requests.get(page_url)

    if response.status_code == 200:
        count = 0
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', title='Yellow Taxi Trip Records')
        
        for link in links:
            # test with one parquet file
            if count == 1:
                break
            link_url = link.get('href')
            file_name = os.path.join(download_dir, link_url.split('/')[-1])
            print(f'downloading {link_url.split('/')[-1]}...')
            urllib.request.urlretrieve(url=link_url, filename=file_name)
            count += 1
            print(f'downloaded {link_url.split('/')[-1]}')
        return True
    else:
        print(f"Error: {response.status_code}")
        return False


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

    from pathlib import Path
    from google.cloud.storage import Client, transfer_manager

    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'serviceKeyGoogle.json'

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
    relative_paths = [path.relative_to(source_directory) for path in file_paths]

    # Finally, convert them all to strings.
    string_paths = [str(path) for path in relative_paths]

    print("Found {} files.".format(len(string_paths)))

    # Start the upload.
    results = transfer_manager.upload_many_from_filenames(
        bucket, string_paths, source_directory=source_directory, max_workers=workers
    )

    for name, result in zip(string_paths, results):
        # The results list is either `None` or an exception for each filename in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))

