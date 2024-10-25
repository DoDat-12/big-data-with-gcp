import os

from bucket import create_bucket_class_location
from load_data import download_files, upload_directory_with_transfer_manager, upload_blob


def main():
    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../serviceKeyGoogle.json'

    bucket_name = 'parquet-uber'
    create_bucket_class_location(bucket_name=bucket_name)  # asia-east1

    if download_files():
        #     upload_directory_with_transfer_manager(bucket_name=bucket_name, source_directory='./data/', workers=4)
        # test with one file
        upload_blob(bucket_name=bucket_name, source_file_name='./data/yellow_tripdata_2024-01.parquet',
                    destination_blob_name='yellow_tripdata_2024-01.parquet')


if __name__ == "__main__":
    main()
