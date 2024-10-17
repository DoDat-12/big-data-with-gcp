import os

from bucket import create_bucket_class_location
from load_data import download_files, upload_directory_with_transfer_manager


def main():
    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'serviceKeyGoogle.json'

    bucket_name = 'uber-parquet'
    create_bucket_class_location(bucket_name=bucket_name)
    
    if download_files():
        upload_directory_with_transfer_manager(bucket_name=bucket_name, source_directory='../data/', workers=8)


if __name__ == "__main__":
    main()
