import os

from bucket import create_bucket_class_location
from load_data import download_files, upload_files


def main():
    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../serviceKeyGoogle.json'

    raw_bucket_name = "uber-datalake"
    job_bucket_name = "uber-pyspark-jobs"
    tmp_bucket_name = "uber-temp"

    # create bucket to store raw data
    create_bucket_class_location(bucket_name=raw_bucket_name)  # us
    create_bucket_class_location(bucket_name=job_bucket_name)
    create_bucket_class_location(bucket_name=tmp_bucket_name)

    # download and upload (testing with 1 file)
    if download_files():
        upload_files(bucket_name=raw_bucket_name, source_directory="./data")
    # upload_directory_with_transfer_manager(
    #     bucket_name=bucket_name,
    #     source_directory='./data/',
    #     workers=4
    # )

    # downloaded_files = []
    # for root, _, files in os.walk("./data/"):
    #     for file in files:
    #         downloaded_files.append(
    #             os.path.join(root, file).replace("\\", "/")
    #         )
    # print(downloaded_files)


if __name__ == "__main__":
    main()
