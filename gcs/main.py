import os

from bucket import create_bucket_class_location
from load_data import download_yellow_files, upload_files


def main():
    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../serviceKeyGoogle.json'

    # bucket for pyspark jobs
    job_bucket_name = "uber-pyspark-jobs"
    create_bucket_class_location(bucket_name=job_bucket_name)

    # create bucket to store raw data
    create_bucket_class_location(bucket_name="uber-2011-154055")
    create_bucket_class_location(bucket_name="uber-2012-154055")
    create_bucket_class_location(bucket_name="uber-2013-154055")
    create_bucket_class_location(bucket_name="uber-2014-154055")
    create_bucket_class_location(bucket_name="uber-2015-154055")
    create_bucket_class_location(bucket_name="uber-2016-154055")
    create_bucket_class_location(bucket_name="uber-2017-154055")
    create_bucket_class_location(bucket_name="uber-2018-154055")
    create_bucket_class_location(bucket_name="uber-2019-154055")
    create_bucket_class_location(bucket_name="uber-2020-154055")
    create_bucket_class_location(bucket_name="uber-2021-154055")
    create_bucket_class_location(bucket_name="uber-2022-154055")
    create_bucket_class_location(bucket_name="uber-2023-154055")
    create_bucket_class_location(bucket_name="uber-2024-154055")

    # download and upload (testing with 1 file)
    if download_yellow_files():
        upload_files(
            source_directory="./yellow"
        )


if __name__ == "__main__":
    main()
