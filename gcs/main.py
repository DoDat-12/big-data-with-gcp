import os

from bucket import create_bucket_class_location
from load_data import download_yellow_files, upload_files


def main():
    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../serviceKeyGoogle.json'

    # raw_yellow_bucket_name = "uber-lake-yellow"
    # job_bucket_name = "uber-pyspark-jobs"
    # tmp_bucket_name = "uber-temp"

    # raw_air_con_null = "uber-lake-chunk-1"
    # raw_air_null = "uber-lake-chunk-2"
    # raw_air = "uber-lake-chunk-3"
    # raw_Air = "uber-lake-chunk-4"

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
    # create_bucket_class_location(bucket_name="lake-test")
    # create_bucket_class_location(bucket_name=job_bucket_name)
    # create_bucket_class_location(bucket_name=tmp_bucket_name)

    # create_bucket_class_location(bucket_name=raw_air_con_null)
    # create_bucket_class_location(bucket_name=raw_air_null)
    # create_bucket_class_location(bucket_name=raw_air)
    # create_bucket_class_location(bucket_name=raw_Air)

    # download and upload (testing with 1 file)
    # if download_yellow_files():
    #     upload_files(
    #         bucket_name=raw_yellow_bucket_name,
    #         source_directory="./yellow"
    #     )

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
