import os
from google.cloud import storage

def create_bucket_class_location(bucket_name):
    """Create a new bucket in the US region with the standard storage class."""
    # bucket_name = 'your-bucket-name'

    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'serviceKeyGoogle.json'


    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = 'STANDARD'
    new_bucket = storage_client.create_bucket(bucket, location='us')
    
    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket


# create_bucket_class_location('uber-parquet')
# information: https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-python
