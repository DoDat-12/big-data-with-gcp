import os
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import Forbidden


def create_bucket_class_location(bucket_name):
    """Create a new bucket in the US region with the standard storage class."""
    # bucket_name = 'your-bucket-name'

    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../serviceKeyGoogle.json'
    storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)

    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists")
        return bucket
    except NotFound:
        print(f"Creating bucket {bucket_name}")
        # bucket = storage_client.create_bucket(bucket_name)
        # bucket.storage_class = "STANDARD"
        new_bucket = storage_client.create_bucket(bucket_name, location="us")
        print(f"Bucket {new_bucket.name} created")
        print(f"Location: {new_bucket.location}")
        print(f"Storage Class: {new_bucket.storage_class}")
        return new_bucket
    except Forbidden as e:
        print(f"You don't have permission to create a bucket: {e}")
        return None

# information
# https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-python
