import os

from google.cloud import storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./serviceKeyGoogle.json"


def list_buckets():
    try:
        # Initialize a storage client
        client = storage.Client()
        # List all buckets in your project
        buckets = client.list_buckets()
        print("Buckets in your GCP project:")
        for bucket in buckets:
            print(bucket.name)
            blobs = client.list_blobs(bucket_or_name=bucket)
            for blob in blobs:
                print(f"\t{blob.name}")

    except Exception as e:
        print(f"Error accessing GCP: {e}")


# Run the function
list_buckets()