import os
import re

from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def start_cluster(project_id, region, cluster_name, **kwargs):
    """Start a Cloud Dataproc cluster
    Args:
        project_id (str): Project ID that contains cluster.
        region (str): Region where the resources live.
        cluster_name (str): Cluster's name for starting.
    """

    # For checking the result of the previous task in Airflow
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(task_ids="crawl_new_data")

    if pulled_value:
        print("No new data! No need to start cluster")
        return False

    # Set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google/credentials/serviceKeyGoogle.json"

    # Create a client with the endpoint set to the desired cluster region.
    client = dataproc.ClusterControllerClient(
        client_options={
            "api_endpoint": f"{region}-dataproc.googleapis.com:443"
        }
    )

    # Operation to start cluster
    operation = client.start_cluster(
        # Using StartClusterRequest
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
        # timeout=None,
    )
    print(f"Starting cluster {cluster_name}...")
    operation.result()
    print(f"Cluster {cluster_name} started successully")


def submit_pyspark_job(project_id, region, cluster_name, gcs_bucket, spark_filename, **kwargs):
    """Submit PySpark Job to cluster
    Args:
        project_id (str): Project ID that contains cluster.
        region (str): Region where the resources live.
        cluster_name (str): Cluster's name to submit job.
        gcs_bucket (str): Bucket's name that contains pyspark file.
        spark_filename (str): Python file.
    """

    # For checking the result of the previous task in Airflow
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(task_ids="crawl_new_data")

    if pulled_value:
        print("No new data! No need to submit job")
        return False

    # Set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google/credentials/serviceKeyGoogle.json"

    # Create the job client
    job_client = dataproc.JobControllerClient(
        client_options={
            "api_endpoint": f"{region}-dataproc.googleapis.com:443"
        }
    )

    # Create the job config - Class Job (5.13.0)
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            # Class PySparkJob (5.13.0)
            "main_python_file_uri": f"gs://{gcs_bucket}/{spark_filename}",
            "args": None,  # No time limit
        },
    }

    operation = job_client.submit_job_as_operation(
        # Using SubmitJobRequest
        request={
            "project_id": project_id,
            "region": region,
            "job": job,
        },
        timeout=None,
    )
    print(f"Job {spark_filename} submmiting...")
    response = operation.result(timeout=None)
    print("Job running...")

    # Dataproc job output is saved to the Cloud Storage bucket
    # allocated to the job. Use regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("utf-8")
    )

    print(f"Job finished successfully: {output}\r\n")


def stop_cluster(project_id, region, cluster_name, **kwargs):
    """Stop a Cloud Dataproc Cluster
    Args:
        project_id (str): Project ID that contains cluster.
        region (str): Region where the resources live.
        cluster_name (str): Name of a cluster that need to be stopped.
    """

    # For checking the result of the previous task in Airflow
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(task_ids="crawl_new_data")

    if pulled_value:
        print("No new data! No need to stop cluster")
        return False

    # Set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google/credentials/serviceKeyGoogle.json"

    # Create a client with the endpoint set to the desired cluster region
    client = dataproc.ClusterControllerClient(
        client_options={
            "api_endpoint": f"{region}-dataproc.googleapis.com:443"
        }
    )

    # Operation to stop cluster
    operation = client.stop_cluster(
        # Using StopClusterRequest
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
        # timeout=None,
    )
    print(f"Stopping cluster {cluster_name}...")
    operation.result()
    print(f"Cluster {cluster_name} stopped successfully")

# information
# https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Cluster
