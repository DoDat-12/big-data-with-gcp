import os
import re  # support regular expressions

from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def create_cluster(project_id, region, cluster_name):
    """Creating a Cloud Dataproc cluster
    Args:
        project_id (string): Project to use for creating resources.
        region (string): Region where the resources should live.
        cluster_name (string): Name to use for creating a cluster.
    """

    # set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceKeyGoogle.json"

    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the cluster config.
    # TODO: config for avoiding quota & subnetwork error - solved
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n2-standard-2",
                "disk_config": {"boot_disk_size_gb": 50},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n2-standard-2",
                "disk_config": {"boot_disk_size_gb": 50},
            },
            "gce_cluster_config": {
                "internal_ip_only": False,
            },
        },
    }

    try:
        cluster_client.get_cluster(
            project_id=project_id, region=region, cluster_name=cluster_name
        )
        print(f"Cluster {cluster_name} already exists.")
    except:
        print(f"Creating cluster {cluster_name}")
        # Create the cluster.
        operation = cluster_client.create_cluster(
            request={"project_id": project_id, "region": region, "cluster": cluster}
        )
        result = operation.result()

        # Output a success message.
        print(f"Cluster created successfully: {result.cluster_name}")


def update_cluster(project_id, region, cluster_name, new_num_instances):
    """Updating a Cloud Dataproc cluster
    Args:
        project_id (str): Project ID that contains cluster.
        region (str): Region where the resources live.
        cluster_name (str): Cluster's name.
        new_num_instances (int): Number of instances
    """

    # set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceKeyGoogle.json"

    # Create a client with the endpoint set to the desired cluster region.
    client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Get cluster you wish to update.
    cluster = client.get_cluster(
        project_id=project_id, region=region, cluster_name=cluster_name
    )

    # Update number of clusters
    mask = {"paths": {"config.worker_config.num_instances": str(new_num_instances)}}

    # Update cluster config
    cluster.config.worker_config.num_instances = new_num_instances

    # Update cluster
    operation = client.update_cluster(
        project_id=project_id,
        region=region,
        cluster=cluster,
        cluster_name=cluster_name,
        update_mask=mask,
    )

    # Output a success message.
    updated_cluster = operation.result()
    print(f"Cluster was updated successfully: {updated_cluster.cluster_name}")


def delete_cluster(project_id, region, cluster_name):
    """Delete a Cloud Dataproc cluster
    Args:
        project_id (str): Project ID that contains cluster.
        region (str): Region where the resources live.
        cluster_name (str): Cluster's name for deleting.
    """

    # Set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceKeyGoogle.json"

    # Create a client with the endpoint set to the desired cluster region.
    client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    operation = client.delete_cluster(
        # Using DeleteClusterRequest
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    print(f"Deleting cluster {cluster_name}...")
    operation.result()
    print(f"Cluster {cluster_name} successfully deleted")


def start_cluster(project_id, region, cluster_name):
    """Start a Cloud Dataproc cluster
    Args:
        project_id (str): Project ID that contains cluster.
        region (str): Region where the resources live.
        cluster_name (str): Cluster's name for starting.
    """

    # Set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceKeyGoogle.json"

    # Create a client with the endpoint set to the desired cluster region.
    client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Operation to start cluster
    operation = client.start_cluster(
        # Using StartClusterRequest
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    print(f"Starting cluster {cluster_name}...")
    operation.result()
    print(f"Cluster {cluster_name} started successully")


def stop_cluster(project_id, region, cluster_name):
    """Stop a Cloud Dataproc Cluster
    Args:
        project_id (str): Project ID that contains cluster.
        region (str): Region where the resources live.
        cluster_name (str): Name of a cluster that need to be stopped.
    """

    # Set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceKeyGoogle.json"

    # Create a client with the endpoint set to the desired cluster region.
    client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Operation to stop cluster
    operation = client.stop_cluster(
        # Using StopClusterRequest
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    print(f"Stopping cluster {cluster_name}...")
    operation.result()
    print(f"Cluster {cluster_name} stopped successfully")


def submit_pyspark_job(project_id, region, cluster_name, gcs_bucket, spark_filename):
    """Submit PySpark Job to cluster
    Args:
        project_id (str): Project ID that contains cluster.
        region (str): Region where the resources live.
        cluster_name (str): Cluster's name to submit job.
        gcs_bucket (str): Bucket's name that contains pyspark file.
        spark_filename (str): Python file.
    """
    # Set up authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceKeyGoogle.json"

    # Create the job client
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the job config - Class Job (5.13.0)
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            # Class PySparkJob (5.13.0)
            "main_python_file_uri": f"gs://{gcs_bucket}/{spark_filename}",
        },
    }

    operation = job_client.submit_job_as_operation(
        # Using SubmitJobRequest
        request={
            "project_id": project_id,
            "region": region,
            "job": job,
        }
    )
    response = operation.result()

    # Dataproc job output is saved to the Cloud Storage bucket
    # allocated to the job. Use regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    # TODO: Check what is this
    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("utf-8")
    )

    print(f"Job finished successfully: {output}\r\n")


# information
# https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Cluster
