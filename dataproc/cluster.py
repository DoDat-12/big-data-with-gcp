import os
from google.cloud import dataproc_v1 as dataproc


def create_cluster(project_id, region, cluster_name):
    """Creating a Cloud Dataproc cluster
    Args:
        project_id (string): Project to use for creating resources.
        region (string): Region where the resources should live.
        cluster_name (string): Name to use for creating a cluster.
    """

    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../serviceKeyGoogle.json'


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
                "disk_config": {
                    "boot_disk_size_gb": 50
                },
            },
            "worker_config": {
                "num_instances": 2, 
                "machine_type_uri": "n2-standard-2",
                "disk_config": {
                    "boot_disk_size_gb": 50
                },
            },
            "gce_cluster_config": {
                "internal_ip_only": False,
            },
        },
    }

    try:
        cluster_client.get_cluster(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name
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
        project_id (str): Project to use for creating resources.
        region (str): Region where the resources should live.
        cluster_name (str): Name to use for creating a cluster.
    """

    # set up authenticate to GCS
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../serviceKeyGoogle.json'


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


# information
# https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Cluster
