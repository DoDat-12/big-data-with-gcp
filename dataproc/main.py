from cluster import (
    create_cluster,
    delete_cluster,
    stop_cluster,
    start_cluster,
    submit_pyspark_job,
)


def main():
    # Cluster information
    project_id = "uber-big-data-439701"
    region = "us-central1"
    cluster_name = "uber-cluster"

    # Create cluster
    create_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name
    )

    # Submit PySpark Job
    submit_pyspark_job(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        gcs_bucket="parquet-uber",
        spark_filename="gcs_test.py",
    )

    # Stop cluster
    stop_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name
    )

    # Start cluster
    # start_cluster(project_id=project_id, region=region, cluster_name=cluster_name)

    # Delete cluster
    delete_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name
    )


if __name__ == "__main__":
    main()
