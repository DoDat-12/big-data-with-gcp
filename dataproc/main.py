from cluster import (
    create_cluster,
    delete_cluster,
    stop_cluster,
    start_cluster,
    submit_pyspark_job,
)


def main():
    # Cluster & job information
    project_id = "uber-analysis-439804"
    region = "us-central1"
    cluster_name = "uber-hadoop-spark-cluster"
    gcs_bucket = "uber-pyspark-jobs"
    spark_file_name = "warehouse_loader_v4.py"
    # gs://uber-pyspark-jobs/warehouse_loader_v1.py

    # Create cluster
    # create_cluster(
    #     project_id=project_id,
    #     region=region,
    #     cluster_name=cluster_name
    # )

    # Submit PySpark Job
    # submit_pyspark_job(
    #     project_id=project_id,
    #     region=region,
    #     cluster_name=cluster_name,
    #     gcs_bucket=gcs_bucket,
    #     spark_filename=spark_file_name,
    # )

    # Stop cluster
    stop_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name
    )

    # Delete cluster
    delete_cluster(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name
    )


if __name__ == "__main__":
    main()
