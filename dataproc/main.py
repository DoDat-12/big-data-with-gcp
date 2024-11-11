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
    spark_file_name = "wh_batch_load.py"
    # 2011 init - gs://uber-pyspark-jobs/wh_init.py
    # batch - gs://uber-pyspark-jobs/wh_batch_load.py

    # Create cluster
    # create_cluster(
    #     project_id=project_id,
    #     region=region,
    #     cluster_name=cluster_name
    # )

    # # Submit PySpark Job
    for i in range(2015, 2025):
        submit_pyspark_job(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
            gcs_bucket=gcs_bucket,
            spark_filename=spark_file_name,
            args=[str(i)],
        )

    # Stop cluster
    # stop_cluster(
    #     project_id=project_id,
    #     region=region,
    #     cluster_name=cluster_name
    # )

    # # Delete cluster
    # delete_cluster(
    #     project_id=project_id,
    #     region=region,
    #     cluster_name=cluster_name
    # )


if __name__ == "__main__":
    main()
