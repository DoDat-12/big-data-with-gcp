from cluster import (
    create_cluster,
    delete_cluster,
    stop_cluster,
    start_cluster,
    submit_pyspark_job,
)


def main():
    # TODO: 12-11-2024 - run this overnight - done
    # Cluster & job information
    project_id = "uber-analysis-439804"
    region = "us-central1"
    cluster_name = "uber-proc"
    gcs_bucket = "uber-pyspark-jobs"

    # 2011 init - gs://uber-pyspark-jobs/wh_init.py
    init_wh = "wh_init.py"
    # 2012 to 2024 batch - gs://uber-pyspark-jobs/wh_batch_load.py
    load_wh = "wh_batch_load.py"

    # Create cluster
    # create_cluster(
    #     project_id=project_id,
    #     region=region,
    #     cluster_name=cluster_name,
    # )

    # Submit init dw job
    submit_pyspark_job(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        gcs_bucket=gcs_bucket,
        spark_filename=init_wh
    )

    # Submit batch processing job
    for i in range(2014, 2025):
        submit_pyspark_job(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
            gcs_bucket=gcs_bucket,
            spark_filename=load_wh,
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
