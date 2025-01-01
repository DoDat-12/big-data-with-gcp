from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

from crawler import crawl_new_data
from pyspark_submit import start_cluster, submit_pyspark_job, stop_cluster

dag = DAG(
    dag_id="monthly_crawl_and_load_job",
    default_args={
        "owner": "tadod",
        "start_date": days_ago(1),
        "execution_timeout": timedelta(minutes=300),
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    schedule_interval="@monthly"
)

crawler = PythonOperator(
    task_id="crawl_new_data",
    python_callable=crawl_new_data,
    op_kwargs={
        "year": 2024
    },
    provide_context=True,
    dag=dag
)

starting_cluster = PythonOperator(
    task_id="start_cluster",
    python_callable=start_cluster,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc"
    },
    provide_context=True,
    dag=dag
)

loading_job_submit = PythonOperator(
    task_id="running_loading_job",
    python_callable=submit_pyspark_job,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc",
        "gcs_bucket": "uber-pyspark-jobs",
        "spark_filename": "loader.py",
        "arg": [str(2024)]
    },
    provide_context=True,
    dag=dag
)

stopping_cluster = PythonOperator(
    task_id="stop_cluster",
    python_callable=stop_cluster,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc",
    },
    provide_context=True,
    dag=dag
)

crawler >> starting_cluster >> loading_job_submit >> stopping_cluster

# project_id = "uber-analysis-439804"
# region = "us-central1"
# cluster_name = "uber-proc"
# gcs_bucket = "uber-pyspark-jobs"
