from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage

import os


def test_gcp_job(**kwargs):
    """Test Permission to GCP"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google/credentials/serviceKeyGoogle.json"

    try:
        client = storage.Client()
        buckets = client.list_buckets()
        print("Buckets in your GCP project:")
        for bucket in buckets:
            print(bucket.name)
    except Exception as e:
        print(f"Error accessing GCP: {e}")
        return False

    return True


def pull_task(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(task_ids='test_gcp')
    if not pulled_value:
        print("Can not access GCS")
    else:
        print("Access GCS successfully")


dag = DAG(
    dag_id="test_gcp_job",
    default_args={
        "owner": "tadod",
        "start_date": days_ago(1)
    },
    schedule_interval="@daily"
)

test_gcp = PythonOperator(
    task_id="test_gcp",
    python_callable=test_gcp_job,
    provide_context=True,
    dag=dag
)

pulling_task = PythonOperator(
    task_id="pull_task",
    python_callable=pull_task,
    provide_context=True,
    dag=dag
)

test_gcp >> pulling_task
