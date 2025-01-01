from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

from pyspark_submit import start_cluster, submit_pyspark_job, stop_cluster

dag = DAG(
    dag_id="monthly_analyze_job",
    default_args={
        "owner": "tadod",
        "start_date": days_ago(1),
        "execution_timeout": timedelta(minutes=500),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 0 2 * *"  # 2nd day of every month
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

general_analyzer = PythonOperator(
    task_id="run_general_analyse_job",
    python_callable=submit_pyspark_job,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc",
        "gcs_bucket": "uber-pyspark-jobs",
        "spark_filename": "general_analyzer.py",
    },
    dag=dag
)

payment_year_analyzer = PythonOperator(
    task_id="run_payment_year_analyse_job",
    python_callable=submit_pyspark_job,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc",
        "gcs_bucket": "uber-pyspark-jobs",
        "spark_filename": "payment_year_analyzer.py",
    },
    dag=dag
)

pickup_location_analyzer = PythonOperator(
    task_id="run_pu_location_analyse_job",
    python_callable=submit_pyspark_job,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc",
        "gcs_bucket": "uber-pyspark-jobs",
        "spark_filename": "pu_location_analyzer.py",
    },
    dag=dag
)

dropoff_location_analyzer = PythonOperator(
    task_id="run_do_location_analyse_job",
    python_callable=submit_pyspark_job,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc",
        "gcs_bucket": "uber-pyspark-jobs",
        "spark_filename": "do_location_analyzer.py",
    },
    dag=dag
)

day_analyzer = PythonOperator(
    task_id="run_day_analyse_job",
    python_callable=submit_pyspark_job,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc",
        "gcs_bucket": "uber-pyspark-jobs",
        "spark_filename": "day_analyzer.py",
    },
    dag=dag
)

hour_analyzer = PythonOperator(
    task_id="run_hour_analyse_job",
    python_callable=submit_pyspark_job,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc",
        "gcs_bucket": "uber-pyspark-jobs",
        "spark_filename": "hour_analyzer.py",
    },
    dag=dag
)

stopping_cluster = PythonOperator(
    task_id="stop_cluster",
    python_callable=stop_cluster,
    op_kwargs={
        "project_id": "uber-analysis-439804",
        "region": "us-central1",
        "cluster_name": "uber-proc"
    },
    provide_context=True,
    dag=dag
)

starting_cluster.set_downstream(general_analyzer)
general_analyzer.set_downstream(payment_year_analyzer)
payment_year_analyzer.set_downstream(pickup_location_analyzer)
pickup_location_analyzer.set_downstream(dropoff_location_analyzer)
dropoff_location_analyzer.set_downstream(day_analyzer)
day_analyzer.set_downstream(hour_analyzer)
hour_analyzer.set_downstream(stopping_cluster)
