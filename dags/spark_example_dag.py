from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from spark_jobs import spark_task

# import sys
# import os

# sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", ""), "..", "spark_jobs"))

# def run_spark_callable():
#     import spark_jobs.spark_task
#     spark_jobs.spark_task.run_spark_job()

default_args = {
    'owner': 'AbdulRub',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='pyspark_airflow_dag',
    schedule='@once',
    default_args=default_args,
    description='Run a Pyspark job from Airflow',
    start_date=datetime(2025, 8, 1),
    catchup=False
        ) as dag:
    PythonOperator(
        task_id='run_spark',
        python_callable=spark_task.run_spark_job
    )