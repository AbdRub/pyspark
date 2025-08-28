from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from tasks import run_first_task, run_second_task, run_third_task

default_args = {
    'owner': 'Abdul Rub',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
            dag_id='dummy_dag',
            default_args=default_args,
            description='A dummy DAG for demonstration purposes',
            schedule='@daily',
            catchup=False,
            start_date=datetime(2025, 8, 26)

        ) as dag:

    task1 = PythonOperator(
        task_id='run_first_task',
        python_callable=run_first_task
    )

    task2 = PythonOperator(
        task_id='run_second_task',
        python_callable=run_second_task
    )

    task3 = PythonOperator(
        task_id='run_third_task',
        python_callable=run_third_task
    )

    task1 >> task2 >> task3