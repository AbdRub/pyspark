from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator

def print_hello_world():
    print("hello_world")

default_args = {
    'owner': 'Abdul Rub',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 26, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
            dag_id='dummy_dag',
            default_args=default_args,
            description='A dummy DAG for demonstration purposes',
            schedule='@daily',
            catchup=False,
            start_date=datetime(2025, 8, 26, 7)

        ) as dag:

    task1 = PythonOperator(
        task_id='printworld',
        python_callable=print_hello_world
    )