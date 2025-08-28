from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'Abdul Rub',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 8, 27),
    'schedule_interval': '@daily',
    'depends_on_past': False
}

@dag(dag_id='taskflow_api_dag',
     default_args=default_args,
     catchup=False,
     description='A dummy DAG for demonstration purposes',
     start_date=datetime(2025, 8, 27))

def taskflow_api_dag():
    
    @task(multiple_outputs=True)
    def get_name():
        return {'first_name': 'Abdul', 'last_name': 'Rub'}

    @task()
    def get_age():
        return '23'

    @task()
    def greet_me(first_name, last_name, age):
        print(f"Hello, my name is {first_name}{last_name} and I am {age} years old.")

    name_dict = get_name()
    age = get_age()
    greet_me(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)

greet_dag = taskflow_api_dag()