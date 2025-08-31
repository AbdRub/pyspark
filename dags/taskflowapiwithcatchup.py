from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(dag_id='taskflowapidagwithcatchup',
     default_args=default_args,
     description='A dummy DAG for demonstration purposes to work with catchup feature',
     start_date=datetime(2025, 8, 31),
     schedule='35 9 * * *',
     catchup=True
     )

def taskflowapidagwithcatchup():

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

greet_dag = taskflowapidagwithcatchup()