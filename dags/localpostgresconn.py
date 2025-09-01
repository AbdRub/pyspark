from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

default_args = {
    'owner': 'abdulrub',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(dag_id='postgres_conn_dag',
     default_args=default_args,
     start_date=datetime(2025, 8, 31),
     description='PostgreSQL connection demonstration with optimized error handling',
     schedule=None,
     catchup=False,
     max_active_runs=1,
     tags=['postgres', 'demo']
     )
def postgres_conn_dag():

    @task()
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='local_postgres_conn')
        sql = """
        CREATE TABLE IF NOT EXISTS public.test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INT CHECK (age > 0),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        try:
            pg_hook.run(sql)
            logging.info("Table creation completed successfully")
            return "success"
        except Exception as e:
            logging.error(f"Table creation failed: {str(e)}")
            raise
    
    @task()
    def insert_data():
        pg_hook = PostgresHook(postgres_conn_id='local_postgres_conn')
        sql = """
        INSERT INTO public.test_table (name, age) 
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING
        """
        try:
            pg_hook.run(sql, parameters=('Abdul Rub', 23))
            logging.info("Data insertion completed successfully")
            return "success"
        except Exception as e:
            logging.error(f"Data insertion failed: {str(e)}")
            raise
    
    create_table() >> insert_data()

postgres_conn_dag()
