def run_first_task(ti):
    # Push a value
    ti.xcom_push(key="xcom_from_first_task", value="value_from_first_task")
    return "first task ran"

def run_second_task(ti):
    # Pull the value from task1
    value = ti.xcom_pull(task_ids="run_first_task", key="xcom_from_first_task")
    print(f"Second task pulled: {value}")
    
    # Push a new value for task3
    ti.xcom_push(key="xcom_from_second_task", value=f"modified_{value}")
    return f"second task ran, got {value}"

def run_third_task(ti):
    # Pull the value from task2
    value = ti.xcom_pull(task_ids="run_second_task", key="xcom_from_second_task")
    print(f"Third task pulled: {value}")
    return f"third task ran, got {value}"

from airflow import DAG, task