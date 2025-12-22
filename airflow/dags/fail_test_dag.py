from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def fail_task():
    raise Exception("의도적으로 실패")

default_args = {
    "owner": "airflow",
    "email": ["symoon1007@gmail.com"],  
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="test_fail_email",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    fail = PythonOperator(
        task_id="fail_task",
        python_callable=fail_task,
    )
