from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "email": ["symoon1007@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

def fail_task():
    raise ValueError("의도적인 실패 테스트")

with DAG(
    dag_id="test_failure_email",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    fail = PythonOperator(
        task_id="fail_task",
        python_callable=fail_task,
    )
