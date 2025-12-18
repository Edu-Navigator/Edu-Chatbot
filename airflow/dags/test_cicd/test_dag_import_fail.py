from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def broken_function():
    raise Exception("DAG import test failure")  # ❌ import 시 에러

with DAG(
    dag_id="test_dag_import_fail",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="broken_task",
        python_callable=broken_function,
    )
