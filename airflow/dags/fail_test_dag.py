from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

def fail_task():
    raise Exception("의도적으로 실패")

with DAG(
    dag_id="test_fail_email_operator",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    fail = PythonOperator(
        task_id="fail_task",
        python_callable=fail_task,
    )

    notify = EmailOperator(
        task_id="send_fail_email",
        to=["symoon1007@gmail.com"],
        subject="[Airflow] DAG 실패 알림",
        html_content="""
        <h3>DAG 실패</h3>
        <p>DAG: {{ dag.dag_id }}</p>
        <p>Task: {{ task.task_id }}</p>
        <p>Execution: {{ ts }}</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    fail >> notify
