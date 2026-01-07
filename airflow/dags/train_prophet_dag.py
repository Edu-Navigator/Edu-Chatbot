import pendulum
from common.default_args import DEFAULT_ARGS
from scripts.modeling_prophet import train_prophet_models

from airflow import DAG

with DAG(
    dag_id="03_weekly_train_prophet",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, tz=pendulum.timezone("Asia/Seoul")),
    schedule="30 10 * * 1",  # start_date의 tz 기준 매주 월요일 10:30 실행
    catchup=False,
    tags=["03", "train", "prophet"],
    default_args=DEFAULT_ARGS,
) as dag:
    traning = train_prophet_models(
        schema="processed",
        table="lecture",
        bucket="team7-batch",
        s3_base_prefix="models/prophet",
        s3_conn="aws_default",
    )
