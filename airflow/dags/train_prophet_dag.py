from airflow import DAG
from datetime import datetime
import pendulum

from scripts.modeling_prophet import train_prophet_models

with DAG(
    dag_id="03_weekly_train_prophet",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="0 13 * * 1", # start_date의 tz 기준 매주 월요일 011:00 "0 11 * * 1"
    catchup=False,
    tags=["03", "train", "prophet"],
) as dag:

    traning = train_prophet_models(
        schema = 'processed',
        table  = 'lecture',
        bucket = 'team7-batch',
        s3_base_prefix = "models/prophet",
        s3_conn = "aws_default"
    )
