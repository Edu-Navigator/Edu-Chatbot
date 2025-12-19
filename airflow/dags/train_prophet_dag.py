from airflow import DAG
from datetime import datetime

from scripts.modeling_prophet import train_prophet_models

with DAG(
    dag_id="03_weekly_train_prophet",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # 매주 월요일 011:00 "0 11 * * 1"
    catchup=False,
    tags=["03", "train", "prophet"],
) as dag:

    train_prophet_models(
        schema = 'processed',
        table  = 'lecture',
        bucket = 'team7-batch',
        s3_base_prefix = "models/prophet",
        s3_conn = "aws_default"
    )
