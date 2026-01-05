"""
과거 교육 시작 일자를 기반으로 prophet 모델을 훈련하고 S3에 pkl파일로 저장한다.

이 DAG는 지역, 교육구분별로 모델링을 수행합니다.
그룹의 강의 시작일별 강의 건수를 사용하여 훈련 데이터를 생성하고,
훈련된 모델을 S3에 저장합니다.

Parameters
----------
schedule_interval : str
    매주 월요일 오전 10시 30분 ('30 10 * * 1')
start_date : datetime
    Starting date for the DAG (2025-12-01)
catchup : bool
    False to prevent backfilling historical runs
tags : list of str
    ["03", "train", "prophet"]

Tasks
-----
train_prophet_models
    훈련데이터를 생성 및 모델 훈련

Dependencies
------------
train_prophet_models

See Also
--------
forecast_prophet_dag : prophet 예측 DAG

"""

from airflow import DAG
from datetime import datetime
import pendulum

from scripts.modeling_prophet import train_prophet_models
from common.default_args import DEFAULT_ARGS

with DAG(
    dag_id="03_weekly_train_prophet",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="30 10 * * 1", # start_date의 tz 기준 매주 월요일 10:30 실행
    catchup=False,
    tags=["03", "train", "prophet"],
    default_args=DEFAULT_ARGS,
) as dag:

    traning = train_prophet_models(
        schema = 'processed',
        table  = 'lecture',
        bucket = 'team7-batch',
        s3_base_prefix = "models/prophet",
        s3_conn = "aws_default"
    )
