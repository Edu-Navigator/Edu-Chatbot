"""
최신 모델이 존재하는지 확인 후 모델을 호출하여 예측 결과를 생성하고 적재한다.

이 DAG는 S3의 최신 모델 pkl 파일이 있는지 확인하고 없다면 다음 task들을 Skip합니다.
파일이 있다면 최신 모델을 로드하여 현재 일자 기준 미래 30일의 강의 시작일자를 추론한 뒤,
추론된 결과 후처리 후 결과를 저장합니다.

Parameters
----------
schedule_interval : str
    매일 오전 10시 35분 ('35 10 * * *')
start_date : datetime
    Starting date for the DAG (2025-12-01)
catchup : bool
    False to prevent backfilling historical runs
tags : list of str
    ["03", "predict", "prophet"]

Tasks
-----
check_latest_model
    최신 모델 존재 확인
predict_prophet
    예측 및 후처리 수행
table_full_refresh
    테이블에 full refresh로 저장

Dependencies
------------
check_latest_model >> predict_prophet >> table_full_refresh

See Also
--------
03_weekly_train_prophet : prophet 훈련 DAG

"""

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime
import pendulum

from scripts.modeling_prophet import check_latest_model_exists, predict_prophet
from scripts.postgres import table_full_refresh
from common.default_args import DEFAULT_ARGS

with DAG(
    dag_id="03_daily_predict_prophet",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="35 10 * * *", # start_date의 tz 기준 오전 10시 35분 실행
    catchup=False,
    tags=["03", "predict", "prophet"],
    default_args=DEFAULT_ARGS,
) as dag:

    # latest에 pkl 파일이없으면 다음 task 모두 skip
    check_model = ShortCircuitOperator(
        task_id="check_latest_model",
        python_callable=check_latest_model_exists,
        op_kwargs = { 
            'bucket': 'team7-batch',
            'prefix': "models/prophet/latest/",
            'conn_name': "aws_default",
        }
    )

    predict = predict_prophet(
        bucket = 'team7-batch',
        prefix = "models/prophet/latest/",
        conn_name = "aws_default",
    )
    
    load = table_full_refresh(
        data = predict, 
        schema = 'analytics', 
        table  = 'pred_aply_edu', 
        conn_name="conn_production")


    check_model >> predict >> load