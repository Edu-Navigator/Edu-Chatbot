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