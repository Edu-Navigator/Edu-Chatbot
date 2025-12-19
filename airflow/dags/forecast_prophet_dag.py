from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime

from scripts.modeling_prophet import check_latest_model_exists, predict_prophet
from scripts.postgres import table_full_refresh


with DAG(
    dag_id="03_daily_predict_prophet",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # "0 12 * * *" 매일 12시 
    catchup=False,
    tags=["03", "predict", "prophet"],
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