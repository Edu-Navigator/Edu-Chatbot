from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime

from scripts.digilearn_task import *

default_args = {
    'owner': 'airflow',
    'email': ['sosoj1552@gmail.com'], # 알림 받을 이메일 주소
    'email_on_failure': True,
    'email_on_success': True,
}

with DAG(
    dag_id="01_digital_learning_crawl_v20251215",
    start_date=datetime(2025, 12, 10),
    schedule = None, # 스케줄 없음
    catchup=False,
    tags=['01', 'raw_data', "digital_learn"],
    default_args=default_args
) as dag:

    t1 = collect_list()
    t2 = collect_detail(t1)
    t3 = transform_data(t2)
    t4 = load_data_to_table(
        t3, 
        schema = "RAW_DATA", 
        table  = "DIGITAL_LEARNING_END", 
        conn_name = "conn_production"
    )

