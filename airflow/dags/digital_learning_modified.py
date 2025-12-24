from airflow import DAG
from datetime import datetime
import pendulum

from scripts.digilearn_task import *
from common.default_args import DEFAULT_ARGS


with DAG(
    dag_id="01_digital_learning_crawl",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="00 10 * * *", # start_date의 tz 기준 오전 10시 실행
    catchup=False,
    tags=['01', 'raw_data', "digital_learn"],
    default_args=DEFAULT_ARGS,
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