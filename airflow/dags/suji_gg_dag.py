from airflow import DAG
from datetime import datetime
import pendulum

from scripts.gg_crawl_task import gg_crawl_task
from scripts.gg_load_task import gg_load_task
from scripts.suji_crawl_task import suji_crawl_task
from scripts.suji_load_task import suji_load_task


# DAG
with DAG(
    dag_id="01_suji_gg_pipeline",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="0 12 * * *", # start_date의 tz 기준 오전 10시 실행
    catchup=False,
    tags=['01', "suji", "gyeonggi"],
) as dag:

    suji_crawl_path = suji_crawl_task()
    suji_load = suji_load_task(
        suji_crawl_path,
        schema='RAW_DATA', 
        table='SUJI_LEARNING')
    
    gg_api_path = gg_crawl_task()
    gg_load = gg_load_task(
        gg_api_path, 
        schema="RAW_DATA", 
        table="GG_LEARNING"
    )

    suji_load >> gg_api_path