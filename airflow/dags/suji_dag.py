from airflow import DAG
import pendulum

from scripts.suji_crawl_task import suji_crawl_task
from scripts.suji_load_task import suji_load_task


with DAG(
    dag_id="01_suji_pipeline",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="10 10 * * *", # start_date의 tz 기준 오전 10시 10분 실행
    catchup=False,
    tags=['01', "suji", "gyeonggi"],
) as dag:

    suji_crawl_path = suji_crawl_task()
    suji_load = suji_load_task(
        suji_crawl_path,
        schema='RAW_DATA', 
        table='SUJI_LEARNING')
    
