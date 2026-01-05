"""
구청의 디지털 관련 교육 정보를 크롤링하고 적재 한다

이 DAG는 용인시 수지구청의 평생학습 강좌 페이지를 크롤링하여 데이터를 수집하고 적재합니다.

Parameters
----------
schedule_interval : str
    매일 오전 10시 10분 ('10 10 * * *')
start_date : datetime
    Starting date for the DAG (2025-12-01)
catchup : bool
    False to prevent backfilling historical runs
tags : list of str
    ['01', "suji", "crawling"]

Tasks
-----
suji_crawl_task
    수지구청 페이지 크롤링
suji_load_task
    테이블에 full refresh로 저장

Dependencies
------------
suji_crawl_task >> suji_load_task

"""

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
    tags=['01', "suji", "crawling"],
) as dag:

    suji_crawl_path = suji_crawl_task()
    suji_load = suji_load_task(
        suji_crawl_path,
        schema='RAW_DATA', 
        table='SUJI_LEARNING')
    
