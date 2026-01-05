"""
타겟 테이블의 최신 생성 일시와 API의 업데이트 일자 비교 로직을 수행하고, 소스에 변경 사항이 발생한 경우 수집을 수행한다.

이 DAG는 경기도 평생학습 크롤링을 통해 데이터 업데이트 일자를 추출하고 타겟 테이블의 max(created_at)값과 비교한 뒤,
데이터 소스가 업데이트 된 경우, 경기도 평생학습 API로 부터 교육 정보를 수집하고 적재합니다.

Parameters
----------
schedule_interval : str
    매일 오전 10시 10분 ('10 10 * * *')
start_date : datetime
    Starting date for the DAG (2025-12-01)
catchup : bool
    False to prevent backfilling historical runs
tags : list of str
    ['01', "gyeonggi", "api"]

Tasks
-----
get_web_updated_date
    경기도 평생학습 API의 업데이트 일자 확인
get_max_created_at
    타겟 테이블의 최신 데이터 생성일자 확인
compare_dates
    데이터 업데이트 일자 비교
end_gg_task
    DAG 종료용 task
gg_crawl_task
    API를 사용해 extract
gg_load_task
    테이블에 full refresh로 저장

Dependencies
------------
[get_web_updated_date, get_max_created_at] >> compare_dates 
>> [end_gg_task | gg_crawl_task >> gg_load_task]

"""

from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator

from scripts.gg_crawl_task import (
    gg_crawl_task,
    get_web_updated_date,
    get_max_created_at,
    compare_dates
    )
from scripts.gg_load_task import gg_load_task


with DAG(
    dag_id="01_gg_pipeline",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="10 10 * * *", # start_date의 tz 기준 오전 10시 10분 실행
    catchup=False,
    tags=['01', "gyeonggi", "api"],
) as dag:


    gg_update_date = get_web_updated_date()
    max_created    = get_max_created_at()
    branch = compare_dates(gg_update_date, max_created)
    
    gg_api_path = gg_crawl_task()
    gg_load = gg_load_task(
        gg_api_path, 
        schema="RAW_DATA", 
        table="GG_LEARNING"
    )
    
    end_gg_task = EmptyOperator(task_id='end_gg_task',)
    
    branch >> [gg_api_path, end_gg_task]
