"""
오프라인 교육장 관련 데이터 소스로부터 데이터를 수집하고, 전처리(위도, 경도 변환) 수행 후 적재.

이 DAG는 2개 데이터 소스로 데이터를 extract하고 적재한다.
- 디지털 배움터 교육장 현황을 공공 API로 부터 수집 및 적재
- 우리동네 디지털 안내소 현황을 S3의 csv 파일로 부터 로드 및 적재
수집된 데이터의 주소를 기준으로 카카오맵 API를 활용해 주소 좌표 변환 처리합니다.

Parameters
----------
schedule_interval : str
    매일 오전 10시 15분 ('15 10 * * *')
start_date : datetime
    Starting date for the DAG (2025-12-01)
catchup : bool
    False to prevent backfilling historical runs
tags : list of str
    ['01', 's3', "images"]

Tasks
-----
api_digi_learning_loc
    디지털 배움터 현황 수집
et_urdn_location
    우리동네 디지털 배움터 현황 로드
get_details_location
    주소 좌표 변환 처리
table_full_refresh
    테이블에 full refresh로 저장

Dependencies
------------
[api_digi_learning_loc >> table_full_refresh]
[et_urdn_location >> table_full_refresh] >> [get_details_location, table_full_refresh]

"""

from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
import pendulum

from scripts.postgres import table_full_refresh
from scripts.extract_locations import *
from common.default_args import DEFAULT_ARGS


with DAG(
    dag_id = '01_collect_location_info',
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="15 10 * * *", # start_date의 tz 기준 오전 10시 15분 실행
    catchup = False,
    tags=['01', 'raw_data', "location"],
    default_args=DEFAULT_ARGS,
) as dag :
    
    # 디지털 배움터 교육장 API
    with TaskGroup(group_id="location_digilearning") as tg1:
        task1_1 = api_digi_learning_loc()
        task1_2 = table_full_refresh(
            task1_1,
            schema='raw_data',
            table='DIGITAL_LEARNING_LOCATION',
            conn_name="conn_production"
        )
    
    # 우리동네 디지털 안내소 : S3에 올라간 csv 데이터 추출
    with TaskGroup(group_id="extract_location_urdns") as tg2:
        task2_1 = et_urdn_location(
            csv_key = "rawdata/csv_manual/urdn_digi_edu.csv",
            bucket  = 'team7-batch',
            conn_name = 'aws_default'
        )
        task2_2 = table_full_refresh(
            task2_1,
            schema='raw_data',
            table='URDN_DIGITAL_LOCATION',
            conn_name="conn_production"
        )

    with TaskGroup(group_id="Transform_and_Load_location_urdns") as tg3:
        task_transform = get_details_location(
            schema='raw_data', 
            table='URDN_DIGITAL_LOCATION',
            conn_name='conn_production'
        )
        
        task_load = table_full_refresh(
            task_transform,
            schema='ANALYTICS',
            table='URDN_DIGITAL',
            conn_name="conn_production"
        )
    
    tg2 >> tg3

    # wait_crawling_images >> [tg1, tg2]