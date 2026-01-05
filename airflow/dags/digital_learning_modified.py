"""
디지털배움터 강의 데이터를 수집하여 적재하는 DAG.

이 DAG는 디지털배움터 사이트에서 강의 목록과 상세 정보를 수집하고,
데이터를 정제한 뒤 PostgreSQL RAW_DATA 스키마에 Full Refresh 방식으로 적재한다.

Parameters
----------
schedule_interval : str
    매일 오전 10시 ('00 10 * * *')
start_date : datetime
    DAG 시작일 (2025-12-01)
catchup : bool
    False (과거 실행 방지)
tags : list of str
    ["01", "raw_data", "digital_learn"]

Tasks
-----
collect_list
    디지털배움터 강의 목록 수집 (Selenium)
collect_detail
    강의 상세 정보 수집 (requests + BeautifulSoup)
transform_data
    데이터 정제 및 컬럼 표준화
load_data_to_table
    PostgreSQL 테이블 Full Refresh 적재

Dependencies
------------
collect_list
    >> collect_detail
    >> transform_data
    >> load_data_to_table

Target Table
------------
RAW_DATA.DIGITAL_LEARNING_END
"""

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