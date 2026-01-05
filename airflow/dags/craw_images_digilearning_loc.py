
"""
디지털 배움터의 건물 이미지를 크롤링하여 S3에 저장한다.

이 DAG는 ‘디지털 배움터’의 배움터 정보를 크롤링하여 건물명과 이미지 URL을 수집하고,
S3에 저장합니다.

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
scrape_images
    이미지 정보(경로, title명) 수집
upload_to_s3
    s3에 데이터 적재

Dependencies
------------
scrape_images >> upload_to_s3

"""

from datetime import datetime
from airflow import DAG
import pendulum

from scripts.craw_imgaes import *
from common.default_args import DEFAULT_ARGS


with DAG(
    dag_id = '01_crawling_images_digilearning',
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="15 10 * * *", # start_date의 tz 기준 오전 10시 15분 실행
    catchup = False,
    tags=['01', 's3', "images"],
    default_args=DEFAULT_ARGS,
) as dag :
    
    res_scrape  = scape_images(
        location_url = "https://www.xn--2z1bw8k1pjz5ccumkb.kr/edc/crse/place.do",
        wait_time = 3
    )
    
    upload_to_s3(
        res_scrape,
        bucket='team7-batch',
        base_key="images/crawling/digital_learning",
        batch_size=12,
        delay=5,
        conn_name='aws_default'
    )