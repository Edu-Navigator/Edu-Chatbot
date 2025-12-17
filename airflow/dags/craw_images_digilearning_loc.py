from datetime import datetime
from airflow import DAG

from scripts.craw_imgaes import *

default_args = {
    'owner': 'airflow',
    'email': ['sosoj1552@gmail.com'], # 알림 받을 이메일 주소
    'email_on_failure': True,
    'email_on_success': True,
}

with DAG(
    dag_id = '01_crawling_images_digilearning',
    start_date = datetime(2025, 12, 10),
    schedule = None, # 스케줄 없음
    catchup = False,
    tags=['01', 's3', "images"],
    default_args=default_args
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