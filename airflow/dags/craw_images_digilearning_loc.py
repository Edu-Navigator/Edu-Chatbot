from datetime import datetime
from airflow import DAG
import pendulum

from scripts.craw_imgaes import *


with DAG(
    dag_id = '01_crawling_images_digilearning',
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="00 15 * * *", # start_date의 tz 기준 오전 10시 실행
    catchup = False,
    tags=['01', 's3', "images"],
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