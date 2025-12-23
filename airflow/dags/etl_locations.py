from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
import pendulum

from scripts.postgres import table_full_refresh
from scripts.extract_locations import *
from airflow.dags.common.default_args import DEFAULT_ARGS


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