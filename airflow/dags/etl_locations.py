from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from scripts.snowflake import *
from scripts.extract_locations import *


with DAG(
    dag_id = 'collect_location_info',
    start_date = datetime(2025, 12, 10),
    schedule = None, # 스케줄 없음
    catchup = False
) as dag :
    # 디지털 배움터 교육장 API
    with TaskGroup(group_id="location_digilearning") as tg1:
        task1_1 = api_digi_learning_loc()
        task1_2 = insert_to_snowflake(
            task1_1,
            schema='raw_data',
            table='DIGITAL_LEARNING_LOCATION',
            conn_id="snowflake_conn"
        )
    
    
    # 우리동네 디지털 안내소 : S3에 올라간 csv 데이터 추출
    with TaskGroup(group_id="extract_location_urdns") as tg2:
        task2_1 = et_urdn_location(
            csv_key = "raw_data/csv_manual/urdn_digi_edu.csv",
            bucket  = 'de7-data-bucket',
            conn_name = 's3_conn'
        )
        task2_2 = insert_to_snowflake(
            task2_1,
            schema='raw_data',
            table='URDN_DIGITAL_LOCATION',
            conn_id="snowflake_conn"
        )

    with TaskGroup(group_id="Transform_and_Load_location_urdns") as tg3:
        task_transform = get_details_location(
            schema='raw_data', 
            table='URDN_DIGITAL_LOCATION',
            conn_name='snowflake_conn'
        )
        
        task_load = insert_to_snowflake(
            task_transform,
            schema='ANALYTICS',
            table='URDN_DIGITAL',
            conn_id="snowflake_conn"
        )
    
    tg2 >> tg3