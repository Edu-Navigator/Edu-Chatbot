from airflow import DAG
from datetime import datetime
# from airflow.sensors.external_task import ExternalTaskSensor

from scripts.digilearn_task import *


with DAG(
    dag_id="01_digital_learning_crawl_v20251215",
    start_date=datetime(2025, 12, 10),
    schedule = "30 1 * * *", # utc 새벽 1시 = kst 오전 10시
    catchup=False,
    tags=['01', 'raw_data', "digital_learn"],
) as dag:
    
    # wait_collect_location = ExternalTaskSensor(
    #     task_id="wait_01_collect_location_info",
    #     external_dag_id="01_collect_location_info",
    #     external_task_id=None,
    #     allowed_states=["success"],
    #     failed_states=["failed", "skipped"],
    #     mode="reschedule",
    #     poke_interval=60,
    #     timeout=60 * 30,
    # )

    t1 = collect_list()
    t2 = collect_detail(t1)
    t3 = transform_data(t2)
    t4 = load_data_to_table(
        t3, 
        schema = "RAW_DATA", 
        table  = "DIGITAL_LEARNING_END", 
        conn_name = "conn_production"
    )

    # wait_collect_location >> t1