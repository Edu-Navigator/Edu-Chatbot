from airflow import DAG
from datetime import datetime
# from airflow.sensors.external_task import ExternalTaskSensor

from scripts.gg_crawl_task import gg_crawl_task
from scripts.gg_load_task import gg_load_task
from scripts.suji_crawl_task import suji_crawl_task
from scripts.suji_load_task import suji_load_task


# DAG
with DAG(
    dag_id="01_suji_gg_pipeline",
    start_date=datetime(2025, 12, 9),
    schedule_interval="0 1 * * *", # utc 새벽 1시 = kst 오전 10시
    catchup=False,
    tags=['01', "suji", "gyeonggi"],
) as dag:
    
    # wait_digital_learning = ExternalTaskSensor(
    #     task_id="wait_01_digital_learning_crawl",
    #     external_dag_id="01_digital_learning_crawl_v20251215",
    #     external_task_id=None,
    #     allowed_states=["success"],
    #     failed_states=["failed", "skipped"],
    #     mode="reschedule",
    #     poke_interval=60,
    #     timeout=60 * 30,
    # )

    suji_crawl_path = suji_crawl_task()
    suji_load = suji_load_task(
        suji_crawl_path,
        schema='RAW_DATA', 
        table='SUJI_LEARNING')
    
    gg_api_path = gg_crawl_task()
    gg_load = gg_load_task(
        gg_api_path, 
        schema="RAW_DATA", 
        table="GG_LEARNING"
    )

    suji_load >> gg_api_path

    # wait_digital_learning >> suji_crawl_path