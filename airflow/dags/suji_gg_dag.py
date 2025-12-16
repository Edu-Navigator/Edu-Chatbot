from airflow import DAG
from datetime import datetime
from scripts.gg_task import *
from scripts.suji_task import *

# DAG
with DAG(
    dag_id="suji_gg_pipeline",
    start_date=datetime(2025, 12, 9),
    schedule_interval=None,
    catchup=False,
    tags=["suji", "gyeonggi"]
):

    suji = suji_task()
    gg = gg_task()
    suji >> gg