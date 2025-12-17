from airflow import DAG
from datetime import datetime
from scripts.gg_crawl_task import gg_crawl_task
from scripts.gg_load_task import gg_load_task
from scripts.suji_crawl_task import suji_crawl_task
from scripts.suji_load_task import suji_load_task

# DAG
with DAG(
    dag_id="suji_gg_pipeline",
    start_date=datetime(2025, 12, 9),
    schedule_interval=None,
    catchup=False,
    tags=["suji", "gyeonggi"]
):

    suji_crawl = suji_crawl_task()
    suji_load = suji_load_task(suji_crawl)
    gg_crawl = gg_crawl_task()
    gg_load = gg_load_task(gg_crawl)

    suji_load >> gg_crawl