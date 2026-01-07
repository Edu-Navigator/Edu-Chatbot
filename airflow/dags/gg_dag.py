import pendulum
from airflow.operators.empty import EmptyOperator
from scripts.gg_crawl_task import compare_dates, get_max_created_at, get_web_updated_date, gg_crawl_task
from scripts.gg_load_task import gg_load_task

from airflow import DAG

with DAG(
    dag_id="01_gg_pipeline",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, tz=pendulum.timezone("Asia/Seoul")),
    schedule="10 10 * * *",  # start_date의 tz 기준 오전 10시 10분 실행
    catchup=False,
    tags=["01", "suji", "gyeonggi"],
) as dag:
    gg_update_date = get_web_updated_date()
    max_created = get_max_created_at()
    branch = compare_dates(gg_update_date, max_created)

    gg_api_path = gg_crawl_task()
    gg_load = gg_load_task(gg_api_path, schema="RAW_DATA", table="GG_LEARNING")

    end_gg_task = EmptyOperator(
        task_id="end_gg_task",
    )

    branch >> [gg_api_path, end_gg_task]
