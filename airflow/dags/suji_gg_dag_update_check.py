from airflow import DAG
from datetime import datetime
import pendulum
from airflow.operators.empty import EmptyOperator
from scripts.gg_crawl_task import gg_crawl_task
from scripts.gg_load_task import gg_load_task
from scripts.suji_crawl_task import suji_crawl_task
from scripts.suji_load_task import suji_load_task

from airflow.decorators import task
from utils.webdriver import get_driver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def get_web_updated_date():
    url = (
        "https://data.gg.go.kr/portal/data/service/selectServicePage.do"
        "?infId=Q318MFGTWD8D0Q36X8H112883673&infSeq=3"
    )
    driver = None
    try:
        driver = get_driver()
        driver.get(url)

        wait = WebDriverWait(driver, 20)
        th = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//th[contains(text(), '최종 수정일자')]")
            )
        )

        update_date_str = th.find_element(By.XPATH, "./following-sibling::td").text
        print(f"URL의 정보갱신일: {update_date_str}")

        return update_date_str # "%Y-%m-%d"

    finally:
        if driver:
            driver.quit()

@task
def get_max_created_at() :
    hook = PostgresHook(postgres_conn_id='conn_production')
    result = hook.get_first(f"""
        SELECT MAX(created_at) as max_created_at
        FROM raw_data.gg_learning
    """)
        
    max_created_at  = result[0]
    max_created_str = (
        max_created_at
        .astimezone()
        .strftime("%Y-%m-%d")
    )
    print(f"raw_data.gg_learning 의 MAX(created_at) : {max_created_str}")
    return max_created_str

@task.branch
def compare_dates(update_dt_str, max_created_at_str):
    update_dt      = datetime.fromisoformat(update_dt_str)
    max_created_at = datetime.fromisoformat(max_created_at_str)
    
    print(f"우리동네 디지털 안내소의 정보 갱신일 : {update_dt}")
    print(f"대상 테이블의 Max created_at: {max_created_at}")
    
    # 수행할 task_id를 return 한다.
    if update_dt > max_created_at:
        print("데이터 업데이트 필요!")
        return 'gg_crawl_task'
    else:
        print("데이터 최신 상태")
        return 'skip_task'

# DAG
with DAG(
    dag_id="01_suji_gg_pipeline_check_update",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="10 10 * * *", # start_date의 tz 기준 오전 10시 10분 실행
    catchup=False,
    tags=['01', "suji", "gyeonggi"],
) as dag:

    # suji_crawl_path = suji_crawl_task()
    # suji_load = suji_load_task(
    #     suji_crawl_path,
    #     schema='RAW_DATA', 
    #     table='SUJI_LEARNING')
    
    gg_update_date = get_web_updated_date()
    max_created    = get_max_created_at()
    branch = compare_dates(gg_update_date, max_created)
    
    # gg_api_path = gg_crawl_task()
    # gg_load = gg_load_task(
    #     gg_api_path, 
    #     schema="RAW_DATA", 
    #     table="GG_LEARNING"
    # )
    gg_api_path = EmptyOperator(
        task_id='gg_api_path',
    )
    end_gg_task = EmptyOperator(
        task_id='end_gg_task',
    )
    
    branch >> [gg_api_path, end_gg_task]

    