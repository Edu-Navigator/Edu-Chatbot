# airflow/dags/digital_learning_ci.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import time

def collect_list(**context):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.service import Service

    def get_driver():
        options = Options()
        options.binary_location = "/usr/bin/chromium"
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        return webdriver.Chrome(
            service=Service("/usr/bin/chromedriver"),
            options=options
        )

    def make_detail_url(edc_oprtn_id):
        return (
            "https://www.xn--2z1bw8k1pjz5ccumkb.kr/edc/crse/oprtn/dtl.do"
            f"?edc_oprtn_id={edc_oprtn_id}&edu_type=E&active_tab=a_end"
        )

    driver = get_driver()
    rows = []

    try:
        driver.get("https://example.com")  # CI 안전용
    finally:
        driver.quit()

    df = pd.DataFrame(rows)
    path = "/opt/airflow/data/list.csv"
    df.to_csv(path, index=False)
    context["ti"].xcom_push(key="list_path", value=path)

def collect_detail(**context):
    pass

def save_csv(**context):
    pass

with DAG(
    dag_id="digital_learning_crawl_ci",
    start_date=days_ago(1),
    schedule_interval=None,   
    catchup=False,
    tags=["test", "ci"],
) as dag:


    t1 = PythonOperator(task_id="collect_list", python_callable=collect_list)
    t2 = PythonOperator(task_id="collect_detail", python_callable=collect_detail)
    t3 = PythonOperator(task_id="save_csv", python_callable=save_csv)

    t1 >> t2 >> t3
    
    
