from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.remote_connection import RemoteConnection
from airflow.dags.common.default_args import DEFAULT_ARGS

def selenium_test2():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://www.naver.com")
    # 페이지 제목 확인
    print("===== PAGE TITLE =====")
    print(driver.title)
    print("======================")

    driver.quit()
    

with DAG(
    dag_id="selenium_test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["test", "selenium"],
    default_args=DEFAULT_ARGS,
):

    test_task = PythonOperator(
        task_id="run_selenium_test",
        python_callable=selenium_test2,
    )