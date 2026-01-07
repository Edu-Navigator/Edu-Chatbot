from datetime import datetime

from airflow.operators.python import PythonOperator
from common.default_args import DEFAULT_ARGS
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from airflow import DAG


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
