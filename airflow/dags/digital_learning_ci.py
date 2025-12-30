# airflow/dags/digital_learning_ci.py
"""
디지털 학습 강의 크롤링 DAG

이 DAG는 특정 교육 사이트에서 강의 정보를 수집하여 통합 CSV 파일을 생성합니다.
1. 강의 목록 수집 (collect_list)
2. 상세 정보 수집 (collect_detail)
3. CSV 파일 저장 (save_csv)
"""

# CI 환경에서는 실제 크롤링을 수행하지 않고,
# DAG import 및 태스크 실행 가능 여부만 검증한다.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime
from dateutil.relativedelta import relativedelta
from common.default_args import DEFAULT_ARGS
import pandas as pd
import time

def collect_list(**context):
    """
    교육 과정 목록 페이지에서 강의 리스트를 수집한다.
    
    Selenium을 사용하여 목록 페이지에 접근한 뒤,
    수집 결과를 CSV 파일로 저장하고 파일 경로를 XCom으로 전달한다.
    CI 환경에서는 실제 크롤링 대신 페이지 접근 여부만 검증한다.
    
    Parameters
    ------------
    **context : dict
        Airflow task context 객체.
        XCom push를 위해 task instance(`ti`)를 포함한다.

    Returns
    -------
    None
        결과는 CSV 파일로 저장되며,
        파일 경로는 XCom을 통해 다음 태스크로 전달된다.
    
    :param context: 설명
    """

    # CI 환경에서는 실제 데이터 수집 대신 드라이버 동작만 검증  
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.service import Service

    def get_driver():
        """
        Selenium Chrome WebDriver를 생성한다.

        Returns
        -------
        selenium.webdriver.Chrome
            Headless 모드로 설정된 Chrome WebDriver 객체
        """
        
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
        """
        강의 상세 페이지 URL을 생성한다.

        Parameters
        ----------
        edc_oprtn_id : str
            강의 운영 ID

        Returns
        -------
        str
            강의 상세 페이지 URL
        """
        
        return (
            "https://www.xn--2z1bw8k1pjz5ccumkb.kr/edc/crse/oprtn/dtl.do"
            f"?edc_oprtn_id={edc_oprtn_id}&edu_type=E&active_tab=a_end"
        )

    driver = get_driver()
    rows = []

    # CI 환경에서는 외부 사이트 접근을 피하기 위해
    # 실제 대상 사이트 대신 example.com으로 드라이버 동작만 확인
    try:
        driver.get("https://example.com")  # CI 안전용
    finally:
        driver.quit()

    df = pd.DataFrame(rows)
    path = "/opt/airflow/data/list.csv"
    df.to_csv(path, index=False)
    context["ti"].xcom_push(key="list_path", value=path)


def collect_detail(**context):
    """
    강의 상세 페이지 정보를 수집한다.

    이전 태스크에서 전달받은 강의 목록을 기반으로
    각 강의의 상세 정보를 수집한다.

    Parameters
    ----------
    **context : dict
        Airflow task context 객체.
        이전 태스크에서 전달된 XCom 데이터를 포함한다.

    Returns
    -------
    None
        상세 정보는 XCom 또는 임시 파일로 저장된다.

    Raises
    ------
    NotImplementedError
        상세 수집 로직이 아직 구현되지 않은 경우
    """
    
    # CI 테스트 단계에서는 미구현 (DAG import / task 연결 검증용)
    pass

def save_csv(**context):
    """
    수집된 데이터를 하나의 CSV 파일로 저장한다.

    이전 태스크들에서 전달된 데이터를 병합하여
    최종 CSV 파일을 생성한다.

    Parameters
    ----------
    **context : dict
        Airflow task context 객체.
        수집된 데이터 경로를 XCom으로 전달받는다.

    Returns
    -------
    None
        최종 CSV 파일을 지정된 경로에 저장한다.
    """
    
    # CI 테스트 단계에서는 미구현 (DAG import / task 연결 검증용)
    pass


# CI 전용 DAG: 수동 트리거 기반, 스케줄 없음
with DAG(
    dag_id="digital_learning_crawl_ci",
    start_date=days_ago(1),
    schedule_interval=None,   
    catchup=False,
    tags=["test", "ci"],
    default_args=DEFAULT_ARGS,
) as dag:


    t1 = PythonOperator(task_id="collect_list", python_callable=collect_list)
    t2 = PythonOperator(task_id="collect_detail", python_callable=collect_detail)
    t3 = PythonOperator(task_id="save_csv", python_callable=save_csv)

    t1 >> t2 >> t3
    
    
