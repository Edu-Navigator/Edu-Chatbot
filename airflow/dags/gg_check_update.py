from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, date

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


with DAG(
    dag_id="gg_learning",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gg", "selenium", "crawl"],
) as dag:

    # 웹 수정일자 조회
    @task
    def get_web_updated_date() -> date:
        url = (
            "https://data.gg.go.kr/portal/data/service/selectServicePage.do"
            "?infId=Q318MFGTWD8D0Q36X8H112883673&infSeq=3"
        )

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = None
        try:
            driver = webdriver.Chrome(
                service=Service("/usr/bin/chromedriver"), 
                options=options,
            )
            driver.get(url)

            wait = WebDriverWait(driver, 20)
            th = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//th[contains(text(), '최종 수정일자')]")
                )
            )

            web_date = datetime.strptime(
                th.find_element(By.XPATH, "./following-sibling::td")
                .text.strip(),
                "%Y-%m-%d",
            ).date()

            return web_date

        finally:
            if driver:
                driver.quit()


    # DB 기준일 조회
    @task
    def get_db_updated_date() -> date | None:
        hook = PostgresHook(postgres_conn_id="conn_production")
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT MAX(source_updated_at) FROM raw.gg_lifelong_learning"
                )
                result = cursor.fetchone()[0]

        if result:
            return result.date()
        return None

    # 최신성 판단 (Branch)
    @task.branch
    def branch_if_updated(web_date: date, db_date: date | None) -> str:
        print(f"[웹] {web_date}, [DB] {db_date}")

        if db_date is not None and web_date <= db_date:
            return "skip_task"

        return "gg_crawl_task"

    # 수집 Skip Task
    @task
    def skip_task():
        raise AirflowSkipException("변경 없음: 수집 생략")

    # 데이터 수집
    @task
    def gg_crawl_task(web_date: date) -> str:
        csv_path = f"/tmp/gg_lifelong_learning_{web_date}.csv"
        print(f"CSV 생성: {csv_path}")

        # TODO: 실제 크롤링 로직
        return csv_path
    
    # DB 적재
    @task
    def gg_load_task(csv_path: str, web_date: date):
        hook = PostgresHook(postgres_conn_id="conn_production")
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO raw.gg_lifelong_learning (
                        col1,
                        col2,
                        source_updated_at
                    )
                    VALUES (%s, %s, %s)
                    """,
                    ("value1", "value2", web_date),
                )
            conn.commit()

        print("DB 적재 완료")

    # =====================
    # Task Flow
    # =====================
    web_date = get_web_updated_date()
    db_date = get_db_updated_date()

    branch = branch_if_updated(web_date, db_date)

    csv_path = gg_crawl_task(web_date)
    load = gg_load_task(csv_path, web_date)

    branch >> skip_task()
    branch >> csv_path
