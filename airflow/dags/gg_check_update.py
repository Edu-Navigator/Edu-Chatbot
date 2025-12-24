from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# =====================
# DAG 설정
# =====================
with DAG(
    dag_id="gg_lifelong_learning",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gg", "selenium", "crawl"],
) as dag:

    # =====================
    # 1️⃣ 웹 수정 여부 체크
    # =====================
    @task
    def check_gg_updated():
        url = (
            "https://data.gg.go.kr/portal/data/service/selectServicePage.do"
            "?infId=Q318MFGTWD8D0Q36X8H112883673&infSeq=3"
        )

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
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
            th.find_element(By.XPATH, "./following-sibling::td").text.strip(),
            "%Y-%m-%d",
        ).date()

        driver.quit()

        # DB에서 마지막 수집 기준일 조회
        hook = PostgresHook(postgres_conn_id="conn_production")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT MAX(source_updated_at) FROM raw.gg_lifelong_learning"
        )
        db_date = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        print(f"[웹] 수정일자: {web_date}, [DB] 기준일자: {db_date}")

        if db_date is not None and web_date <= db_date:
            raise AirflowSkipException("변경 없음 → 수집 생략")

        return str(web_date)

    # =====================
    # 2️⃣ 데이터 수집
    # =====================
    @task
    def gg_crawl_task(source_updated_at: str) -> str:
        """
        실제 구현에서는
        - API 호출
        - CSV 저장
        - 경로 반환
        """
        csv_path = f"/opt/airflow/data/gg_lifelong_learning_{source_updated_at}.csv"
        print(f"CSV 생성: {csv_path}")
        return csv_path

    # =====================
    # 3️⃣ DB 적재
    # =====================
    @task
    def gg_load_task(csv_path: str, source_updated_at: str):
        hook = PostgresHook(postgres_conn_id="conn_production")
        conn = hook.get_conn()
        cursor = conn.cursor()

        """
        예시 INSERT
        실제로는 COPY 또는 executemany 사용 추천
        """
        cursor.execute(
            """
            INSERT INTO raw.gg_lifelong_learning (
                col1,
                col2,
                source_updated_at
            )
            VALUES (%s, %s, %s)
            """,
            ("value1", "value2", source_updated_at),
        )

        conn.commit()
        cursor.close()
        conn.close()

        print("DB 적재 완료")

    # =====================
    # Task Flow
    # =====================
    web_date = check_gg_updated()
    csv_path = gg_crawl_task(web_date)
    gg_load_task(csv_path, web_date)
