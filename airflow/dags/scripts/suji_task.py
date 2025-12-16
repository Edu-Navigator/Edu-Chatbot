from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd

@task
def suji_task():
    url = "https://www.sujigu.go.kr/lmth/07_itedu01_connect_01.asp"

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    
    wait = WebDriverWait(driver, 10) 

    records = []

    # 1) 상세 페이지 파싱 함수
    def parse_detail_page(link):
        driver.get(link)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.listRead table tbody")))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.select_one("div.listRead table tbody")

        trs = table.select("tr")
        record = {"APLY_URL": link}

        for tr in trs:
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td:
                key = th.get_text(strip=True)
                value = td.get_text(" ", strip=True)
                record[key] = value

        return record


    # 2) 진행중/전 페이지 확인
    soup = BeautifulSoup(driver.page_source, "html.parser")
    no_data = soup.select_one("td.noData")

    if not no_data:
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tbody tr td.subject a")))
        elements = driver.find_elements(By.CSS_SELECTOR, "tbody tr td.subject a")
        links_now = [el.get_attribute("href") for el in elements]

        # 상세 페이지 파싱
        for link in links_now:
            records.append(parse_detail_page(link))


    # 3) 진행마감 탭 이동 + 링크 수집 + 상세 페이지 파싱
    select_box = Select(driver.find_element(By.ID, "strSearch_part"))
    select_box.select_by_value("end")
    
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tbody tr td.subject a"))) 

    elements = driver.find_elements(By.CSS_SELECTOR, "tbody tr td.subject a")
    links_end = [el.get_attribute("href") for el in elements]

    for link in links_end:
        records.append(parse_detail_page(link))

    driver.quit()


    # 4) DataFrame 생성 + 컬럼명 매핑
    mapping = {
        "강좌명": "LCTR_NM",
        "강사": "LCTR_TEACHER",
        "신청": "REGISTER_PSCP",
        "강의기간": "LCTR_BGN_END",
        "교육시간": "LCTR_TIME",
        "접수기간": "APLY_BGN_END",
        "접수상태": "APLY_STATUS",
        "거주지 제한": "APLY_LOCATE_LIMIT",
        "교육대상": "LCTR_TARGET",
        "교육장소": "LCTR_LOCATE",
        "자세한 내용": "DETAIL"
    }

    df = pd.DataFrame(records)
    df = df.rename(columns=mapping)

    ordered_cols = [
        "APLY_URL",
        "LCTR_NM",
        "LCTR_TEACHER",
        "REGISTER_PSCP",
        "LCTR_BGN_END",
        "LCTR_TIME",
        "APLY_BGN_END",
        "APLY_STATUS",
        "APLY_LOCATE_LIMIT",
        "LCTR_TARGET",
        "LCTR_LOCATE",
        "DETAIL"
    ]

    df = df.reindex(columns=ordered_cols)

    # 5) Snowflake에 데이터 적재
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # DATABASE_NAME = "DIGIEDU"
    # SCHEMA_NAME = "RAW_DATA"

    # cursor.execute(f"USE DATABASE {DATABASE_NAME}") 
    # cursor.execute(f"USE SCHEMA {SCHEMA_NAME}")
    SCHEMA_NAME = "RAW_DATA"
    table_name = "SUJI_LEARNING"

    # 테이블 생성/초기화
    # cursor.execute(f"""
    #     CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table_name} (
    #         APLY_URL STRING,
    #         LCTR_NM STRING,
    #         LCTR_TEACHER STRING,
    #         REGISTER_PSCP STRING,
    #         LCTR_BGN_END STRING,
    #         LCTR_TIME STRING,
    #         APLY_BGN_END STRING,
    #         APLY_STATUS STRING,
    #         APLY_LOCATE_LIMIT STRING,
    #         LCTR_TARGET STRING,
    #         LCTR_LOCATE STRING,
    #         DETAIL STRING
    #     )
    # """)
    # cursor.execute(f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name}")
    
    # 테이블 full refresh
    cursor.execute(
            f"DELETE FROM {SCHEMA_NAME}.{table_name}"
        )

    # 데이터프레임을 튜플 리스트로 변환
    data_to_insert = [
        tuple(row[col] if pd.notnull(row[col]) else None for col in ordered_cols)
        for _, row in df.iterrows()
    ]

    placeholders = ','.join(['%s'] * len(ordered_cols))
    insert_query = f"INSERT INTO {SCHEMA_NAME}.{table_name} VALUES ({placeholders})"
    
    if data_to_insert: # 데이터가 있을 경우에만 실행
        cursor.executemany(insert_query, data_to_insert)

    cursor.close()
    conn.commit()
    conn.close()

    return f"Suji task completed: {len(df)} records processed."