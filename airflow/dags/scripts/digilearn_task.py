from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import time
import requests
from bs4 import BeautifulSoup

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from utils.webdriver import get_driver

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

DIGI_END_COLNAME_MAP = {
    "교육명" : "LCTR_NM",
    "url"   : "APLY_URL",
    "강의유형" : "LCTR_TYPE", # ?
    "교육방식" : "LCTR_WAY",
    "과정구분" : "LCTR_CATEGORY",
    "역량" : "LCTR_ABILITY",
    "교육기간" : "LCTR_BGN_END",
    "교육시간" : "LCTR_TIME",
    "교육일정" : "LCTR_SCHEDULE",
    "교육내용" : "LCTR_CONTENT",
    "강사명" : "LCTR_TEACHER",
    "보조강사/가이드명" : "LCTR_SUB_TEACHER",
    "담당 배움터" : "DL_CHARGE",
    "실제 교육장소" : "DL_NM",
    "group" : "LCTR_STATUS"
}

@task
def collect_list(**context):

    def make_detail_url(edc_oprtn_id):
        return (
            "https://www.xn--2z1bw8k1pjz5ccumkb.kr/edc/crse/oprtn/dtl.do"
            f"?edc_oprtn_id={edc_oprtn_id}&edu_type=E&active_tab=a_end"
        )

    AREAS = {"서울": "101", "경기도": "203"}

    today = datetime.today()
    begin_date = (today - relativedelta(months=1)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")

    BASE_URL = (
        "https://www.xn--2z1bw8k1pjz5ccumkb.kr/edc/crse/oprtn/list.do"
        "?active_tab={tab}&pno={pno}&punit=5&psize=10"
        "&sch_edc_bgn_dt={bgn}&sch_edc_end_dt={end}"
        "&sch_area_cd={area}"
    )

    driver = get_driver()
    rows = []

    try:
        for area_name, area_cd in AREAS.items():
            for tab in ["a_ing", "a_end"]:
                pno = 1
                while True:
                    url = BASE_URL.format(
                        tab=tab,
                        pno=pno,
                        bgn=begin_date,
                        end=end_date,
                        area=area_cd
                    )
                    driver.get(url)

                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".edu_list"))
                    )
                    time.sleep(1)

                    items = driver.find_elements(By.CSS_SELECTOR, "div.edulistel")
                    if not items:
                        break

                    for item in items:
                        edc_id = item.get_attribute("data-edc_oprtn_id")
                        if not edc_id:
                            continue

                        title = item.find_element(
                            By.CSS_SELECTOR, "a.title span.tit"
                        ).text.strip()

                        status = item.find_element(
                            By.CSS_SELECTOR, "span.cateTag"
                        ).text.strip()

                        rows.append({
                            "교육명": title,
                            "상태": status,
                            "url": make_detail_url(edc_id)
                        })
                    pno += 1
                    if pno % 10 == 0 :
                        logging.info(f"[@@] Extracted : {area_name} .... Page Number {pno}")
    finally:
        driver.quit()

    df = pd.DataFrame(rows)
    path = f"{Variable.get('DATA_DIR')}/list.csv"
    df.to_csv(path, index=False)
    logging.info(f"[@@] Saved CSV File in local : {path}")
    # context["ti"].xcom_push(key="list_path", value=path)
    return path

@task
def collect_detail(input_path, **context):
    df_list = pd.read_csv(input_path)

    logging.info(f"[@@] Start - get details of digital learning. shape of data = {df_list.shape}")
    rows = []
    for _, row in df_list.iterrows():
        res = requests.get(row["url"], timeout=10)
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")

        detail = {}
        for tr in soup.select("table tr"):
            th, td = tr.select_one("th"), tr.select_one("td")
            if th and td:
                detail[th.text.strip()] = td.text.strip()

        rows.append({**row.to_dict(), **detail})
        time.sleep(0.2)
    logging.info(f"[@@] End - get details of digital learning ")
    
    df = pd.DataFrame(rows)
    path = f"{Variable.get('DATA_DIR')}/detail.csv"
    df.to_csv(path, index=False)
    logging.info(f"[@@] Saved CSV File in local : {path}")
    # context["ti"].xcom_push(key="detail_path", value=path)
    return path

@task
def transform_data(input_path,**context):
    df = pd.read_csv(input_path)

    def classify(status):
        return "ing" if "모집" in status or "접수" in status else "end"

    df["group"] = df["상태"].apply(classify)
    
    
    # 컬럼명 변경 및 적재할 컬럼만 수집
    df = df.rename(columns = DIGI_END_COLNAME_MAP)[list(DIGI_END_COLNAME_MAP.values())]
    path_end = f"{Variable.get('DATA_DIR')}/digital_end_{context['ds']}.csv"
    logging.info(f"[@@] End - final CSV file(DIGI_END) saved in local")
    
    df.to_csv(path_end, index=False, encoding="utf-8-sig")
    # context["ti"].xcom_push(key="end_path", value=path_end)
    return path_end
    
@task
def load_data_to_snowflake(input_path, schema, table, conn_name="snowflake_conn", **context):
    df = pd.read_csv(input_path)
    df = df.where(pd.notnull(df), None)

    hook = SnowflakeHook(snowflake_conn_id=conn_name)
    conn, cursor = None, None
    try :
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("BEGIN")

        ## Full refresh
        # DELETE
        cursor.execute(
            f"DELETE FROM {schema}.{table}"
        )
        logging.info(f"[삭제 - {schema}.{table}] {cursor.rowcount}개 행")
        
        # INSERT (executemany 사용 - 가장 안전)
        insert_query = f"""
        INSERT INTO {schema}.{table} ({', '.join(df.columns.tolist())})
        VALUES ({', '.join(['%s'] * len(df.columns))})
        """
        cursor.executemany(insert_query, df.values.tolist())
        logging.info(f"[삽입 - {schema}.{table}] {cursor.rowcount}개 행")
        
        cursor.execute("COMMIT")
        logging.info(f"[종료] {schema}.{table} : 완료")
    except Exception as e:
        logging.error(f"[오류 발생] {schema}.{table} : {type(e).__name__} - {str(e)}")
        cursor.execute("ROLLBACK")
        logging.info(f"[롤백 완료]")
    finally :
        cursor.close()
        conn.close()