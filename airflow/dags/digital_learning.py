# airflow/dags/digital_learning.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import time

def collect_list(**context):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    def get_driver():
        chrome_options = Options()
        chrome_options.binary_location = "/usr/bin/chromium"
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")

        service = Service("/usr/bin/chromedriver")
        return webdriver.Chrome(service=service, options=chrome_options)


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
    finally:
        driver.quit()

    df = pd.DataFrame(rows)
    path = "/opt/airflow/data/list.csv"
    df.to_csv(path, index=False)
    context["ti"].xcom_push(key="list_path", value=path)

def collect_detail(**context):
    import requests
    from bs4 import BeautifulSoup

    list_path = context["ti"].xcom_pull(key="list_path")
    df_list = pd.read_csv(list_path)

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

    df = pd.DataFrame(rows)
    path = "/opt/airflow/data/detail.csv"
    df.to_csv(path, index=False)
    context["ti"].xcom_push(key="detail_path", value=path)

def save_csv(**context):
    df = pd.read_csv(context["ti"].xcom_pull(key="detail_path"))

    def classify(status):
        return "ing" if "모집" in status or "접수" in status else "end"

    df["group"] = df["상태"].apply(classify)
    today = datetime.today().strftime("%Y%m%d")

    df[df["group"] == "ing"].to_csv(
        f"/opt/airflow/data/digital_ing_{today}.csv",
        index=False, encoding="utf-8-sig"
    )

    df[df["group"] == "end"].to_csv(
        f"/opt/airflow/data/digital_end_{today}.csv",
        index=False, encoding="utf-8-sig"
    )
    
with DAG(
    dag_id="digital_learning_crawl",
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["crawl"],
) as dag:

    t1 = PythonOperator(task_id="collect_list", python_callable=collect_list)
    t2 = PythonOperator(task_id="collect_detail", python_callable=collect_detail)
    t3 = PythonOperator(task_id="save_csv", python_callable=save_csv)

    t1 >> t2 >> t3
