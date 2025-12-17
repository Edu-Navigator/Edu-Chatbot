from airflow.decorators import task
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd

@task
def suji_crawl_task():
    url = "https://www.sujigu.go.kr/lmth/07_itedu01_connect_01.asp"

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = None
    records = []

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        wait = WebDriverWait(driver, 10)

        def parse_detail_page(link):
            try:
                driver.get(link)
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.listRead table tbody")
                    )
                )

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

            except Exception as e:
                # 상세 페이지 하나 실패해도 전체 중단 방지
                print(f"[WARN] 상세 페이지 파싱 실패: {link} / {e}")
                return None

        # 진행중
        soup = BeautifulSoup(driver.page_source, "html.parser")
        if not soup.select_one("td.noData"):
            wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "tbody tr td.subject a")
                )
            )
            links_now = [
                el.get_attribute("href")
                for el in driver.find_elements(By.CSS_SELECTOR, "tbody tr td.subject a")
            ]

            for link in links_now:
                record = parse_detail_page(link)
                if record:
                    records.append(record)

        # 진행마감
        select_box = Select(driver.find_element(By.ID, "strSearch_part"))
        select_box.select_by_value("end")

        wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "tbody tr td.subject a")
            )
        )

        links_end = [
            el.get_attribute("href")
            for el in driver.find_elements(By.CSS_SELECTOR, "tbody tr td.subject a")
        ]

        for link in links_end:
            record = parse_detail_page(link)
            if record:
                records.append(record)

    finally:
        if driver:
            driver.quit()

    # DataFrame 정리
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
        "자세한 내용": "DETAIL",
    }

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
        "DETAIL",
    ]

    df = pd.DataFrame(records).rename(columns=mapping)
    df = df.reindex(columns=ordered_cols)

    # XCom 반환
    return df.to_dict(orient="records")
