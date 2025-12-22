from airflow.decorators import task
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from airflow.models import Variable

@task
def suji_crawl_task():
    """
    수지구청 평생학습 홈페이지에서 교육 강좌 정보를 크롤링하여 CSV 파일로 저장한다.

    Selenium을 사용하여 수지구청 평생학습 강좌 목록 페이지에 접근한 뒤,
    진행 중인 강좌와 진행 마감된 강좌의 상세 페이지를 순회하며
    강좌 정보를 수집한다. 수집된 데이터는 정규화된 컬럼 구조로 변환하여
    CSV 파일로 저장하고, 저장 경로를 XCom으로 반환한다.

    Returns
    -------
    str
        크롤링 결과가 저장된 CSV 파일의 경로이다.
        downstream task에서 입력 경로로 사용된다.

    Notes
    -----
    - 데이터 출처
        - 수지구청 평생학습 홈페이지
    - 크롤링 방식
        - Selenium(WebDriver) 기반 동적 페이지 크롤링을 수행한다.
        - BeautifulSoup을 이용한 HTML 파싱을 수행한다.
    - 수집 대상
        - 진행 중인 강좌
        - 진행 마감된 강좌
    - 데이터 처리
        - 목록 페이지에서 강좌 목록을 수집한다.
        - 상세 페이지 단위로 추가 정보를 파싱한다.
        - 내부 표준 컬럼 스키마에 맞게 DataFrame을 구성한다.
    - 결과 저장
        - Airflow Variable `DATA_DIR` 경로 하위에
        `suji_crawl_res.csv` 파일로 저장한다.
    - 반환값
        - CSV 파일 경로(str)를 반환한다.
        - downstream task에서 입력 경로로 사용된다.
    """

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
            """
            강좌 상세 페이지를 파싱하여 단일 강좌 정보를 딕셔너리로 반환한다.

            Parameters
            ----------
            link : str
                강좌 상세 페이지 URL.

            Returns
            -------
            dict or None
                강좌 상세 정보가 담긴 딕셔너리.
                페이지 파싱에 실패한 경우 None을 반환한다.
            """
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
    
    path = f"{Variable.get('DATA_DIR')}/suji_crawl_res.csv"
    df.to_csv(path, index=False)

    # XCom 반환
    return path
