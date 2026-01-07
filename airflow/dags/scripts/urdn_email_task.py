import logging
from datetime import datetime

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from utils.webdriver import get_driver

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

URDN_UPDATE_ALERT_TEMPLATE = """
<h3>최신 데이터 업데이트 알림 메일입니다.</h3>
<p>다음 URL로 이동하여, 업데이트된 CSV을 다운받고 S3에 업데이트 해주시길 바랍니다.</p>
<ul>
    <li><strong>URL:</strong> <a href="https://map.seoul.go.kr/smgis2/themeGallery/detail?theme_id=1693303773827">스마트 서울맵의 우리동네 디지털안내소 상세 페이지</a></li>
    <li><strong>S3 경로:</strong> <code>rawdata/csv_manual/urdn_digi_edu.csv</code></li>
</ul>
"""


@task
def crawl_update_date():
    url = "https://map.seoul.go.kr/smgis2/themeGallery/detail?theme_id=1693303773827"
    driver = get_driver()
    driver.get(url)

    wait = WebDriverWait(driver, 30)  # 최대 10초 대기

    tar_xpath = "//tr[td/span[text()='정보갱신일']]/td[2]"
    update_date = wait.until(lambda d: (el := d.find_element(By.XPATH, tar_xpath)) and el.text.strip() != "" and el)
    update_date_str = update_date.text
    logging.info(f"URL의 정보갱신일: {update_date_str}")
    return update_date_str  # 문자열 "%Y-%m-%d %H:%M:%S" 형식


@task
def get_max_created_at(schema, table, conn_name="conn_production"):
    hook = PostgresHook(postgres_conn_id=conn_name)
    result = hook.get_first(f"""
        SELECT MAX(created_at) as max_created_at
        FROM {schema}.{table}
    """)

    max_created_at = result[0]
    max_created_str = max_created_at.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"{schema}.{table}의 MAX(created_at) : {max_created_str}")
    return max_created_str


@task.branch
def compare_dates(update_dt_str, max_created_at_str):
    update_dt = datetime.fromisoformat(update_dt_str)
    max_created_at = datetime.fromisoformat(max_created_at_str)

    logging.info(f"우리동네 디지털 안내소의 정보 갱신일 : {update_dt}")
    logging.info(f"대상 테이블의 Max created_at: {max_created_at}")

    # 수행할 task_id를 return 한다.
    if update_dt > max_created_at:
        logging.info("데이터 업데이트 필요!")
        return "send_email"
    else:
        logging.info("데이터 최신 상태")
        return "end_task"
