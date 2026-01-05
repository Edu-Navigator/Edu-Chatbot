from airflow.decorators import task
from utils.webdriver import get_driver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import logging
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
    """
    데이터 소스 업데이트 일자 확인

    타겟 URL에서 정보 갱신일을 크롤링하고,
    문자열로 반환한다.

    Returns
    -------
    str
        task 실행 결과로 "%Y-%m-%d %H:%M:%S" 포맷이며,
        downstream task에서 입력으로 사용된다.

    """
    url = "https://map.seoul.go.kr/smgis2/themeGallery/detail?theme_id=1693303773827"
    driver = get_driver()
    driver.get(url)

    wait = WebDriverWait(driver, 30)  # 최대 10초 대기

    tar_xpath = "//tr[td/span[text()='정보갱신일']]/td[2]"
    update_date = wait.until(
        lambda d: (
            el := d.find_element(By.XPATH, tar_xpath)
        ) and el.text.strip() != "" and el
    )
    update_date_str = update_date.text
    logging.info(f"URL의 정보갱신일: {update_date_str}")
    return update_date_str # 문자열 "%Y-%m-%d %H:%M:%S" 형식


@task
def get_max_created_at(schema, table, conn_name='conn_production'):
    """
    테이블의 최신 데이터 생성일자 확인

    타겟 테이블에서 max(created_at)을 조회하고,
    문자열로 반환한다.

    Parameters
    ----------
    schema : str
        조회할 DB의 스키마 명
    table : str
        조회할 테이블 명
    conn_name : str
        default = conn_production
        DB 연결 아이디명

    Returns
    -------
    str
        task 실행 결과로 "%Y-%m-%d %H:%M:%S" 포맷이며,
        downstream task에서 입력으로 사용된다.

    Notes
    -----
    - 처리 로직
        - DB의 UTC 정보를 반영한 datetime으로 변환한다.
        
    """
    hook = PostgresHook(postgres_conn_id=conn_name)
    result = hook.get_first(f"""
        SELECT MAX(created_at) as max_created_at
        FROM {schema}.{table}
    """)
        
    max_created_at  = result[0]
    max_created_str = (
        max_created_at
        .astimezone()
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    logging.info(f"{schema}.{table}의 MAX(created_at) : {max_created_str}")
    return max_created_str

@task.branch
def compare_dates(update_dt_str, max_created_at_str):
    """
    일자 비교 후 결과에 따라 task명 반환

    데이터 소스와 테이블의 정보 갱신일을 비교하고,
    소스의 데이터가 업데이트 된 경우 'send_email'을 반환하고 아니면 'end_task'를 반환한다.

    Parameters
    ----------
    update_dt_str : str
        데이터 소스 업데이트 일자
    max_created_at_str : str
        테이블 업데이트 일자

    Returns
    -------
    str
        수행될 task_id를

    """
    update_dt      = datetime.fromisoformat(update_dt_str)
    max_created_at = datetime.fromisoformat(max_created_at_str)
    
    logging.info(f"우리동네 디지털 안내소의 정보 갱신일 : {update_dt}")
    logging.info(f"대상 테이블의 Max created_at: {max_created_at}")
    
    # 수행할 task_id를 return 한다.
    if update_dt > max_created_at:
        logging.info("데이터 업데이트 필요!")
        return 'send_email'
    else:
        logging.info("데이터 최신 상태")
        return 'end_task'
