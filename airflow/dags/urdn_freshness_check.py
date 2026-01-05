"""
타겟 테이블의 최신 생성 일시와 데이터 소스의 업데이트 일자 비교 로직을 수행하고, 데이터 소스에 변경 사항이 발생한 경우 이메일을 발송한다.

이 DAG는 ‘우리동네 디지털 안내소’로 등록된 통신사의 정보가 업데이트 되었는지 감지하고,
타겟 테이블의 마지막 적재 시점(MAX(created_at))과 원천 데이터 소스의 최신 업데이트 일자를 비교합니다.
새로운 데이터 업데이트가 감지될 경우, 담당자에게 행동 매뉴얼을 메일로 전송합니다.

Parameters
----------
schedule_interval : str
    매일 오전 9시 30분 ('30 9 * * *')
start_date : datetime
    Starting date for the DAG (2025-12-01)
catchup : bool
    False to prevent backfilling historical runs
tags : list of str
    ['00', 'check', 'email', 'urdn']

Tasks
-----
crawl_update_date
    소스 페이지의 데이터 업데이트 일자 확인
get_max_created_at
    타겟 테이블의 최신 데이터 생성일자 확인
compare_dates
    데이터 업데이트 일자 비교
end_task
    DAG 종료용 task
send_email
    담당자에게 이메일 발송
    

Dependencies
------------
[crawl_update_date, get_max_created_at] >> compare_dates >> [end_task | send_email]


See Also
--------
01_collect_location_info : 우리동네 디지털 안내소 데이터 처리 DAG
"""

from airflow import DAG
import pendulum

# from common.default_args import DEFAULT_ARGS
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator

from scripts.urdn_email_task import (
    crawl_update_date, 
    get_max_created_at, 
    compare_dates, 
    URDN_UPDATE_ALERT_TEMPLATE
)

with DAG(
    dag_id = '00_check_urdn_center_update',
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="30 9 * * *", # start_date의 tz 기준 오전 9시 30분 실행
    catchup = False,
    tags=['00', 'check', 'email', 'urdn'],
    # default_args=DEFAULT_ARGS,
) as dag :

    update_cycle = crawl_update_date()
    max_created = get_max_created_at(
        schema = 'raw_data', 
        table = 'urdn_digital_location',
    )
    
    branch = compare_dates(update_cycle, max_created)
    send_email = EmailOperator(
        task_id='send_email',
        to=['sosoj1552@gmail.com'],
        subject='[Alert] 우리동네 디지털 안내소 데이터 업데이트 필요',
        html_content=URDN_UPDATE_ALERT_TEMPLATE
    )
    
    # 5. 종료
    end_task = EmptyOperator(
        task_id='end_task',
    )
    
    branch >> [send_email, end_task]