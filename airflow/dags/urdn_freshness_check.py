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