from airflow.models.dag import DAG 
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor

from scripts.lecture_task import (
    suji_data_processing_task,
    gg_data_processing_task,
    # digi_data_processing_task,
    digi_end_data_processing_task,
    combine_and_insert_lecture_task,
    lecture_location_image_task,
    edu_info_task
)


with DAG(
    dag_id="01_lecture_dag",
    start_date=datetime(2025, 12, 9), 
    schedule="0 1 * * *", # utc 새벽 1시 = kst 오전 10시
    catchup=False,
    tags=["01", "postgres", "lecture", 'edu_info'],
) as dag:
    
    wait_suji_gg_pipeline = ExternalTaskSensor(
        task_id="wait_01_suji_gg_pipeline",
        external_dag_id="01_suji_gg_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,   # 최대 30분 대기
    )
    
    # 1. Task 인스턴스 및 XCom 결과 생성.
    
    suji_path = suji_data_processing_task()
    gg_path = gg_data_processing_task()
    # digi_path = digi_data_processing_task()
    digi_end_path = digi_end_data_processing_task()
    
    # Task 5: 통합 및 삽입
    final_combine = combine_and_insert_lecture_task(
        suji_path=suji_path,
        gg_path=gg_path,
        # digi_path=digi_path,
        digi_end_path=digi_end_path,
    )
    
    # Task 6: 강좌 위치 정보 업데이트
    location_image_update = lecture_location_image_task()

    # Task 7: analytics.edu_info 업데이트
    edu_info_update = edu_info_task()

    # 2. 의존성 정의
    [suji_path, gg_path, 
    # digi_path, 
    digi_end_path] >> final_combine >> location_image_update >> edu_info_update

    wait_suji_gg_pipeline >> [suji_path, gg_path, 
                            # digi_path,
                            digi_end_path]