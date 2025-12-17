from airflow.models.dag import DAG 
from datetime import datetime
from scripts.lecture_task import (
    create_lecture_table_task,
    suji_data_processing_task,
    gg_data_processing_task,
    digi_data_processing_task,
    digi_end_data_processing_task,
    combine_and_insert_lecture_task,
    lecture_location_image_task
)


with DAG(
    dag_id="lecture_dag",
    start_date=datetime(2025, 12, 9), 
    schedule_interval=None,
    catchup=False,
    tags=["snowflake", "etl", "lecture"]
) as dag:
    
    # 1. Task 인스턴스 및 XCom 결과 생성.
    
    # Task 1: 대상 테이블 생성
    create_table = create_lecture_table_task()
    
    # Task 2-5: 데이터 처리 (병렬 실행)
    df_suji = suji_data_processing_task()
    df_gg = gg_data_processing_task()
    df_digi = digi_data_processing_task() 
    df_digi_end = digi_end_data_processing_task()
    
    # Task 6: 통합 및 삽입
    final_combine = combine_and_insert_lecture_task(
        df_suji=df_suji,
        df_gg=df_gg,
        df_digi=df_digi,
        df_digi_end=df_digi_end,
    )
    
    # Task 7: 강좌 위치 정보 업데이트
    location_image_update = lecture_location_image_task()

    # 2. 의존성 정의
    create_table >> [df_suji, df_gg, df_digi, df_digi_end] >> final_combine >> location_image_update