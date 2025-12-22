from airflow.models.dag import DAG 
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum

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
    dag_id="02_lecture_dag",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="00 15 * * *", # start_date의 tz 기준 오전 10시 실행
    catchup=False,
    tags=["02", "postgres", "lecture", 'edu_info'],
) as dag:
    
    # -------------------------------
    # 앞선 DAG의 success를 기다리는 센서
    # -------------------------------
    wait_crawling_images = ExternalTaskSensor(
        task_id="wait_01_crawling_images_digilearning",
        external_dag_id="01_crawling_images_digilearning",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,   # 최대 30분 대기
        execution_date_fn=lambda dt: dt,
    )

    wait_etl_locations = ExternalTaskSensor(
        task_id="wait_01_collect_location_info",
        external_dag_id="01_collect_location_info",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        execution_date_fn=lambda dt: dt,
    )

    wait_digital_learning = ExternalTaskSensor(
        task_id="wait_01_digital_learning_crawl",
        external_dag_id="01_digital_learning_crawl",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        execution_date_fn=lambda dt: dt,
    )

    wait_suji_gg_pipeline = ExternalTaskSensor(
        task_id="wait_01_suji_gg_pipeline",
        external_dag_id="01_suji_gg_pipeline",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        execution_date_fn=lambda dt: dt,
    )
    
    # -------------------------------
    # 2. Task 인스턴스 생성
    # -------------------------------
    suji_path = suji_data_processing_task()
    gg_path = gg_data_processing_task()
    # digi_path = digi_data_processing_task()
    digi_end_path = digi_end_data_processing_task()
    
    final_combine = combine_and_insert_lecture_task(
        suji_path=suji_path,
        gg_path=gg_path,
        # digi_path=digi_path,
        digi_end_path=digi_end_path,
    )
    
    location_image_update = lecture_location_image_task()
    edu_info_update = edu_info_task()

    # -------------------------------
    # 3. 의존성 정의 : 1~4번 DAG가 모두 끝난 뒤 lecture 시작
    # -------------------------------
    for t in [suji_path, gg_path, digi_end_path]:  # 이후 digi_path 추가
        [
            wait_crawling_images,
            wait_etl_locations,
            wait_digital_learning,
            wait_suji_gg_pipeline,
        ] >> t

    # lecture DAG 내부 의존성
    [
        suji_path,
        gg_path,
        # digi_path,
        digi_end_path,
    ] >> final_combine >> location_image_update >> edu_info_update
