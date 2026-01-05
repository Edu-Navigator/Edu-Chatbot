"""
데이터 수집 DAG들이 성공으로 종료되면, 원본 데이터를 전처리하고 취합 및 적재한다.

이 DAG는 데이터베이스에서 각 테이블의 컬럼명을 표준화하여 타켓 테이블의 스키마에 맞게 변환,
강의장 주소 기준 행정구역(시, 도, 구) 표준화, 강의 이미지 URL 맵핑 등의 처리를 수행하고 적재합니다.

Parameters
----------
schedule_interval : str
    매일 오전 10시 20분 ('20 10 * * *')
start_date : datetime
    Starting date for the DAG (2025-12-01)
catchup : bool
    False to prevent backfilling historical runs
tags : list of str
    ["02", "postgres", "lecture", 'edu_info']

Tasks
-----
suji_data_processing_task
    수지구청 데이터 컬럼 표준화
gg_data_processing_task
    경기도 평생학습 데이터 컬럼 표준화
digi_end_data_processing_task
    디지털 배움터 데이터 컬럼 표준화
combine_and_insert_lecture_task
    데이터 취합 및 1차 적재
lecture_location_image_task
    주소 및 이미지 맵핑 전처리
edu_info_task
    테이블 full refresh로 저장

Dependencies
------------
[suji_data_processing_task, gg_data_processing_task, digi_end_data_processing_task] 
>> combine_and_insert_lecture_task >> lecture_location_image_task >> edu_info_task

Notes
-----
- ExternalTaskSensor를 사용해 DAG가 성공했는지 탐색합니다.

See Also
--------
01_collect_location_info : 장소 관련 데이터 수집 DAG
01_crawling_images_digilearning : 장소 관련 이미지 수집 DAG
01_digital_learning_crawl : 디지털 배움터 강의 수집 DAG
01_gg_pipeline : 경기도 평생학습 데이터 수집 DAG
01_suji_pipeline : 용인시 수지구청 데이터 수집 DAG

"""

from airflow.models.dag import DAG 
from datetime import datetime, timedelta
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
from common.default_args import DEFAULT_ARGS


with DAG(
    dag_id="02_lecture_dag",
    start_date=pendulum.datetime(2025, 12, 1, 0, 0, 
                                tz=pendulum.timezone("Asia/Seoul")), 
    schedule="20 10 * * *", # start_date의 tz 기준 오전 10시 실행
    catchup=False,
    tags=["02", "postgres", "lecture", 'edu_info'],
    default_args=DEFAULT_ARGS,
) as dag:
    
    # -------------------------------
    # 앞선 DAG의 success를 기다리는 센서
    # -------------------------------
    wait_digital_learning = ExternalTaskSensor(
        task_id="wait_01_digital_learning_crawl",
        external_dag_id="01_digital_learning_crawl",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        execution_date_fn=lambda dt: dt - timedelta(minutes=20),
    )

    wait_suji_pipeline = ExternalTaskSensor(
        task_id="wait_01_suji_pipeline",
        external_dag_id="01_suji_pipeline",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        execution_date_fn=lambda dt: dt - timedelta(minutes=10),
    )
    
    wait_gg_pipeline = ExternalTaskSensor(
        task_id="wait_01_gg_pipeline",
        external_dag_id="01_gg_pipeline",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        execution_date_fn=lambda dt: dt - timedelta(minutes=10),
    )

    wait_crawling_images = ExternalTaskSensor(
        task_id="wait_01_crawling_images_digilearning",
        external_dag_id="01_crawling_images_digilearning",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        execution_date_fn=lambda dt: dt - timedelta(minutes=5),
    )

    wait_etl_locations = ExternalTaskSensor(
        task_id="wait_01_collect_location_info",
        external_dag_id="01_collect_location_info",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 30,
        execution_date_fn=lambda dt: dt - timedelta(minutes=5),
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
            wait_suji_pipeline,
            wait_gg_pipeline,
        ] >> t

    # lecture DAG 내부 의존성
    [
        suji_path,
        gg_path,
        # digi_path,
        digi_end_path,
    ] >> final_combine >> location_image_update >> edu_info_update
