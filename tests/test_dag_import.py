# tests/test_dag_import.py

import os
from airflow.models import DagBag

def test_dag_import():
    "모든 DAG가 오류 없이 임포트되는지 테스트"
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    
    # 디버깅 정보 출력
    print(f"\n{'='*50}")
    print(f"DAG 폴더: /opt/airflow/dags")
    print(f"DAG 폴더 내용: {os.listdir('/opt/airflow/dags')}")
    print(f"발견된 DAG: {list(dagbag.dags.keys())}")
    print(f"임포트 오류: {dagbag.import_errors}")
    print(f"{'='*50}\n")
    
    # 1. import 오류 체크 
    assert len(dagbag.import_errors) == 0, \
        f"DAG import 오류 발견:\n{dagbag.import_errors}"
    
    # 2. DAG가 최소 1개 이상 있는지 체크
    assert len(dagbag.dags) > 0, \
        "DAG를 찾을 수 없습니다. DAG 파일에 유효한 DAG 객체가 있는지 확인하세요."
    
    # 3. 특정 DAG 존재 여부 체크 (필요한 경우 주석 해제)
    print(f"모든 테스트 통과! {len(dagbag.dags)}개의 DAG를 찾았습니다")