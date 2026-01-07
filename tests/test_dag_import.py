# tests/test_dag_import.py
"""
Airflow DAG import 테스트

- dags/ 디렉토리에 있는 모든 DAG 파일이
  문법 오류나 import 오류 없이 정상적으로 로드되는지 검증한다.
- CI 환경에서 DAG 배포 전 최소한의 안정성 체크를 수행하기 위한 테스트이다.
"""

import os

from airflow.models import DagBag


def test_dag_import():
    """
    모든 Airflow DAG가 오류 없이 임포트되는지 검증한다.

    This test checks:
    1. DAG import 과정에서 발생한 오류가 없는지
    2. 최소 1개 이상의 DAG가 로드되는지

    Notes
    -----
    - Airflow의 DagBag을 사용하여 dags 디렉토리를 스캔한다.
    - CI 환경에서는 DAG 실행이 아닌 *임포트 가능 여부*만 검증한다.
    """

    # DagBag을 이용해 DAG 디렉토리 로드
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)

    # 디버깅 정보 출력
    print(f"\n{'=' * 50}")
    print("DAG 폴더: /opt/airflow/dags")
    print(f"DAG 폴더 내용: {os.listdir('/opt/airflow/dags')}")
    print(f"발견된 DAG: {list(dagbag.dags.keys())}")
    print(f"임포트 오류: {dagbag.import_errors}")
    print(f"{'=' * 50}\n")

    # 1. import 오류 체크
    assert len(dagbag.import_errors) == 0, f"DAG import 오류 발견:\n{dagbag.import_errors}"

    # 2. DAG가 최소 1개 이상 있는지 체크
    assert len(dagbag.dags) > 0, "DAG를 찾을 수 없습니다. DAG 파일에 유효한 DAG 객체가 있는지 확인하세요."

    # 3. 검증 통과 시 정보 출력
    print(f"모든 테스트 통과! {len(dagbag.dags)}개의 DAG를 찾았습니다")
