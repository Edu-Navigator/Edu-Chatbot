import logging
import time

import pandas as pd
import requests
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.kakaomap_api import fetch_geocding
from utils.s3_utils import load_csv

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)


GEO_COLNAME_MAP = {
    "address_name": "ADDRESS",
    "building_name": "BUILDING_NAME",
    "region_1depth_name": "LC_1",
    "region_2depth_name": "LC_2",
    "region_3depth_name": "LC_3",
    "x": "ADDRESS_X",
    "y": "ADDRESS_Y",
}

USE_GEO_KEYS = [
    "address_name",
    "building_name",
    "region_1depth_name",
    "region_2depth_name",
    "region_3depth_name",
    "x",
    "y",
]


@task
def api_digi_learning_loc(**context):
    """
    운영중인 디지털 배움터 장소 정보를 수집.

    공공API의 디지털 배움터 현황 데이터를 요청하고
    한글로된 컬럼명을 영문으로 변환하여 반환한다.

    Returns
    -------
    dict
        디지털 배움터 위치 및 부가 정보
        downstream task에서 입력으로 사용된다.

    Notes
    -----
    - 입력 데이터
        - 공공API의 디지털 배움터 현황
    - 처리 로직
        - 한문 컬럼명을 영문으로 변환한다.
    """
    base_url = "https://api.odcloud.kr/api/15134509/v1"
    get_url = "/uddi:7172b895-8193-44c1-a3c7-c52322104653"

    params = {
        "serviceKey": Variable.get("api_15134509_ServiceKey"),
        "page": 1,
        "perPage": 200,
    }
    res = requests.get(base_url + get_url, params=params)
    df = pd.DataFrame(res.json()["data"])
    colname_kor_to_eng = {
        "관리지역": "LC_1",
        "배움터 유형": "DL_CATEGORY",
        "배움터명": "DL_NM",
        "배움터주소": "DL_ADDRESS",
        "시군구": "LC_2",
        "이용정원": "DL_PCSP",
    }
    df = df.rename(columns=colname_kor_to_eng)

    return df.to_dict("records")


@task
def et_urdn_location(csv_key, bucket, conn_name="s3_conn", **context):
    """
    s3에 csv로 저장된 데이터 로드 및 전처리

    우리동네 디지털 안내소의 정보를 s3에서 로드하고
    적재할 테이블의 컬럼명에 맞게 수정한다.

    Parameters
    ----------
    csv_key : str
        s3에 저장된 csv
    bucket : str
        데이터 적재용 s3 버킷명
    conn_name : str
        Airflow Connection에 등록된 s3 연결명

    Returns
    -------
    dict
        우리동네 디지털 안내소 위치 정보
        downstream task에서 입력으로 사용된다.

    Notes
    -----
    - 입력 데이터
        - s3에 저장된 csv 파일을 사용한다
    - 처리 로직
        - cot_conts_name에 '삭제'라는 단어가 포함된 경우 제외한다.
        - 컬럼명을 table에 맞게 매핑한다.
    """
    df = load_csv(bucket=bucket, key=csv_key, aws_conn_id=conn_name)
    logger.info(f"{df.head()}")

    colnames = ["cot_conts_name", "cot_sub_cate_name", "cot_addr_full_new", "cot_tel_no"]
    df = df[~df["cot_conts_name"].str.contains("삭제")][colnames].reset_index(drop=True)

    df = df.rename(
        columns={
            "cot_conts_name": "URDN_NM",
            "cot_sub_cate_name": "URDN_CATEGORY",
            "cot_addr_full_new": "ADDRESS_TXT",
            "cot_tel_no": "TEL_NO",
        }
    )
    return df.to_dict("records")


@task
def get_details_location(schema, table, conn_name="conn_production", **context):
    """
    우리동네 디지털 안내소 주소의 경도, 위도 정보 탐색

    주소의 상세 정보를 카카오맵 API를 활용해 조회하고,
    도로명 주소 기준 지역명, 경도, 위도 정보를 추가한다.

    Parameters
    ----------
    schema : str
        input 데이터 schema 명
    table : str
        input 데이터 table 명
    conn_name : str
        Airflow Connections에 등록된 Postgres 연결명

    Returns
    -------
    dict
        downstream task에서 입력으로 사용된다.

    Notes
    -----
    - 입력 데이터
        - DB에 저장된 테이블을 로드한다.
    - 처리 로직
        - 카카오맵 API를 사용해 주소의 좌표 정보 조회
        - 도로명 주소의 값 사용
    """
    hook = PostgresHook(postgres_conn_id=conn_name)
    df = hook.get_pandas_df(f"SELECT * FROM {schema}.{table};")
    df.columns = [c.upper() for c in df.columns]

    for idx, row in df.iterrows():
        addr = row["ADDRESS_TXT"]

        # 주소 to 좌표
        res_data = fetch_geocding(addr, api_key=Variable.get("kakao_api_key"))

        if res_data:
            road_addr = res_data["documents"][0].get("road_address")
            # 사용할 key, value만 추출
            fin_adr = {k: road_addr.get(k) for k in USE_GEO_KEYS}

            for col in fin_adr.keys():
                df.at[idx, col] = fin_adr[col]

        time.sleep(0.15)  # rate limit

    df = df.rename(columns=GEO_COLNAME_MAP)

    df = df[
        ~df["ADDRESS"].isnull()
        & (df["ADDRESS"] != "nan")
        & (df["ADDRESS"].str.lower() != "nan")
        & (df["ADDRESS"] != "null")
    ].reset_index(drop=True)
    df["DL_NM"] = df["URDN_CATEGORY"] + " " + df["URDN_NM"]

    res = df[["DL_NM", "TEL_NO", "LC_1", "LC_2", "LC_3", "ADDRESS", "ADDRESS_X", "ADDRESS_Y"]].to_dict("records")

    return res
