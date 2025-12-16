from utils.s3_utils import load_csv
import pandas as pd
import requests, time
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from utils.kakaomap_api import fetch_geocding

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)


GEO_COLNAME_MAP = {
    'address_name':'ADDRESS',
    'building_name':'BUILDING_NAME',
    'region_1depth_name':'LC_1',
    'region_2depth_name':'LC_2',
    'region_3depth_name':'LC_3',
    'x':'ADDRESS_X',
    'y':'ADDRESS_Y',
}

USE_GEO_KEYS = [
    'address_name', 'building_name', 'region_1depth_name',
    'region_2depth_name', 'region_3depth_name', 'x', 'y'
]


@task
def api_digi_learning_loc(**context):
    base_url = "https://api.odcloud.kr/api/15134509/v1"
    get_url  = "/uddi:7172b895-8193-44c1-a3c7-c52322104653"

    params = {
        "serviceKey" : Variable.get("api_15134509_ServiceKey"),
        "page":1,
        "perPage":200,
    }
    res = requests.get(base_url+get_url, params=params)
    df = pd.DataFrame(res.json()['data'])
    colname_kor_to_eng = {
        '관리지역':'LC_1',
        '배움터 유형':'DL_CATEGORY',
        '배움터명':'DL_NM', 
        '배움터주소':'DL_ADDRESS',
        '시군구':'LC_2',
        '이용정원':'DL_PCSP'
    }
    df = df.rename(columns=colname_kor_to_eng)
    
    return df.to_dict('records')

@task
def et_urdn_location(csv_key,bucket,conn_name='s3_conn', **context):
    df = load_csv(bucket=bucket, key=csv_key, aws_conn_id=conn_name)
    logger.info(f"{df.head()}")
    
    colnames = ['cot_conts_name', 'cot_sub_cate_name', 'cot_addr_full_new', 'cot_tel_no']
    df = df[~df['cot_conts_name'].str.contains('삭제')][colnames]\
                .reset_index(drop=True)
    
    df = df.rename(columns = {
        'cot_conts_name':'URDN_NM',
        'cot_sub_cate_name':'URDN_CATEGORY',
        'cot_addr_full_new':'ADDRESS_TXT', 
        'cot_tel_no':'TEL_NO'
    })
    return df.to_dict('records')

@task
def get_details_location(schema, table, conn_name='snowflake_conn', **context):
    hook = SnowflakeHook(conn_name)
    df = hook.get_pandas_df(f"SELECT * FROM {schema}.{table};")
    
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
    
    df = df[~df['ADDRESS'].isnull()].reset_index(drop=True)
    df['DL_NM'] = df['URDN_CATEGORY']+" "+df['URDN_NM']

    res = df[['DL_NM', 'TEL_NO', 'LC_1', 'LC_2', 'LC_3', 'ADDRESS','ADDRESS_X', 'ADDRESS_Y']]\
        .to_dict('records')
    
    return res