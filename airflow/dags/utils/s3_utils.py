from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import pandas as pd
from io import BytesIO, StringIO

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)


def list_files(bucket, prefix, postfix="", aws_conn_id="aws_default"):
    """
    S3의 파일 목록 반환
    
    접두사(그리고 접미사)를 포함하는 파일 목록을 모두 반환한다.

    Parameters
    ----------
    bucket : str
        s3 버킷명
    prefix : str
        접두사, 탐색 원하는 s3 경로
    postfix : str
        default=''
        접미사, 특정 확장자가 필요한 경우 사용. ".csv", ".json" 
    aws_conn_id : str
        default='aws_default'
        Airflow Connections에 등록된 s3 연결명

    Returns
    -------
    list
        조건에 맞는 파일 목록
    """
    hook = S3Hook(aws_conn_id=aws_conn_id)
    keys = hook.list_keys(bucket, prefix)
    return [k for k in keys if k.endswith(postfix)]

def load_json(bucket, key, aws_conn_id="aws_default"):
    """
    S3에 저장된 json 파일 로드

    Parameters
    ----------
    bucket : str
        s3 버킷명
    key : str
        s3 파일 key
    aws_conn_id : str
        default='aws_default'
        Airflow Connections에 등록된 s3 연결명

    Returns
    -------
    dict
        로드한 json 데이터
    """
    hook = S3Hook(aws_conn_id=aws_conn_id)
    file_obj = hook.get_key(key, bucket_name=bucket)
    data = json.loads(file_obj.get()["Body"].read().decode("utf-8"))
    return data


def load_csv(bucket, key, aws_conn_id="aws_default"):
    """
    S3에 저장된 csv 파일 로드

    Parameters
    ----------
    bucket : str
        s3 버킷명
    key : str
        s3 파일 key
    aws_conn_id : str
        default='aws_default'
        Airflow Connections에 등록된 s3 연결명

    Returns
    -------
    pd.DataFrame
        로드한 csv 데이터
    """
    hook = S3Hook(aws_conn_id=aws_conn_id)
    file_obj = hook.get_key(key, bucket_name=bucket)
    data_io = StringIO(file_obj.get()["Body"].read().decode('cp949'))
    return pd.read_csv(data_io)

def load_all_jsons(bucket, prefix, aws_conn_id="aws_default"):
    """
    S3에 저장된 특정 접두사를 가진 key기준 json 파일 로드

    Parameters
    ----------
    bucket : str
        s3 버킷명
    prefix : str
        접두사, 데이터 로드 원하는 s3 경로
    aws_conn_id : str
        default='aws_default'
        Airflow Connections에 등록된 s3 연결명

    Returns
    -------
    list
        로드한 json 데이터
    """
    # 적재된 json 파일들 목록 가져오기
    logger.info(f"Walk prefix key... {prefix}")
    lst_fnames = list_files(bucket, prefix, postfix='.json'
                            , aws_conn_id=aws_conn_id)
    logger.info(f"File Count = {len(lst_fnames)}, List = {lst_fnames}")
    
    # json 데이터 로드
    all_data = []
    for fname in lst_fnames:
        data = load_json(bucket=bucket, key=fname, aws_conn_id=aws_conn_id)
        all_data.extend(data)
    logger.info(f"Every Files loaded")
    logger.info(f"Total num of data = {len(all_data)}")
    return all_data

def upload_data_to_parquet(data, bucket, save_key, aws_conn_id='aws_default', replace=True):
    """
    DataFrame을 parquet 확장자로 저장

    Parameters
    ----------
    data : pd.DataFrame
        저장할 데이터
    bucket : str
        저장할 버킷명
    save_key : str
        저장할 s3 key
    aws_conn_id : str
        default='aws_default'
        Airflow Connections에 등록된 s3 연결명
    """
    s3_hook = S3Hook(aws_conn_id)
    buffer = BytesIO()
    data.to_parquet(buffer, index=False)

    logging.info(f"Saving to {save_key} in S3")
    s3_hook.load_bytes(
        bytes_data=buffer.getvalue(),
        key=save_key,
        bucket_name=bucket,
        replace=replace
    )
    logging.info(f"Successfully Data uploaded to {save_key}")

def upload_dict_to_csv(data, bucket, save_key, aws_conn_id='aws_default', replace=True):
    """
    dict을 csv 확장자로 저장

    더 자세한 설명을 여기에 작성.
    여러 줄로 써도 됩니다.

    Parameters
    ----------
    data : dict
        저장할 데이터
    bucket : str
        저장할 버킷명
    save_key : str
        저장할 s3 key
    aws_conn_id : str
        default='aws_default'
        Airflow Connections에 등록된 s3 연결명
    """
    data = pd.DataFrame(data)
    
    s3_hook = S3Hook(aws_conn_id)
    buffer = StringIO()
    data.to_csv(buffer, index=False)

    logging.info(f"Saving to {save_key} in S3")
    s3_hook.load_string(
        string_data=buffer.getvalue(),
        key=save_key,
        bucket_name=bucket,
        replace=replace
    )
    logging.info(f"Successfully Data uploaded to {save_key}")