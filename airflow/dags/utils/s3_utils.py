from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import pandas as pd
from io import BytesIO, StringIO

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)


def list_files(bucket, prefix, postfix="", aws_conn_id="aws_s3_conn_id"):
    """
    Get all of the files From S3
    """
    # prefix  : 탐색 원하는 s3 경로
    # postfix : 특정 확장자가 필요한 경우 사용. ".csv", ".json" 
    hook = S3Hook(aws_conn_id=aws_conn_id)
    keys = hook.list_keys(bucket, prefix)
    return [k for k in keys if k.endswith(postfix)]

def load_json(bucket, key, aws_conn_id="aws_s3_conn_id"):
    """
    Load Json file From S3
    """
    hook = S3Hook(aws_conn_id=aws_conn_id)
    file_obj = hook.get_key(key, bucket_name=bucket)
    data = json.loads(file_obj.get()["Body"].read().decode("utf-8"))
    return data


def load_csv(bucket, key, aws_conn_id="aws_s3_conn_id"):
    """
    Load CSV file From S3
    """
    hook = S3Hook(aws_conn_id=aws_conn_id)
    file_obj = hook.get_key(key, bucket_name=bucket)
    data_io = StringIO(file_obj.get()["Body"].read().decode('cp949'))
    return pd.read_csv(data_io)

def load_all_jsons(bucket, prefix, aws_conn_id="aws_s3_conn_id"):
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

def upload_data_to_parquet(data, aws_conn_id, bucket, save_key, replace=True):
    """
    Upload dataframe to parquet
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

def upload_dict_to_csv(data, aws_conn_id, bucket, save_key, replace=True):
    """
    Upload dataframe to csv
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