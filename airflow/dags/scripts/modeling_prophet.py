from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from prophet import Prophet
import joblib
import tempfile
import json, re
import pandas as pd
import numpy as np
from datetime import datetime

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)
logging.getLogger("prophet").setLevel(logging.WARNING)
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

def normalize(text):
    """
    그룹명 생성시 사용되는 문자열 표준화

    Parameters
    ----------
    text : str
        그룹 텍스트

    Returns
    -------
    str
        공백 처리된 문자열
    """
    text = str(text).strip()
    text = re.sub(r"\s+", "_", text)
    return text

def make_group_id(lc_1, lc_2, lctr_category):
    """
    모델명 저장시 사용할 그룹명 생성

    전처리된 각 문자열을 사용해 그룹명을 생성한다

    Parameters
    ----------
    lc_1 : str
        지역(도, 시)
    lc_2 : str
        지역(시, 구)
    lctr_category : str
        교육 구분

    Returns
    -------
    str
        구분자가 포함된 그룹명
    """
    return f"grp__{normalize(lc_1)}__{normalize(lc_2)}__{normalize(lctr_category)}"

def make_train_data(data, row, today):
    """
    prophet 모델 훈련데이터 생성

    현 그룹의 최초 데이터 발생 시점부터 현재(dag 수행일)전 까지 시점 생성
    각 시점별 시작된 강의 수를 y값으로 사용(강의가 없다면 0)

    Parameters
    ----------
    data : pd.DataFrame
        현재 그룹의 input 데이터
    row : pd.Series
        현재 그룹에 대한 통계 정보
    today : str
        모델링 시점 일자
    
    Returns
    -------
    pd.DataFrame
        현 그룹의 훈련데이터(ds, y)
    """
    all_dates = pd.date_range(
            start = row['min_lctr_bgn'],
            end   = today,
            inclusive='left'
        )
    
    daily_data = (
        data.groupby(data['lctr_bgn'].dt.date)
        .size()
        .reindex(all_dates, fill_value=0)
        .reset_index()
    )
    daily_data.columns = ['ds', 'y']
    return daily_data

@task
def train_prophet_models(schema, table, bucket, s3_base_prefix, s3_conn="aws_default", **context):
    """
    prophet 모델 훈련

    강의 시작일별 강의 건수를 활용해 prophet 모델을 훈련한다.
    지역, 교육 구분별로 모델링을 진행하며 
    최소 데이터가 5건 이상 존재하는 그룹만 훈련을 수행한다.

    Parameters
    ----------
    schema : str
        input 데이터 schema 명
    table : str
        input 데이터 table 명
    bucket : str
        모델 저장될 s3 버킷명
    s3_base_prefix : str
        모델 저장될 기본 s3 경로
    s3_conn : str
        default = 'aws_default'
        Airflow Connections에 등록된 s3 연결명

    Notes
    -----
    - 그룹
        - 지역(도,시,구), 교육구분별 모델링 진행
    - 훈련 대상
        - 최소 데이터가 5건 이상인 경우 훈련 진행
    - 모델 훈련
        - prophet은 데이터 값이 작거나 패턴 추론에 실패하는 경우 에러가 발생함.
        - 에러가 발생한 그룹은 로그 띄우고 skip
    - 모델 저장
        - latest 및 훈련일 폴더에 저장
    - metadata 정보 저장
        - 최근 훈련된 그룹 정보 확인 가능
    """
    # 데이터 전처리
    hook = PostgresHook(postgres_conn_id='conn_production')
    select_query = f"""
    SELECT 
        * 
    FROM {schema}.{table}
    WHERE lctr_bgn >= '2021-01-01';
    """
    df = hook.get_pandas_df(select_query)
    
    grp_df = df.groupby(['lc_1', 'lc_2', 'lctr_category'],as_index=False)\
                .agg(
                    count_row=('lctr_bgn','count'),
                    min_lctr_bgn=('lctr_bgn', 'min'),
                )\
                .sort_values('count_row', ascending=False)
    grp_df = (
        grp_df
        .query("count_row >= 5")
        .reset_index(drop=True)
    ) # 각 그룹의 최소 row 수가 5개 이상
    
    # group별 모델 훈련 및 pkl 파일 저장
    today     = context['data_interval_end'].to_date_string()
    today_key = context['data_interval_end'].strftime("%Y%m%d")
    
    s3hook   = S3Hook(aws_conn_id=s3_conn)
    s3client = s3hook.get_conn() # boto3 client
    
    trained_groups = []
    for _, row in grp_df.iterrows():
        group = make_group_id(
            lc_1=row['lc_1'], 
            lc_2=row['lc_2'], 
            lctr_category=row['lctr_category']
        )
        
        # 현재 그룹의 훈련 데이터 생성
        q = f"lctr_category=='{row['lctr_category']}' & lc_1=='{row['lc_1']}' & lc_2=='{row['lc_2']}'"
        cur_data   = df.query(q).reset_index(drop=True)
        train_data = make_train_data(cur_data, row, today)
        
        # prophet 모델 훈련
        try : 
            model = Prophet(
                growth='linear',
                weekly_seasonality=True,   # 요일 효과 중요
                yearly_seasonality=False, 
                daily_seasonality=True
            )
            model.add_country_holidays(country_name='KR')
            model.fit(train_data)
            
            # 모델 저장
            with tempfile.NamedTemporaryFile(suffix=".pkl") as tmp:
                joblib.dump(model, tmp.name)
                
                s3client.upload_file(
                    tmp.name,
                    bucket,
                    f"{s3_base_prefix}/{today_key}/model_{group}.pkl"
                )
                
                s3client.upload_file(
                    tmp.name,
                    bucket,
                    f"{s3_base_prefix}/latest/model_{group}.pkl"
                )
            
            trained_groups.append(group)
            logging.info(f"Successfully Trained : {group}")
        
        except Exception as e :
            # 에러 발생 : 현재 그룹 훈련 Skip
            logging.info(f"Error Occured during training : {e}")
            logging.info(f"Skip Training model for Group :\n\t {group}")
            continue
    # end-for loop
    
    # metadata저장 : 이번에 훈련된 group 정보
    metadata = {
        "trained_at_based_execution_date": context['execution_date'].isoformat(),
        "trained_at" : datetime.now().isoformat(),
        "model_date": today,
        "groups": trained_groups,
        "algorithm": "prophet"
    }
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as tmp:
        json.dump(metadata, tmp)
        tmp.flush()

        s3client.upload_file(
            tmp.name,
            bucket,
            f"{s3_base_prefix}/{today_key}/metadata.json"
        )
        s3client.upload_file(
            tmp.name,
            bucket,
            f"{s3_base_prefix}/latest/metadata.json"
        )    
    logging.info(f"End Training Prophet : {len(trained_groups)} is trained")


def check_latest_model_exists(bucket, prefix, conn_name='aws_default', **context) :
    """
    pkl 파일 존재 확인

    ShortCircuitOperator와 사용되는 함수로
    S3 latest 경로에 pkl 모델이 하나라도 있는지 확인한다.

    Parameters
    ----------
    bucket : str
        탐색할 s3 버킷명
    prefix : str
        탐색할 s3 경로명
    conn_name : str
        default = 'aws_default'
        Airflow Connections에 등록된 s3 연결명
    
    Returns
    -------
    boolean
        False이면 downstream task를 skip한다.
    """
    s3hook   = S3Hook(aws_conn_id=conn_name)
    s3client = s3hook.get_conn() # boto3 client

    response = s3client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    if "Contents" not in response:
        logging.info("No objects found in latest model path.")
        return False

    model_files = [
        obj["Key"]
        for obj in response["Contents"]
        if obj["Key"].endswith(".pkl")
    ]

    if not model_files:
        logging.info("No model (.pkl) files found in latest.")
        return False

    logging.info(f"Found {len(model_files)} model files.")
    return True

def get_group_values(grp_name):
    """
    그룹명 분할

    '__'로 구분된 각 그룹명 구성 요소를 추출한다.
    '_'로 치환된 띄워쓰기를 원복시킨다.

    Parameters
    ----------
    grp_name : str
        모델 그룹명

    Returns
    -------
    dict
        그룹명을 구성하던 지역 및 교육구분 정보
    """
    grp_ele = grp_name.split('__')[1:]
    # _ -> whitespace 로 변환
    return {
        'lc_1' : re.sub(r"_", " ", grp_ele[0]), 
        'lc_2' : re.sub(r"_", " ", grp_ele[1]),
        'lctr_category' : re.sub(r"_", " ", grp_ele[2]),
    }

@task
def predict_prophet(bucket, prefix="models/prophet/latest/", conn_name="aws_default", **context):
    """
    prophet 예측 수행

    latest에 저장된 모델을 기준으로 예측을 수행한다.
    dag 실행일 기준으로 예측데이터 시점을 생성하고 예측값을 산출한다.
    yhat이 0이 아닌 ds를 사용하며 룰 기반으로 최종 수강신청 시작일을 계산한다.
    
    Parameters
    ----------
    bucket : str
        모델 저장 s3 버킷명
    prefix : str
        default='models/prophet/latest/'
        모델 저장 s3 경로
    conn_name
        default='aws_default'
        Airflow Connections에 등록된 s3 연결명

    Returns
    -------
    str
        task 실행 결과로 생성된 파일 경로이다.
        downstream task에서 입력으로 사용된다.

    Notes
    -----
    - 예측용 input 데이터
        - 예측 수행시점을 기준으로 미래 30일 생성
    - 예측값 후처리
        - yhat값이 0.3 이상인 ds만 활용
        - 강의 시작일자를 의미하는 ds의 20일 이전 date 계산
        - date가 예측 수행시점보다 미래인 경우만 최종값으로 활용
    """
    s3hook   = S3Hook(aws_conn_id=conn_name)
    s3client = s3hook.get_conn() # boto3 client

    response = s3client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    model_keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".pkl")
    ]

    if not model_keys:
        raise ValueError("No model files found, but prediction task was triggered.")

    predictions = []
    
    today = context['ds']
    future = pd.DataFrame({
        "ds": pd.date_range(start=today, periods=30, freq="D")
    })

    for key in model_keys:
        group_id = key.split("/")[-1].replace(".pkl", "")
        group_values = get_group_values(group_id)
        
        logging.info(f"[@@] Start forecasting... {group_id} ")
        logging.info(f"{group_values}")
        
        # 1) 모델 다운로드
        with tempfile.NamedTemporaryFile() as tmp:
            s3client.download_file(bucket, key, tmp.name)
            model = joblib.load(tmp.name)

        # 2) 예측 수행 & 예측값 전처리
        forecast = model.predict(future)
        forecast['yhat'] = np.where(
                forecast['yhat'] < 0.3, 0,
                np.ceil(forecast['yhat'])
            )
        
        if len(forecast.query("yhat > 0")) != 0 :
            # 예측 결과가 0보다 큰 경우만 적재
            logging.info(forecast.query("yhat > 0")[['ds', 'yhat']].assign(**group_values))
            
            predictions.append(
                forecast.query("yhat > 0")[['ds', 'yhat']]\
                    .assign(**group_values)
            )
    # end for-loop
    
    # 예상 수강 신청일자 계산 : ds - 20일 (데이터상의 중앙값 기준)
    # 예상결과가 오늘 일자보다 미래인 경우만 적재
    all_preds = pd.concat(predictions)
    all_preds['pred_aply_bgn'] = all_preds["ds"] - pd.Timedelta(days=20)
    all_preds = (
        all_preds[all_preds['pred_aply_bgn'] > today]
        .drop(columns=['ds', 'yhat'])
    )
    
    logging.info(f"Preditions : ")
    print("\n",all_preds.head())
    logging.info(f"=================================")
    
    path = f"{Variable.get('DATA_DIR')}/pred_prophet.csv"
    all_preds.to_csv(path, index=False)
    logging.info(f"[@@] Saved CSV File in local : {path}")
    return path