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
    text = str(text).strip()
    text = re.sub(r"\s+", "_", text)
    return text

def make_group_id(lc_1, lc_2, lctr_category):
    return f"grp__{normalize(lc_1)}__{normalize(lc_2)}__{normalize(lctr_category)}"

def make_train_data(data, row, today):
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
def train_prophet_models(schema, table, bucket, s3_base_prefix, s3_conn="s3_conn", **context):
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
    today     = context['ds']
    today_key = context['execution_date'].strftime("%Y%m%d")
    
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

# ======================================
def check_latest_model_exists(bucket, prefix, conn_name, **context) :
    """
    S3 latest 경로에 pkl 모델이 하나라도 있는지 확인
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
    grp_ele = grp_name.split('__')[1:]
    # _ -> whitespace 로 변환
    return {
        'lc_1' : re.sub(r"_", " ", grp_ele[0]), 
        'lc_2' : re.sub(r"_", " ", grp_ele[1]),
        'lctr_category' : re.sub(r"_", " ", grp_ele[2]),
    }

@task
def predict_prophet(
    bucket,
    prefix = "models/prophet/latest/",
    conn_name = "aws_default",
    **context
):
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
        "ds": pd.date_range(start=today, periods=29, freq="D")
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
        .drop(columns=['ds'])
    )
    
    logging.info(f"Preditions : ")
    print(all_preds.head())
    logging.info(f"=================================")
    
    logging.info(f"[@@] Saved CSV File in local : {path}")
    path = f"{Variable.get('DATA_DIR')}/pred_prophet.csv"
    res.to_csv(path, index=False)
    return path