from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import requests
import math


@task
def gg_task():

    API_KEY = "7796e90b54c64c1ab96597f09ac555e5"
    BASE_URL = "https://openapi.gg.go.kr/ALifetimeLearningLecture"
    PSIZE = 1000

    # 1) 총 데이터 개수 확인
    first_url = f"{BASE_URL}?KEY={API_KEY}&Type=json&pindex=1&pSize={PSIZE}"
    first_res = requests.get(first_url).json()

    # 데이터가 없는 경우를 처리 (예외 방지)
    if "ALifetimeLearningLecture" not in first_res or len(first_res["ALifetimeLearningLecture"]) < 2:
        total_count = 0
    else:
        total_count = first_res["ALifetimeLearningLecture"][0]["head"][0]["list_total_count"]
    
    total_pages = math.ceil(total_count / PSIZE) if total_count > 0 else 0

    print("총 데이터:", total_count)
    print("총 페이지:", total_pages)

    # 2) 전체 페이지 반복 수집
    all_rows = []

    for page in range(1, total_pages + 1):
        url = f"{BASE_URL}?KEY={API_KEY}&Type=json&pindex={page}&pSize={PSIZE}"
        res = requests.get(url).json()
        
        # 'row' 키가 있는지 확인하여 데이터가 없는 페이지 처리
        if len(res["ALifetimeLearningLecture"]) > 1 and "row" in res["ALifetimeLearningLecture"][1]:
            rows = res["ALifetimeLearningLecture"][1]["row"]
            all_rows.extend(rows)
        

    # 3) 필요한 컬럼 추출
    use_cols = [
        "LECT_TITLE", "EDU_BEGIN_DE", "EDU_END_DE", "EDU_BEGIN_TM", "EDU_END_TM",
        "LECT_CONT", "EDU_TARGET_DIV_NM", "EDU_METH_DIV_NM", "OPERT_WDAY_INFO",
        "EDU_PLC", "LECT_PSN_CNT", "ATENLECT_CHRG", "REFINE_ROADNM_ADDR",
        "OPERT_INST_NM", "OPERT_INST_TELNO", "RECEPT_BEGIN_DE", "RECEPT_END_DE",
        "RECEPT_METH_DIV_NM", "SELECT_METH_DIV_NM", "HMPG_ADDR",
        "OADT_SPORT_LECT_YN", "ACBS_EVALTN_PNT_RECOGN_YN",
        "LFLERNACCT_EVALTN_RECOGN_YN", "DATA_STD_DE"
    ]
    
    if not all_rows:
        df = pd.DataFrame(columns=use_cols)
    else:
        df = pd.DataFrame(all_rows)[use_cols]

    # 4) 컬럼 매핑
    rename_map = {
        "LECT_TITLE": "LCTR_NM",
        "EDU_BEGIN_DE": "LCTR_BGN",
        "EDU_END_DE": "LCTR_END",
        "EDU_BEGIN_TM": "LCTR_BGN_TIME",
        "EDU_END_TM": "LCTR_END_TIME",
        "LECT_CONT": "LCTR_CONTENT",
        "EDU_TARGET_DIV_NM": "LCTR_TARGET",
        "EDU_METH_DIV_NM": "LCTR_WAY",
        "OPERT_WDAY_INFO": "LCTR_DAY",
        "EDU_PLC": "LCTR_LOCATE",
        "LECT_PSN_CNT": "LCTR_PCSP",
        "ATENLECT_CHRG": "LCTR_PRICE",
        "REFINE_ROADNM_ADDR": "LCTR_ADDRESS",
        "OPERT_INST_NM": "LCTR_OPERATE",
        "OPERT_INST_TELNO": "OPERATE_TELEPHONE",
        "RECEPT_BEGIN_DE": "APLY_BGN",
        "RECEPT_END_DE": "APLY_END",
        "RECEPT_METH_DIV_NM": "APLY_WAY",
        "SELECT_METH_DIV_NM": "SELECT_WAY",
        "HMPG_ADDR": "APLY_URL",
        "OADT_SPORT_LECT_YN": "NOTUSE1",
        "ACBS_EVALTN_PNT_RECOGN_YN": "NOTUSE2",
        "LFLERNACCT_EVALTN_RECOGN_YN": "NOTUSE3",
        "DATA_STD_DE": "NOTUSE4"
    }

    df = df.rename(columns=rename_map)
    
    # INTEGER 컬럼 타입 명시적 변환
    df['LCTR_PCSP'] = pd.to_numeric(df['LCTR_PCSP'], errors='coerce').astype('Int64')
    df['LCTR_PRICE'] = pd.to_numeric(df['LCTR_PRICE'], errors='coerce').astype('Int64')

    # 5) Snowflake에 데이터 적재
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # DATABASE_NAME = "DIGIEDU"
    # SCHEMA_NAME = "RAW_DATA"

    # cursor.execute(f"USE DATABASE {DATABASE_NAME}") 
    # cursor.execute(f"USE SCHEMA {SCHEMA_NAME}")
    SCHEMA_NAME = "RAW_DATA"
    table_name = "GG_LEARNING"

    # cursor.execute(f"""
    #     CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table_name} (
    #         LCTR_NM STRING,
    #         LCTR_BGN STRING,
    #         LCTR_END STRING,
    #         LCTR_BGN_TIME STRING,
    #         LCTR_END_TIME STRING,
    #         LCTR_CONTENT STRING,
    #         LCTR_TARGET STRING,
    #         LCTR_WAY STRING,
    #         LCTR_DAY STRING,
    #         LCTR_LOCATE STRING,
    #         LCTR_PCSP INTEGER,
    #         LCTR_PRICE INTEGER,
    #         LCTR_ADDRESS STRING,
    #         LCTR_OPERATE STRING,
    #         OPERATE_TELEPHONE STRING,
    #         APLY_BGN STRING,
    #         APLY_END STRING,
    #         APLY_WAY STRING,
    #         SELECT_WAY STRING,
    #         APLY_URL STRING,
    #         NOTUSE1 STRING,
    #         NOTUSE2 STRING,
    #         NOTUSE3 STRING,
    #         NOTUSE4 STRING
    #     )
    # """)
    # cursor.execute(f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name}")
    
    # 테이블 full refresh
    cursor.execute(
            f"DELETE FROM {SCHEMA_NAME}.{table_name}"
        )

    ordered_cols = list(df.columns)

    # 데이터프레임을 튜플 리스트로 변환
    data_to_insert = [
        tuple(row[col] if pd.notnull(row[col]) else None for col in ordered_cols)
        for _, row in df.iterrows()
    ]
    
    # 데이터 insert
    placeholders = ','.join(['%s'] * len(ordered_cols))
    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

    if data_to_insert: # 데이터가 있을 경우에만 실행
        cursor.executemany(insert_query, data_to_insert)

    cursor.close()
    conn.commit()
    conn.close()
