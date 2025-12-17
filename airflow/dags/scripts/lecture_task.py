from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
import pandas as pd
import numpy as np
from datetime import date
from utils.lecture_common_func import *
from utils.kakaomap_api import *


# -----------------------------------------------------------
# 상수 및 설정
# -----------------------------------------------------------

# Snowflake 연결
SNOWFLAKE_CONN_ID = 'snowflake_conn'
DB_NM = "DIGIEDU"
SCHEMA_PROCESSED = "PROCESSED"
TABLE_LECTURE = "LECTURE"
JUSO_API_URL = "https://business.juso.go.kr/addrlink/addrLinkApi.do"
JUSO_API_KEY = Variable.get("juso_api_key")
KAKAO_API_KEY = Variable.get("kakao_api_key")


# 원본 데이터 테이블
TABLE_RAW_SUJI = "RAW_DATA.SUJI_LEARNING" 
TABLE_RAW_GG = "RAW_DATA.GG_LEARNING"
TABLE_RAW_DIGI = "RAW_DATA.DIGITAL_LEARNING_RAW" 
TABLE_RAW_DIGI_END = "RAW_DATA.DIGITAL_LEARNING_END_RAW"

# LECTURE 테이블 생성 SQL
CREATE_LECTURE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {DB_NM}.{SCHEMA_PROCESSED}.{TABLE_LECTURE} (
    LCTR_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    APLY_URL TEXT,
    LCTR_WAY TEXT,
    LCTR_CATEGORY TEXT,
    LCTR_NM TEXT,
    APLY_WAY TEXT,
    APLY_BGN TIMESTAMP,
    APLY_END TIMESTAMP,
    LCTR_BGN TIMESTAMP,
    LCTR_END TIMESTAMP,
    LCTR_CONTENT TEXT,
    PCSP INTEGER,
    IS_APLY_AVL BOOLEAN,

    DL_NM TEXT,
    ADDRESS TEXT,
    LC_1 TEXT,
    LC_2 TEXT,
    LC_3 TEXT,
    ADDRESS_X TEXT,
    ADDRESS_Y TEXT,
    LCTR_IMAGE TEXT
);
"""

# -----------------------------------------------------------
# 개별 데이터 처리 함수 (테이블별 데이터 파싱)
# -----------------------------------------------------------

def process_suji(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    수지구청 데이터를 LECTURE 테이블 형식에 맞게 Pandas로 변환 및 파싱.
    """
    if df_raw.empty:
        return pd.DataFrame()
    
    
    # [1단계] Raw 데이터 파싱 (LECTURE 스키마의 임시 DataFrame 구성)
    temp_df = pd.DataFrame(index=df_raw.index)

    # 1. APLY_URL
    temp_df['APLY_URL'] = df_raw['APLY_URL']

    # 2. LCTR_WAY (고정값: 오프라인)
    temp_df['LCTR_WAY'] = '오프라인'

    # 3. LCTR_CATEGORY (초기 NULL 처리 - 공통 함수에서 채워짐)
    temp_df['LCTR_CATEGORY'] = None

    # 4. LCTR_NM
    temp_df['LCTR_NM'] = df_raw['LCTR_NM']

    # 5. APLY_WAY (고정값: 온라인)
    temp_df['APLY_WAY'] = '온라인'
    
    # 6. APLY_BGN_END 파싱
    apply_bgn_str = df_raw['APLY_BGN_END'].str.split('~', expand=True)[0]
    apply_end_str = df_raw['APLY_BGN_END'].str.split('~', expand=True)[1]
    
    # 6-1. APLY_BGN (날짜만)
    apply_bgn_date_only = apply_bgn_str.str.split('(', expand=True)[0].str.strip()
    temp_df['APLY_BGN'] = pd.to_datetime(apply_bgn_date_only, errors='coerce')
    # 6-2. APLY_END (날짜만)
    apply_end_date_only = apply_end_str.str.split('(', expand=True)[0].str.strip()
    temp_df['APLY_END'] = pd.to_datetime(apply_end_date_only, errors='coerce')

    # 7. LCTR_BGN_END 파싱
    lctr_bgn_str = df_raw['LCTR_BGN_END'].str.split('~', expand=True)[0]
    lctr_end_str = df_raw['LCTR_BGN_END'].str.split('~', expand=True)[1]

    # 7-1. LCTR_BGN (날짜만)
    temp_df['LCTR_BGN'] = pd.to_datetime(lctr_bgn_str.str.strip(), errors='coerce')
    # 7-2. LCTR_END (날짜만)
    temp_df['LCTR_END'] = pd.to_datetime(lctr_end_str.str.split('(', expand=True)[0].str.strip(), errors='coerce')
    
    # 8. LCTR_CONTENT (강의명 재사용)
    temp_df['LCTR_CONTENT'] = df_raw['LCTR_NM']
    
    # 9. PCSP (정원 24로 고정)
    temp_df['PCSP'] = 24
    
    # 10. IS_APLY_AVL
    current_date = pd.to_datetime(date.today()) 
    temp_df['IS_APLY_AVL'] = (temp_df['APLY_END'].notna()) & (temp_df['APLY_END'] >= current_date)
    
    # 11. 주소 및 장소 관련 데이터 (우선 NULL 처리)
    temp_df['DL_NM'] = df_raw['LCTR_LOCATE']
    temp_df['ADDRESS'] = '경기도 용인시 수지구 포은대로 435 수지구청,수지보건소'
    temp_df['LC_1'] = None
    temp_df['LC_2'] = None
    temp_df['LC_3'] = None
    temp_df['ADDRESS_X'] = None
    temp_df['ADDRESS_Y'] = None
    
    # 12. LCTR_IMAGE
    temp_df['LCTR_IMAGE'] = None
    
    # [2단계] 공통 필터링 및 카테고리/ROW 분리 로직
    # 1. 강의 필터링
    df_filtered = filter_data(temp_df)
    
    # 2. 카테고리 매핑 및 Row 분리 (3번 조건)
    df_final = map_and_explode_categories(df_filtered)

    # LECTURE 테이블의 최종 컬럼 순서 조정
    final_columns = [
        'APLY_URL', 'LCTR_WAY', 'LCTR_CATEGORY', 'LCTR_NM', 'APLY_WAY', 
        'APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END', 'LCTR_CONTENT', 
        'PCSP', 'IS_APLY_AVL', 'DL_NM', 'ADDRESS', 'LC_1', 'LC_2', 'LC_3', 
        'ADDRESS_X', 'ADDRESS_Y', 'LCTR_IMAGE'
    ]
    return df_final[final_columns]


def process_gg(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    경기도 평생학습 데이터를 LECTURE 테이블 형식에 맞게 변환 및 파싱.
    """
    if df_raw.empty:
        return pd.DataFrame()
    
    # [1단계] Raw 데이터 파싱 (LECTURE 스키마의 임시 DataFrame 구성)
    temp_df = pd.DataFrame(index=df_raw.index)

    # 1. APLY_URL
    temp_df['APLY_URL'] = df_raw['APLY_URL']
    
    # 2. LCTR_WAY (강의 방식) 매핑
    def map_lctr_way(way):
        if way in ['온라인']:
            return '온라인'
        elif way in ['오프라인']:
            return '오프라인'
        elif way in ['온라인+오프라인', '혼합']:
            return '혼합'
        return None # 매핑되지 않는 경우
        
    temp_df['LCTR_WAY'] = df_raw['LCTR_WAY'].apply(map_lctr_way)
    
    # 3. LCTR_CATEGORY (초기 NULL 처리 - 공통 함수에서 채워짐)
    temp_df['LCTR_CATEGORY'] = None
    
    # 4. LCTR_NM
    temp_df['LCTR_NM'] = df_raw['LCTR_NM']
    
    # 5. APLY_WAY (신청 방식) 매핑
    def map_aply_way(way):
        if way in ['인터넷', '온라인', '온라인접수']:
            return '온라인'
        elif way in ['방문', '방문접수']:
            return '방문접수'
        return '온라인 또는 방문접수' # 나머지
        
    temp_df['APLY_WAY'] = df_raw['APLY_WAY'].apply(map_aply_way)
    
    # 6, 7, 8, 9. 날짜 변환 (Raw 데이터는 '2025-03-04' 형태의 문자열)
    # pd.to_datetime을 사용하여 날짜 타입으로 일괄 변환
    temp_df['APLY_BGN'] = pd.to_datetime(df_raw['APLY_BGN'], errors='coerce')
    temp_df['APLY_END'] = pd.to_datetime(df_raw['APLY_END'], errors='coerce')
    temp_df['LCTR_BGN'] = pd.to_datetime(df_raw['LCTR_BGN'], errors='coerce')
    temp_df['LCTR_END'] = pd.to_datetime(df_raw['LCTR_END'], errors='coerce')
    
    # 10. LCTR_CONTENT 파싱 및 변환
    # '&amp;gt' -> ', ' 변환
    temp_df['LCTR_CONTENT'] = df_raw['LCTR_CONTENT'].astype(str).str.replace('&amp;gt', ', ', regex=False)

    # 11. PCSP
    temp_df['PCSP'] = pd.to_numeric(df_raw['LCTR_PCSP'], errors='coerce').fillna(0).astype(int)
    
    # 12. IS_APLY_AVL
    current_date = pd.to_datetime(date.today()) 
    # APLY_END가 오늘보다 작으면 (과거면) False, 아니면 True
    temp_df['IS_APLY_AVL'] = (temp_df['APLY_END'].notna()) & (temp_df['APLY_END'] >= current_date)
    
    # 13. DL_NM (교육 장소/운영처) 로직
    def map_dl_nm(operate, locate):
        operate = str(operate).strip()
        locate = str(locate).strip()
        # 1) LCTR_OPERATE == LCTR_LOCATE
        if operate == locate:
            return locate
        # 2) LCTR_OPERATE == ‘경기도 수원시평생학습관’
        if operate == '경기도 수원시평생학습관':
            return locate
        # 3) LCTR_OPERATE 앞의 3글자 == LCTR_LOCATE 앞의 3글자
        if operate[:3] == locate[:3] and len(operate) >= 3 and len(locate) >= 3:
            return locate
        # 4) 나머지
        return f"{operate} - {locate}"
        
    temp_df['DL_NM'] = df_raw.apply(
        lambda row: map_dl_nm(row['LCTR_OPERATE'], row['LCTR_LOCATE']), 
        axis=1
    )
    
    # 14. [2단계]에서 주소 검색 및 좌표 변환을 위해 LCTR_OPERATE, LCTR_LOCATE 보존
    temp_df['LCTR_OPERATE'] = df_raw['LCTR_OPERATE'] 
    temp_df['LCTR_LOCATE'] = df_raw['LCTR_LOCATE']

    # 15. 주소 및 장소 관련 데이터 (우선 NULL 처리)
    temp_df['ADDRESS'] = None
    temp_df['LC_1'] = None
    temp_df['LC_2'] = None
    temp_df['LC_3'] = None
    temp_df['ADDRESS_X'] = None
    temp_df['ADDRESS_Y'] = None
    temp_df['LCTR_IMAGE'] = None
    
    # [2단계] 공통 필터링 및 카테고리/ROW 분리 로직
    # 1. 강의 필터링 (필수)
    df_filtered = filter_data(temp_df)

    def resolve_gg_address(row) -> str | None:
        operate = str(row['LCTR_OPERATE']).strip()
        locate = str(row['LCTR_LOCATE']).strip()

        # 1차 키워드 결정
        if operate == '디지털배움터':
            primary_keyword = locate
        else:
            primary_keyword = operate

        # 1차 검색
        address = fetch_road_address(primary_keyword, JUSO_API_KEY, JUSO_API_URL)
        if address:
            return address

        # 2차 검색: locate 사용 (의미 없는 값 제외)
        if locate in ('평생학습관', '온라인'):
            return None

        return fetch_road_address(locate, JUSO_API_KEY, JUSO_API_URL)
    
    # df_filtered는 LCTR_OPERATE와 LCTR_LOCATE를 포함하므로 apply 가능
    df_filtered['ADDRESS'] = df_filtered.apply(resolve_gg_address, axis=1)
    
    # 2. 카테고리 매핑 및 Row 분리
    df_final = map_and_explode_categories(df_filtered)

    # LECTURE 테이블의 최종 컬럼 순서 조정
    final_columns = [
        'APLY_URL', 'LCTR_WAY', 'LCTR_CATEGORY', 'LCTR_NM', 'APLY_WAY', 
        'APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END', 'LCTR_CONTENT', 
        'PCSP', 'IS_APLY_AVL', 'DL_NM', 'ADDRESS', 'LC_1', 'LC_2', 'LC_3', 
        'ADDRESS_X', 'ADDRESS_Y', 'LCTR_IMAGE'
    ]
    return df_final[final_columns]


def process_digi(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    디지털 배움터 (수강신청 가능) 데이터를 LECTURE 테이블 형식에 맞게 변환 및 파싱.
    """
    if df_raw.empty:
        return pd.DataFrame()

def process_digi_end(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    디지털 배움터 (수강 종료) 데이터를 LECTURE 테이블 형식에 맞게 변환 및 파싱.
    """
    if df_raw.empty:
        return pd.DataFrame()

    # [1단계] Raw 데이터 파싱 (LECTURE 스키마의 임시 DataFrame 구성)
    temp_df = pd.DataFrame(index=df_raw.index)

    # 1. APLY_URL
    temp_df['APLY_URL'] = df_raw['APLY_URL']

    # 2. LCTR_WAY (강의 방식) 매핑
    def map_lctr_way(way):
        way = str(way).strip()
        if way in ['집합', '에듀버스']:
            return '오프라인'
        elif way in ['온라인']:
            return '온라인'
        return None 
        
    temp_df['LCTR_WAY'] = df_raw['LCTR_WAY'].apply(map_lctr_way)

    # 3. LCTR_CATEGORY (초기 NULL 처리 - 공통 함수에서 채워짐)
    temp_df['LCTR_CATEGORY'] = None
    
    # 4. LCTR_NM
    temp_df['LCTR_NM'] = df_raw['LCTR_NM']

    # 5. APLY_WAY: '온라인' 고정
    temp_df['APLY_WAY'] = '온라인'

    # 6. APLY_BGN: NULL 고정
    temp_df['APLY_BGN'] = None

    # 7. APLY_END: NULL 고정
    temp_df['APLY_END'] = None

    # 8, 9. LCTR_BGN, LCTR_END 파싱 (예시: '2025-11-17 ~ 2025-11-17 (1 일)')
    
    # ' ~ '를 기준으로 앞 부분(시작일) 추출
    lctr_bgn_series = df_raw['LCTR_BGN_END'].astype(str).str.split('~', expand=True)[0].str.strip()
    # ' ~ '를 기준으로 뒷 부분, 그리고 그 부분에서 ' (' 앞 부분(종료일) 추출
    lctr_end_series_temp = df_raw['LCTR_BGN_END'].astype(str).str.split('~', expand=True)[1].str.strip()
    lctr_end_series = lctr_end_series_temp.str.split('(', expand=True)[0].str.strip()

    temp_df['LCTR_BGN'] = pd.to_datetime(lctr_bgn_series, errors='coerce')
    temp_df['LCTR_END'] = pd.to_datetime(lctr_end_series, errors='coerce')
    
    # 10. LCTR_CONTENT
    temp_df['LCTR_CONTENT'] = df_raw['LCTR_CONTENT']

    # 11. PCSP: NONE 고정
    temp_df['PCSP'] = None

    # 12. IS_APLY_AVL: FALSE 고정
    temp_df['IS_APLY_AVL'] = False

    # 13, 14 DL_NM, ADDRESS 추출
    parsed = df_raw['DL_NM'].apply(parse_dl_nm_and_address)

    temp_df['ADDRESS'] = parsed.apply(lambda x: x[0])
    temp_df['DL_NM'] = parsed.apply(lambda x: x[1])
    
    # 15. NULL 값 지정 컬럼
    temp_df['LC_1'] = None
    temp_df['LC_2'] = None
    temp_df['LC_3'] = None
    temp_df['ADDRESS_X'] = None
    temp_df['ADDRESS_Y'] = None
    temp_df['LCTR_IMAGE'] = None
    
    # [2단계] 공통 필터링 및 카테고리/ROW 분리 로직
    # 1. 강의 필터링
    df_filtered = filter_data(temp_df)
    
    # 2. 카테고리 매핑 및 Row 분리
    df_final = map_and_explode_categories(df_filtered)

    # LECTURE 테이블의 최종 컬럼 순서 조정
    final_columns = [
        'APLY_URL', 'LCTR_WAY', 'LCTR_CATEGORY', 'LCTR_NM', 'APLY_WAY', 
        'APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END', 'LCTR_CONTENT', 
        'PCSP', 'IS_APLY_AVL', 'DL_NM', 'ADDRESS', 'LC_1', 'LC_2', 'LC_3', 
        'ADDRESS_X', 'ADDRESS_Y', 'LCTR_IMAGE'
    ]
    return df_final[final_columns]



# -----------------------------------------------------------
# Airflow Task 정의
# -----------------------------------------------------------

@task
def create_lecture_table_task():
    """ 
    Task 1: LECTURE 테이블을 정의하고 생성. 
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # DB/Schema 선택
        cursor.execute(f"USE DATABASE {DB_NM}")
        cursor.execute(f"USE SCHEMA {SCHEMA_PROCESSED}")
        
        # 테이블 생성 SQL 실행
        cursor.execute(CREATE_LECTURE_TABLE_SQL)
        
        conn.commit()
        print(f"LECTURE 테이블 생성 완료")
        
    except Exception as e:
        conn.rollback()
        print(f"LECTURE 테이블 생성 실패: {e}")
        raise
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@task(do_xcom_push=True)
def suji_data_processing_task():
    """ 
    Task 2: SUJI_LEARNING 데이터를 처리하고 DataFrame을 XCom으로 반환.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql_query = f"SELECT * FROM {DB_NM}.{TABLE_RAW_SUJI}"
    df_raw_suji = hook.get_pandas_df(sql_query)
    print(f"원본 데이터 (SUJI_LEARNING) {len(df_raw_suji)}건 로드 완료")
    
    if df_raw_suji.empty:
        print("SUJI_LEARNING 테이블에 데이터가 없어 빈 DataFrame을 반환")
        return pd.DataFrame() 

    df_suji = process_suji(df_raw_suji)
    print(f"데이터 처리 완료 (SUJI_LEARNING) {len(df_suji)}건")
    return df_suji


@task(do_xcom_push=True)
def gg_data_processing_task():
    """ 
    Task 3: GG_LEARNING 데이터를 처리하고 DataFrame을 XCom으로 반환.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql_query = f"SELECT * FROM {DB_NM}.{TABLE_RAW_GG}"
    df_raw_gg = hook.get_pandas_df(sql_query)
    print(f"원본 데이터 (GG_LEARNING) {len(df_raw_gg)}건 로드 완료")
    
    if df_raw_gg.empty:
        print("GG_LEARNING 테이블에 데이터가 없어 빈 DataFrame을 반환")
        return pd.DataFrame() 

    df_gg = process_gg(df_raw_gg)
    print(f"데이터 처리 완료 (GG_LEARNING) {len(df_gg)}건")
    return df_gg


@task(do_xcom_push=True)
def digi_data_processing_task():
    """ 
    Task 4: 디지털 배움터(신청 가능) 데이터를 처리하고 DataFrame을 XCom으로 반환.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql_query = f"SELECT * FROM {DB_NM}.{TABLE_RAW_DIGI}"
    df_raw_digi = hook.get_pandas_df(sql_query)
    print(f"원본 데이터 (DIGITAL_LEARNING) {len(df_raw_digi)}건 로드 완료")
    
    if df_raw_digi.empty:
        print("DIGITAL_LEARNING 테이블에 데이터가 없어 빈 DataFrame을 반환")
        return pd.DataFrame()

    df_digi = process_digi(df_raw_digi)
    print(f"데이터 처리 완료 (DIGITAL_LEARNING) {len(df_digi)}건")
    return df_digi


@task(do_xcom_push=True)
def digi_end_data_processing_task():
    """ 
    Task 5: 디지털 배움터(수강 종료) 데이터를 처리하고 DataFrame을 XCom으로 반환.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql_query = f"SELECT * FROM {DB_NM}.{TABLE_RAW_DIGI_END}"
    df_raw_digi_end = hook.get_pandas_df(sql_query)
    print(f"원본 데이터 (DGITAL_LEARNING_END) {len(df_raw_digi_end)}건 로드 완료")
    
    if df_raw_digi_end.empty:
        print("DGITAL_LEARNING_END 테이블에 데이터가 없어 빈 DataFrame을 반환합니다.")
        return pd.DataFrame()

    df_digi_end = process_digi_end(df_raw_digi_end)
    print(f"데이터 처리 완료 (DGITAL_LEARNING_END) {len(df_digi_end)}건")
    return df_digi_end


@task
def combine_and_insert_lecture_task(df_suji: pd.DataFrame, df_gg: pd.DataFrame, df_digi: pd.DataFrame, df_digi_end: pd.DataFrame):
    """
    Task 6: 4개의 DataFrame을 통합하여 LECTURE 테이블에 INSERT.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # ------------------------------------------------------------------
    # 1. 데이터 통합 및 준비
    # ------------------------------------------------------------------
    
    all_dataframes = [df_suji, df_gg, df_digi, df_digi_end]
    valid_dataframes = [df for df in all_dataframes if not df.empty]
    
    if not valid_dataframes:
        print("LECTURE 테이블에 삽입할 데이터가 없음")
        return

    df_final = pd.concat(valid_dataframes, ignore_index=True)
    
    # LECTURE 테이블의 컬럼 순서 (LCTR_ID 제외, AUTOINCREMENT 컬럼)
    ordered_cols = [
        'APLY_URL', 'LCTR_WAY', 'LCTR_CATEGORY', 'LCTR_NM', 'APLY_WAY', 
        'APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END', 'LCTR_CONTENT', 
        'PCSP', 'IS_APLY_AVL', 'DL_NM', 'ADDRESS', 'LC_1', 'LC_2', 'LC_3', 
        'ADDRESS_X', 'ADDRESS_Y', 'LCTR_IMAGE'
    ]

    # snowflake에 맞게 날짜 변환, 이후 삭제 또는 변경 필요한지 테스트 필요
    date_cols = ['APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END']
    for col in date_cols:
        # datetime 객체를 'YYYY-MM-DD' 형식의 문자열로 변환
        df_final[col] = df_final[col].dt.strftime('%Y-%m-%d')

    df_final = df_final[ordered_cols]
    nrows = len(df_final)
    print(f"총 {nrows}건의 데이터 통합 완료")

    # ------------------------------------------------------------------
    # 2. Snowflake SQL 작업: TRUNCATE & INSERT
    # ------------------------------------------------------------------
    
    table_full_name = f"{DB_NM}.{SCHEMA_PROCESSED}.{TABLE_LECTURE}"
    
    try:
        # DB/Schema 선택
        cursor.execute(f"USE DATABASE {DB_NM}")
        cursor.execute(f"USE SCHEMA {SCHEMA_PROCESSED}")

        # A. 기존 데이터 TRUNCATE
        truncate_sql = f"TRUNCATE TABLE {table_full_name}"
        cursor.execute(truncate_sql)

        # B. 데이터프레임을 튜플 리스트로 변환 (None/NaN 처리 포함)
        data_to_insert = [
            tuple(row[col] if pd.notnull(row[col]) else None for col in ordered_cols)
            for _, row in df_final.iterrows()
        ]
        
        # C. INSERT 실행
        if data_to_insert:
            placeholders = ','.join(['%s'] * len(ordered_cols))
            insert_query = f"INSERT INTO {table_full_name} ({', '.join(ordered_cols)}) VALUES ({placeholders})"
            
            cursor.executemany(insert_query, data_to_insert)
            print(f"통합 데이터 ({nrows}건) -> LECTURE 테이블 최종 INSERT")
        else:
            print("삽입할 데이터가 없어 INSERT 작업 수행X")

        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"데이터 적재 실패: {e}")
        raise
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@task
def lecture_location_image_task():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # lecture 테이블 로드
    sql = f"""
        SELECT
            LCTR_ID,
            ADDRESS,
            LCTR_CATEGORY,
            LCTR_IMAGE
        FROM {DB_NM}.{SCHEMA_PROCESSED}.{TABLE_LECTURE}
    """
    df = hook.get_pandas_df(sql)

    # 주소 기반 지역/좌표 계산
    def resolve_row(row):
        if pd.isna(row["ADDRESS"]) or row["ADDRESS"] is None:
            return pd.Series([None, None, None, None, None])

        lc_1, lc_2, lc_3, x, y = resolve_lc_and_coord(row["ADDRESS"], KAKAO_API_KEY)
        return pd.Series([lc_1, lc_2, lc_3, x, y])

    df[["LC_1", "LC_2", "LC_3", "ADDRESS_X", "ADDRESS_Y"]] = df.apply(
        resolve_row,
        axis=1
    )

    # 카테고리 -> 이미지 URL 매핑
    BASE_IMAGE_URL = "https://team7-batch.s3.ap-northeast-2.amazonaws.com/images/made/"

    mask = df["LCTR_IMAGE"].isna() & df["LCTR_CATEGORY"].notna()

    df.loc[mask, "LCTR_IMAGE"] = (
        BASE_IMAGE_URL
        + df.loc[mask, "LCTR_CATEGORY"].astype(str)
        + ".jpg"
    )

    # Snowflake UPDATE
    conn = hook.get_conn()
    cursor = conn.cursor()

    update_sql = f"""
        UPDATE {DB_NM}.{SCHEMA_PROCESSED}.{TABLE_LECTURE}
        SET
            LC_1 = %s,
            LC_2 = %s,
            LC_3 = %s,
            ADDRESS_X = %s,
            ADDRESS_Y = %s,
            LCTR_IMAGE = %s
        WHERE LCTR_ID = %s
    """

    update_data = [
        (
            row.LC_1,
            row.LC_2,
            row.LC_3,
            row.ADDRESS_X,
            row.ADDRESS_Y,
            row.LCTR_IMAGE,
            row.LCTR_ID,
        )
        for row in df.itertuples(index=False)
    ]

    cursor.executemany(update_sql, update_data)
    conn.commit()

    cursor.close()
    conn.close()