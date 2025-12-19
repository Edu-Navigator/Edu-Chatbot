from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pandas as pd
import logging
from utils.lecture_common_func import *
from utils.lecture_process_func import *


# -----------------------------------------------------------
# 상수 및 설정
# -----------------------------------------------------------

logger = logging.getLogger("airflow.task")

# PostgreSQL 연결
POSTGRES_CONN_ID = "conn_production"
SCHEMA_PROCESSED = "PROCESSED"
SCHEMA_ANALYTICS = "ANALYTICS"
TABLE_LECTURE = "LECTURE"
TABLE_EDU_INFO = "EDU_INFO"
JUSO_API_URL = "https://business.juso.go.kr/addrlink/addrLinkApi.do"

# 원본 데이터 테이블
TABLE_RAW_SUJI = "RAW_DATA.SUJI_LEARNING" 
TABLE_RAW_GG = "RAW_DATA.GG_LEARNING"
# TABLE_RAW_DIGI = "RAW_DATA.DIGITAL_LEARNING_RAW"
TABLE_RAW_DIGI_END = "RAW_DATA.DIGITAL_LEARNING_END"


# -----------------------------------------------------------
# Task 정의
# -----------------------------------------------------------

@task(do_xcom_push=True)
def suji_data_processing_task():
    """
    RAW_DATA.SUJI_LEARNING 테이블의 데이터를 조회하여
    수지구청 평생학습 데이터 전처리를 수행한다.

    PostgreSQL에서 원본 데이터를 로드한 후 컬럼명을 표준화하고,
    `process_suji` 함수를 통해 LECTURE 테이블 스키마에 맞게 변환한다.
    변환 결과는 CSV 파일로 저장되며, 해당 파일 경로를 반환한다.

    Returns
    -------
    str or pandas.DataFrame
        전처리 결과 CSV 파일의 경로.
        원본 데이터가 비어 있는 경우 빈 DataFrame을 반환한다.
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_query = f"SELECT * FROM {TABLE_RAW_SUJI}"
    df_raw_suji = hook.get_pandas_df(sql_query)

    logger.info(f"DB에서 가져온 실제 컬럼명: {df_raw_suji.columns.tolist()}")

    df_raw_suji.columns = [c.upper() for c in df_raw_suji.columns]

    logger.info(f"변환된 컬럼명: {df_raw_suji.columns.tolist()}")

    logger.info(f"원본 데이터 (SUJI_LEARNING) {len(df_raw_suji)}건 로드 완료")
    
    if df_raw_suji.empty:
        logger.info("SUJI_LEARNING 테이블에 데이터가 없어 빈 DataFrame을 반환")
        return pd.DataFrame() 

    df_suji = process_suji(df_raw_suji)

    path = f"{Variable.get('DATA_DIR')}/suji.csv"
    df_suji.to_csv(path, index=False, encoding="utf-8-sig")

    logger.info(f"SUJI 처리 결과 CSV 저장 완료: {path}")
    return path


@task(do_xcom_push=True)
def gg_data_processing_task():
    """
    RAW_DATA.GG_LEARNING 테이블의 데이터를 조회하여
    경기도 평생학습 데이터 전처리를 수행한다.

    PostgreSQL에서 원본 데이터를 로드한 후 컬럼명을 표준화하고,
    `process_gg` 함수를 통해 LECTURE 테이블 스키마에 맞게 변환한다.
    변환 결과는 CSV 파일로 저장되며, 해당 파일 경로를 반환한다.

    Returns
    -------
    str or pandas.DataFrame
        전처리 결과 CSV 파일의 경로.
        원본 데이터가 비어 있는 경우 빈 DataFrame을 반환한다.
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_query = f"SELECT * FROM {TABLE_RAW_GG}"
    df_raw_gg = hook.get_pandas_df(sql_query)
    df_raw_gg.columns = [c.upper() for c in df_raw_gg.columns]
    logger.info(f"원본 데이터 (GG_LEARNING) {len(df_raw_gg)}건 로드 완료")
    
    if df_raw_gg.empty:
        logger.info("GG_LEARNING 테이블에 데이터가 없어 빈 DataFrame을 반환")
        return pd.DataFrame() 

    df_gg = process_gg(df_raw_gg)

    path = f"{Variable.get('DATA_DIR')}/gg.csv"
    df_gg.to_csv(path, index=False, encoding="utf-8-sig")

    logger.info(f"GG 처리 결과 CSV 저장 완료: {path}")
    return path


# @task(do_xcom_push=True)
# def digi_data_processing_task():
    # """
    # RAW_DATA.DIGITAL_LEARNING 테이블의 데이터를 조회하여
    # 디지털 배움터 수강 중 강좌 데이터 전처리를 수행한다.

    # PostgreSQL에서 원본 데이터를 로드한 후 컬럼명을 표준화하고,
    # `process_digi` 함수를 통해 LECTURE 테이블 스키마에 맞게 변환한다.
    # 변환 결과는 CSV 파일로 저장되며, 해당 파일 경로를 반환한다.

    # Returns
    # -------
    # str or pandas.DataFrame
    #     전처리 결과 CSV 파일의 경로.
    #     원본 데이터가 비어 있는 경우 빈 DataFrame을 반환한다.
    # """

#     hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#     sql_query = f"SELECT * FROM {TABLE_RAW_DIGI}"
#     df_raw_digi = hook.get_pandas_df(sql_query)
#     df_raw_digi.columns = [c.upper() for c in df_raw_digi.columns]
#     logger.info(f"원본 데이터 (DIGITAL_LEARNING) {len(df_raw_digi)}건 로드 완료")

#     if df_raw_digi.empty:
#         logger.info("DIGITAL_LEARNING 테이블에 데이터가 없어 빈 DataFrame을 반환")
#         return pd.DataFrame()

#     df_digi = process_digi(df_raw_digi)

#     path = f"{Variable.get('DATA_DIR')}/digi.csv"
#     df_digi.to_csv(path, index=False, encoding="utf-8-sig")

#     logger.info(f"DIGITAL_LEARNING 처리 결과 CSV 저장 완료: {path}")
#     return path


@task(do_xcom_push=True)
def digi_end_data_processing_task():
    """
    RAW_DATA.DIGITAL_LEARNING_END_RAW 테이블의 데이터를 조회하여
    디지털 배움터 수강 종료 강좌 데이터 전처리를 수행한다.

    PostgreSQL에서 원본 데이터를 로드한 후 컬럼명을 표준화하고,
    `process_digi_end` 함수를 통해 LECTURE 테이블 스키마에 맞게 변환한다.
    변환 결과는 CSV 파일로 저장되며, 해당 파일 경로를 반환한다.

    Returns
    -------
    str or pandas.DataFrame
        전처리 결과 CSV 파일의 경로.
        원본 데이터가 비어 있는 경우 빈 DataFrame을 반환한다.
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_query = f"SELECT * FROM {TABLE_RAW_DIGI_END}"
    df_raw_digi_end = hook.get_pandas_df(sql_query)
    df_raw_digi_end.columns = [c.upper() for c in df_raw_digi_end.columns]
    logger.info(f"원본 데이터 (DGITAL_LEARNING_END) {len(df_raw_digi_end)}건 로드 완료")
    
    if df_raw_digi_end.empty:
        logger.info("DGITAL_LEARNING_END 테이블에 데이터가 없어 빈 DataFrame을 반환")
        return pd.DataFrame()

    df_digi_end = process_digi_end(df_raw_digi_end)

    path = f"{Variable.get('DATA_DIR')}/digi_end.csv"
    df_digi_end.to_csv(path, index=False, encoding="utf-8-sig")

    logger.info(f"DIGITAL_LEARNING_END 처리 결과 CSV 저장 완료: {path}")
    return path


@task
def combine_and_insert_lecture_task(
    suji_path: str,
    gg_path: str,
    # digi_path: str,
    digi_end_path: str
):
    """
    SUJI, GG, DIGITAL_END 전처리 결과를 통합하여
    LECTURE 테이블에 적재하는 task.

    개별 전처리 task에서 생성된 CSV 파일을 로드하여 하나의 DataFrame으로 통합한 뒤,
    컬럼 순서를 LECTURE 테이블 스키마에 맞게 정렬하고 CSV 파일을 생성한다.
    생성된 CSV 데이터를 기반으로 PROCESSED.LECTURE 테이블에
    전체 DELETE 후 INSERT 방식으로 적재한다.

    Parameters
    ----------
    suji_path : str
        수지구청 평생학습 전처리 결과 CSV 파일 경로.
    gg_path : str
        경기도 평생학습 전처리 결과 CSV 파일 경로.
    digi_end_path : str
        디지털 배움터 수강 종료 데이터 전처리 결과 CSV 파일 경로.

    Notes
    -----
    - 입력 데이터
        - 각 upstream task에서 생성된 CSV 파일을 입력으로 사용한다.
        - 경로가 없거나 CSV가 비어 있는 경우 해당 데이터는 통합 대상에서 제외한다.
    - 데이터 통합
        - 여러 CSV 파일을 하나의 DataFrame으로 병합(concat)한다.
        - LECTURE 테이블 스키마에 맞춰 컬럼 순서를 재정렬한다.
    - 결과 저장
        - Airflow Variable `DATA_DIR` 경로 하위에
        `lecture.csv` 파일로 저장한다.
    - 적재 방식
        - PROCESSED.LECTURE 테이블에 대해 전체 DELETE 후 INSERT 방식으로 적재한다.
        - 트랜잭션 단위로 처리되며 실패 시 ROLLBACK을 수행한다.
    - 예외 처리
        - 통합 대상 데이터가 없는 경우 테이블 적재를 수행하지 않는다.
        - INSERT 중 오류 발생 시 예외를 재발생시켜 task 실패로 처리한다.
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn, cursor = None, None

    # csv 로드 및 데이터 통합
    
    paths = [suji_path, gg_path, 
            # digi_path, 
            digi_end_path]
    dfs = []

    for path in paths:
        if not path:
            continue

        df = pd.read_csv(path)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        logger.info("적재할 데이터가 없어 LECTURE 테이블 적재 스킵")
        return

    df_final = pd.concat(dfs, ignore_index=True)
    
    # LECTURE 테이블의 컬럼 순서 (LCTR_ID 제외, AUTOINCREMENT 컬럼)
    ordered_cols = [
        'APLY_URL', 'LCTR_WAY', 'LCTR_CATEGORY', 'LCTR_NM', 'APLY_WAY', 
        'APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END', 'LCTR_CONTENT', 
        'PCSP', 'IS_APLY_AVL', 'DL_NM', 'ADDRESS', 'LC_1', 'LC_2', 'LC_3', 
        'ADDRESS_X', 'ADDRESS_Y', 'LCTR_IMAGE'
    ]

    # # snowflake에 맞게 날짜 변환, 이후 삭제 또는 변경 필요한지 테스트 필요
    # date_cols = ['APLY_BGN', 'APLY_END', 'LCTR_BGN', 'LCTR_END']
    # for col in date_cols:
    #     # datetime 객체를 'YYYY-MM-DD' 형식의 문자열로 변환
    #     df_final[col] = df_final[col].dt.strftime('%Y-%m-%d')

    df_final = df_final[ordered_cols]

    # csv 생성
    path = f"{Variable.get('DATA_DIR')}/lecture.csv"
    df_final.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"lecture.csv 생성 완료: {path}")

    # PROCESSED.LECTURE 테이블에 적재
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("BEGIN")

        df_insert = pd.read_csv(path)
        if df_insert.empty:
            logger.info("lecture.csv 비어 있음: 적재 생략")
            return

        cursor.execute(f"DELETE FROM {SCHEMA_PROCESSED}.{TABLE_LECTURE}")
        logger.info(f"[DELETE] {SCHEMA_PROCESSED}.{TABLE_LECTURE} 전체 삭제")

        cols = df_insert.columns.tolist()
        placeholders = ",".join(["%s"] * len(cols))

        insert_sql = f"""
            INSERT INTO {SCHEMA_PROCESSED}.{TABLE_LECTURE}
            ({','.join(cols)})
            VALUES ({placeholders})
        """

        data = [
            tuple(row[col] if pd.notnull(row[col]) else None for col in cols)
            for _, row in df_insert.iterrows()
        ]

        if data:
            cursor.executemany(insert_sql, data)
            logger.info(f"[삽입] - {SCHEMA_PROCESSED}.{TABLE_LECTURE}] {cursor.rowcount}개 행")

        conn.commit()
        logger.info(f"[종료] {SCHEMA_PROCESSED}.{TABLE_LECTURE} : 완료")

    except Exception:
        if conn:
            conn.rollback()
        logger.exception("LECTURE 테이블 INSERT 중 오류 발생, ROLLBACK 수행")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@task
def lecture_location_image_task():
    """
    PROCESSED.LECTURE 테이블의 주소 및 카테고리 정보를 기반으로
    지역 정보, 좌표, 강의 이미지 URL을 계산하여 UPDATE하는 후처리 task.

    LECTURE 테이블을 조회한 후 주소를 기준으로 행정구역(LC_1~LC_3)과
    좌표(ADDRESS_X, ADDRESS_Y)를 계산하고,
    강의 카테고리를 기반으로 이미지 URL을 생성하여
    해당 컬럼들을 UPDATE한다.

    Notes
    -----
    - 입력 데이터
        - PROCESSED.LECTURE 테이블을 조회하여 처리한다.
        - 주소(ADDRESS), 강의 카테고리(LCTR_CATEGORY)를 사용한다.
    - 지역 및 좌표 처리
        - Kakao Local API를 이용해 주소 기반 행정구역과 좌표를 계산한다.
        - ADDRESS가 없는 경우 해당 행은 NULL로 유지한다.
    - 이미지 URL 처리
        - LCTR_IMAGE가 NULL이고 LCTR_CATEGORY가 존재하는 경우에만 이미지 URL을 생성한다.
        - 카테고리명을 파일명으로 사용하여 S3 이미지 경로를 구성한다.
    - 적재 방식
        - LCTR_ID를 기준으로 PROCESSED.LECTURE 테이블을 UPDATE한다.
        - executemany를 사용하여 일괄 UPDATE를 수행한다.
    - 트랜잭션 처리
        - UPDATE 작업은 트랜잭션 단위로 처리되며,
        오류 발생 시 commit되지 않는다.
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # lecture 테이블 로드
    sql = f"""
        SELECT
            LCTR_ID,
            ADDRESS,
            LCTR_CATEGORY,
            LCTR_IMAGE
        FROM {SCHEMA_PROCESSED}.{TABLE_LECTURE}
    """
    df = hook.get_pandas_df(sql)
    df.columns = [c.upper() for c in df.columns]

    # 주소 기반 지역/좌표 계산
    def resolve_row(row):
        KAKAO_API_KEY = Variable.get("kakao_api_key")
        
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

    # PROCESSED.LECTURE 테이블에 적재
    conn = hook.get_conn()
    cursor = conn.cursor()

    update_sql = f"""
        UPDATE {SCHEMA_PROCESSED}.{TABLE_LECTURE}
        SET
            lc_1 = %s,
            lc_2 = %s,
            lc_3 = %s,
            address_x = %s,
            address_y = %s,
            lctr_image = %s
        WHERE lctr_id = %s
    """

    cursor.executemany(
        update_sql,
        [
            (
                r.LC_1, r.LC_2, r.LC_3,
                r.ADDRESS_X, r.ADDRESS_Y,
                r.LCTR_IMAGE, r.LCTR_ID
            )
            for r in df.itertuples()
        ]
    )

    conn.commit()
    logger.info("LECTURE 테이블 지역 정보, 좌표, 이미지 URL UPDATE 완료")

    cursor.close()
    conn.close()


@task
def edu_info_task():
    """
    PROCESSED.LECTURE 테이블에서 신청 가능한 강의 데이터만 필터링하여
    ANALYTICS.EDU_INFO 테이블에 적재하는 task.

    PROCESSED.LECTURE 테이블을 조회한 후
    신청 가능 여부(is_aply_avl = true) 조건을 만족하는 데이터만 선택하여,
    ANALYTICS.EDU_INFO 테이블에 전체 DELETE 후 INSERT 방식으로 적재한다.

    Notes
    -----
    - 입력 데이터
        - PROCESSED.LECTURE 테이블을 조회하여 처리한다.
    - 데이터 필터링
        - is_aply_avl = true 인 데이터만 적재 대상에 포함한다.
    - 적재 방식
        - ANALYTICS.EDU_INFO 테이블에 대해 전체 DELETE 후 INSERT 방식으로 적재한다.
    - 트랜잭션 처리
        - DELETE 및 INSERT는 각각 독립적으로 실행된다.
        - SQL 실행 중 오류 발생 시 해당 task는 실패 처리된다.
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # 1. DELETE
    delete_sql = f"""
        DELETE FROM {SCHEMA_ANALYTICS}.{TABLE_EDU_INFO};
    """
    hook.run(delete_sql, autocommit=True)
    logger.info(f"[삭제] {SCHEMA_ANALYTICS}.{TABLE_EDU_INFO} 데이터 삭제 완료")

    # 2. INSERT
    insert_sql = f"""
        INSERT INTO {SCHEMA_ANALYTICS}.{TABLE_EDU_INFO}
        SELECT *
        FROM {SCHEMA_PROCESSED}.{TABLE_LECTURE}
        WHERE is_aply_avl = true;
    """
    hook.run(insert_sql, autocommit=True)
    logger.info(
        f"[삽입] {SCHEMA_ANALYTICS}.{TABLE_EDU_INFO} 적재 완료 (is_aply_avl = true)"
    )