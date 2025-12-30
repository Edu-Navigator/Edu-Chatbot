from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
import pandas as pd

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

@task
def gg_load_task(input_path, schema, table):
    """
    경기도 평생학습 OpenAPI 수집 결과 CSV 파일을 PostgreSQL 테이블에 적재한다.

    입력된 CSV 파일을 읽어 전체 데이터를 PostgreSQL 테이블에 적재하며,
    적재 전 기존 데이터는 모두 삭제한다.
    데이터가 없는 경우에는 task를 Skip 처리한다.

    Parameters
    ----------
    input_path : str
        OpenAPI 수집 결과 CSV 파일 경로이다.
    schema : str
        적재 대상 PostgreSQL 스키마 이름이다.
    table : str
        적재 대상 PostgreSQL 테이블 이름이다.

    Returns
    -------
    str
        적재 완료 메시지 문자열이다.
        삽입된 데이터 건수를 포함한다.

    Notes
    -----
    - 데이터 출처
        - 경기도 평생학습 OpenAPI 수집 결과 CSV
    - 적재 방식
        - CSV 파일 로드 후 전체 DELETE → INSERT 방식이다.
        - 트랜잭션 단위로 처리되며 실패 시 ROLLBACK을 수행한다.
    - 데이터가 없는 경우
        - CSV 파일 내 데이터가 0건인 경우이다.
        - AirflowSkipException을 발생시켜 task를 Skip 처리한다.
    - 적재 대상 테이블
        - (schema.table) 형태로 동적 지정한다.
    """

    df = pd.read_csv(input_path)
    if len(df) == 0:
        raise AirflowSkipException("수지구청 데이터 없음: 적재 생략")

    hook = PostgresHook(postgres_conn_id="conn_production")
    conn, cursor = None, None

    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        
        cursor.execute(f"DELETE FROM {schema}.{table}")
        logging.info(f"[삭제 - {schema}.{table}] {cursor.rowcount}개 행")

        cols = df.columns.tolist()
        placeholders = ",".join(["%s"] * len(cols))

        insert_sql = f"""
            INSERT INTO {schema}.{table}
            ({','.join(cols)})
            VALUES ({placeholders})
        """

        data = [
            tuple(row[col] if pd.notnull(row[col]) else None for col in cols)
            for _, row in df.iterrows()
        ]

        if data:
            cursor.executemany(insert_sql, data)
            logging.info(f"[삽입 - {schema}.{table}] {cursor.rowcount}개 행")

        conn.commit()
        logging.info(f"[종료] {schema}.{table} : 완료")

    except Exception as e:
        logging.error(f"[오류 발생] {schema}.{table} : {type(e).__name__} - {str(e)}")
        if conn:
            conn.rollback()
            logging.info(f"[롤백 완료]")
        raise RuntimeError(f"수지구청 데이터 적재 오류: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return f"경기도 평생학습 데이터 적재 완료: 총 {len(df)}건 삽입"
