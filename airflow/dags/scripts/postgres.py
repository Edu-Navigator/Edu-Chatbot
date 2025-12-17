from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from airflow.exceptions import AirflowSkipException


import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

@task    
def table_full_refresh(data:dict, schema, table, conn_name="conn_production", **context):
    
    df = pd.DataFrame(data)
    # 빈 DataFrame 체크
    if df.empty:
        logging.warning(f"[경고] 빈 DataFrame - {schema}.{table} 적재 스킵")
        raise AirflowSkipException(" 데이터 없음: 적재 생략")

    hook = PostgresHook(postgres_conn_id=conn_name)
    conn, cursor = None, None
    try :
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("BEGIN")

        ## Full refresh
        # DELETE
        cursor.execute(
            f"DELETE FROM {schema}.{table}"
        )
        deleted_count = cursor.rowcount
        logging.info(f"[삭제 - {schema}.{table}] {deleted_count}개 행")
        
        # INSERT (executemany 사용)
        # PostgreSQL은 컬럼명에 예약어나 특수문자가 있을 수 있으므로 쌍따옴표로 감싸기
        insert_query = f"""
        INSERT INTO {schema}.{table} ({', '.join(df.columns)})
        VALUES ({', '.join(['%s'] * len(df.columns))})
        """
        
        # DataFrame을 리스트로 변환 (None 값 처리 포함)
        data_to_insert = [tuple(row) for row in df.replace({pd.NA: None, pd.NaT: None}).values]
        cursor.executemany(insert_query, data_to_insert)
        inserted_count = cursor.rowcount
        logging.info(f"[삽입 - {schema}.{table}] {inserted_count}개 행")
        
        conn.commit()
        logging.info(f"[종료] {schema}.{table} : 완료")
        
    except Exception as e:
        logging.error(f"[오류 발생] {schema}.{table} : {type(e).__name__} - {str(e)}")
        if conn:
            conn.rollback()
            logging.info(f"[롤백 완료]")
        raise
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
