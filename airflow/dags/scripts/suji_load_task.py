from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from airflow.exceptions import AirflowSkipException

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

@task
def suji_load_task(input_path, schema, table):
    # df = pd.DataFrame(records)
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
            logging.info(f"[삽입] - {schema}.{table}] {cursor.rowcount}개 행")
        
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

    return f"수지구청 데이터 적재 완료: {len(df)} 건 삽입"
