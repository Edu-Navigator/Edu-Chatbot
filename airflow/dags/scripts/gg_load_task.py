from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

@task
def gg_load_task(records: list):
    if not records:
        return "경기도 평생학습 데이터 없음: 적재 생략"

    df = pd.DataFrame(records)

    SCHEMA_NAME = "RAW_DATA"
    TABLE_NAME = "GG_LEARNING"

    hook = PostgresHook(postgres_conn_id="conn_production")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}")

        cols = df.columns.tolist()
        placeholders = ",".join(["%s"] * len(cols))

        insert_sql = f"""
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
            ({','.join(cols)})
            VALUES ({placeholders})
        """

        data = [
            tuple(row[col] if pd.notnull(row[col]) else None for col in cols)
            for _, row in df.iterrows()
        ]

        if data:
            cursor.executemany(insert_sql, data)

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"경기도 평생학습 데이터 적재 오류: {e}")

    finally:
        cursor.close()
        conn.close()

    return f"경기도 평생학습 데이터 적재 완료: 총 {len(df)}건 삽입"
