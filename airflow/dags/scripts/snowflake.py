from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd

import logging
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

def generate_create_table_sql(df, table_name, schema="PUBLIC"):
    # pandas dtype → Snowflake 타입 매핑
    dtype_map = {
        "int64": "NUMBER",
        "float64": "FLOAT",
        "object": "VARCHAR",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP_NTZ",
    }

    columns_sql = []
    for col, dtype in df.dtypes.items():
        snow_type = dtype_map.get(str(dtype), "VARCHAR")
        columns_sql.append(f'"{col}" {snow_type}')

    columns_str = ",\n    ".join(columns_sql)

    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        {columns_str}
    );
    """
    return sql.strip()


@task    
def insert_to_snowflake(data:dict, schema, table, conn_id="snowflake_conn", **context):
    
    df = pd.DataFrame(data)

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn, cursor = None, None
    try :
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("BEGIN")
        
        # CREATE TABLE IF NOT EXIST
        create_query = generate_create_table_sql(df, table_name=table, schema=schema)
        logging.info(f"[생성 - {schema}.{table}] \n{create_query}")
        cursor.execute(create_query)

        ## Full refresh
        # DELETE
        cursor.execute(
            f"DELETE FROM {schema}.{table}"
        )
        logging.info(f"[삭제 - {schema}.{table}] {cursor.rowcount}개 행")
        
        # INSERT (executemany 사용 - 가장 안전)
        insert_query = f"""
        INSERT INTO {schema}.{table} ({', '.join(df.columns.tolist())})
        VALUES ({', '.join(['%s'] * len(df.columns))})
        """
        cursor.executemany(insert_query, df.values.tolist())
        logging.info(f"[삽입 - {schema}.{table}] {cursor.rowcount}개 행")
        
        cursor.execute("COMMIT")
        logging.info(f"[종료] {schema}.{table} : 완료")
    except Exception as e:
        logging.error(f"[오류 발생] {schema}.{table} : {type(e).__name__} - {str(e)}")
        cursor.execute("ROLLBACK")
        logging.info(f"[롤백 완료]")
    finally :
        cursor.close()
        conn.close()
