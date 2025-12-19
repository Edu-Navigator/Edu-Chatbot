from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

# 함수 설정
def print_test():
    print('Hello World')

with DAG(
    'CD_test',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag :
    # task 설정
    t1 = PythonOperator( 
        task_id = 'print_test', #task 이름 설정
        python_callable=print_test, # 불러올 함수 설정
        dag=dag #dag 정보 
    )