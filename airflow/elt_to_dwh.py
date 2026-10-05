from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import day_ago
from datetime import timedelta
import sys
import subprocess
import os

sys.path.append('')
sys.path.append('')
sys.path.append('')
from crawl_ohlcv_company import crawl_extract_companies
from crawl_ohlcv_index import crawl_index
from crawl_ohlcv_market_macro import crawl_industries
from crawl_RSS_news import crawl_industries

from load_api_to_parquet import load_api_to_parquet
from load_db_to_parquet import load_db_to_parquet

from transform_to_datawarehouse_1 import transform_to_datawarehouse_1
from transform_to_datawarehouse_2 import transform_to_datawarehouse_2
from transform_to_datawarehouse_3 import transform_to_datawarehouse_3
from transform_to_datawarehouse_4 import transform_to_datawarehouse_4
from transform_to_datawarehouse_5 import transform_to_datawarehouse_5

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def run_shell_script():
    os.environ['PATH'] = '/bin:/usr/bin:' + os.environ['PATH']
    result = subprocess.run(['/bin/bash', '/home/anhcu/Final_ETL_App/etl-app/elt/scripts/load/load_parquet_to_hdfs.sh'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise Exception(f"Shell script failed with error: {result.stderr.decode()}")

with DAG(
    dag_id='ELT_to_Data_Warehouse',
    default_args=default_args,
    description='ETL DAG for Data Warehouse',
    schedule_interval='1 1 * * *',  # Chạy khi được kích hoạt bởi DAG khác
    start_date=days_ago(1),
    catchup=False,
) as dag:
    crawl_news_task = PythonOperator(
        task_id='crawl_news',
        python_callable=crawl_news,
    )

    crawl_ohlcs_task = PythonOperator(
        task_id='crawl_ohlcs',
        python_callable=crawl_ohlcs,
    )

    load_api_to_parquet_task = PythonOperator(
        task_id='load_api_to_parquet',
        python_callable=load_api_to_parquet,
    )

    load_db_to_parquet_task = PythonOperator(
        task_id='load_db_to_parquet',
        python_callable=load_db_to_parquet,
    )

    load_parquet_to_hdfs_task = PythonOperator(
        task_id='load_parquet_to_hdfs',
        python_callable=run_shell_script
    )

    process_companies_task = PythonOperator(
        task_id='process_companies',
        python_callable=transform_to_datawarehouse_1,
    )
    
    process_ohlcs_task = PythonOperator(
        task_id='process_ohlcs',
        python_callable=transform_to_datawarehouse_2,
    )
    
    process_news_task = PythonOperator(
        task_id='process_news',
        python_callable=transform_to_datawarehouse_3,
    )

    # Định nghĩa thứ tự chạy các task
    crawl_news_task >> load_api_to_parquet_task
    crawl_ohlcs_task >> load_api_to_parquet_task
    crawl_news_task >> load_db_to_parquet_task
    crawl_ohlcs_task >> load_db_to_parquet_task
    [load_api_to_parquet_task, load_db_to_parquet_task] >> load_parquet_to_hdfs_task
    load_parquet_to_hdfs_task >> process_companies_task >> process_ohlcs_task >> process_news_task