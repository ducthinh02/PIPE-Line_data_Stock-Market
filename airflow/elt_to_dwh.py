from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.append(
    "/home/ubuntu/PIPE-Line_data_Stock-Market/.venv/lib/python3.14/site-packages"
)
import subprocess
import os
from datetime import timedelta,datetime

sys.path.append('/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/src/extract')
sys.path.append('/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/src/load')
sys.path.append('/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/src/transform')
from crawl_ohlcv_company import crawl_ohlcv_company
from crawl_ohlcv_index import crawl_ohlcv_index
from crawl_ohlcv_market_macro import crawl_ohlcv_market_macro
from crawl_RSS_news import crawl_RSS_news

from load_api_to_parquet import load_api_to_parquet
from load_db_to_parquet import load_db_to_parquet

from transform_to_datawarehouse_1 import transform_to_datawarehouse_1
from transform_to_datawarehouse_2 import transform_to_datawarehouse_2
from transform_to_datawarehouse_3 import transform_to_datawarehouse_3
from transform_to_datawarehouse_4 import transform_to_datawarehouse_4
from transform_to_datawarehouse_5 import transform_to_datawarehouse_5

default_args = {
    'owner': 'thinh_DE',
    'depends_on_past': False,
    'email' : ['nguyenthinhkma2002@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ELT_to_Data_Warehouse',
    default_args=default_args,
    description='ETL DAG for Data Warehouse',
    schedule='1 1 * * 1-6',  # Chạy khi được kích hoạt bởi DAG khác
    start_date=datetime(2026,10,9),
    catchup=False,
) as dag:
    crawl_news_task = PythonOperator(
        task_id='crawl_news',
        python_callable=crawl_RSS_news,
    )

    crawl_ohlcv_companies_task = PythonOperator(
        task_id='crawl_ohlcv_companies',
        python_callable=crawl_ohlcv_company,
    )

    crawl_ohlcv_index_task = PythonOperator(
        task_id='crawl_ohlcv_index',
        python_callable=crawl_ohlcv_index,
    )
    
    crawl_ohlcv_market_macro_task = PythonOperator(
        task_id='crawl_ohlcv_market_macro',
        python_callable=crawl_ohlcv_market_macro,
    )
    
    load_api_to_parquet_task = PythonOperator(
        task_id='load_api_to_parquet',
        python_callable=load_api_to_parquet,
    )

    load_db_to_parquet_task = PythonOperator(
        task_id='load_db_to_parquet',
        python_callable=load_db_to_parquet,
    )

    load_parquet_to_hdfs_task = BashOperator(
        task_id='load_parquet_to_hdfs',
        bash_command="/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/src/load/load_parquet_to_hdfs.sh",
    )

    process_companies_vs_index_task = PythonOperator(
        task_id='process_dim_companies_vs_index',
        python_callable=transform_to_datawarehouse_1,
    )
    
    process_ohlcv_company_vs_index_task = PythonOperator(
        task_id='process_ohlcv_company_VS_index',
        python_callable=transform_to_datawarehouse_2,
    )
    
    process_ohlcv_retail_vs_rate_task = PythonOperator(
        task_id='process_ohlcv_retail_vs_rate',
        python_callable=transform_to_datawarehouse_3,
    )
    
    process_dim_news_task = PythonOperator(
        task_id='process_dim_news',
        python_callable=transform_to_datawarehouse_4,
    )
    
    process_fact_news_task_task = PythonOperator(
        task_id='process_fact_news',
        python_callable=transform_to_datawarehouse_5,
    )

    # Định nghĩa thứ tự chạy các task
    
    [   crawl_news_task,
        crawl_ohlcv_companies_task,
        crawl_ohlcv_market_macro_task,
        crawl_ohlcv_index_task 
    ] >> load_api_to_parquet_task
    
    [   crawl_news_task,
        crawl_ohlcv_companies_task,
        crawl_ohlcv_market_macro_task,
        crawl_ohlcv_index_task 
        ] >> load_db_to_parquet_task
    
    [load_api_to_parquet_task, load_db_to_parquet_task] >> load_parquet_to_hdfs_task\
        >> process_companies_vs_index_task \
        >> process_ohlcv_company_vs_index_task >> process_ohlcv_retail_vs_rate_task\
        >> process_dim_news_task >> process_fact_news_task_task
    