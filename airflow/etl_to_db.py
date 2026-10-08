from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta,datetime
import sys

# Import các hàm từ các tập lệnh của bạn
sys.path.append('/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/src/extract')
sys.path.append('/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/src/transform')
sys.path.append('/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/src/load')
from crawl_extract_companies import crawl_extract_companies
from crawl_index import crawl_index
from crawl_industries import crawl_industries

from transform_to_database_1 import transform_to_database_1
from transform_to_database_2 import transform_to_database_2
from transform_to_database_3 import transform_to_database_3

from load_json_to_db_1 import load_json_to_db_1
from load_json_to_db_2 import load_json_to_db_2
from load_json_to_db_3 import load_json_to_db_3

# Định nghĩa các tham số mặc định của DAG
default_args = {
    'owner': 'thinh_DE',
    'depends_on_past': False,
    'email': ['nguyenthinhkma2002@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Khởi tạo DAG
with DAG(
    dag_id='ETL_to_Database',
    default_args=default_args,
    description='ETL DAG',
    schedule='1 0 * * 0',
    start_date=datetime(2026,10,9),
    catchup=False,
) as dag:

    # Task 1: Crawl companies
    crawl_companies_task = PythonOperator(
        task_id='crawl_companies',
        python_callable=crawl_extract_companies,
    )

    # Task 2: Crawl index
    crawl_index_task = PythonOperator(
        task_id='crawl_index',
        python_callable=crawl_index
    )

    # Task 3: Crawl industries
    crawl_industries_task = PythonOperator(
        task_id='crawl_industries',
        python_callable=crawl_industries,
    )

    # Task 4: Transform to database 1
    transform_to_database_1_task = PythonOperator(
        task_id='transform_to_database_1',
        python_callable=transform_to_database_1,
    )

    # Task 5: Load JSON to DB 1
    load_json_to_db_1_task = PythonOperator(
        task_id='load_json_to_db_1',
        python_callable=load_json_to_db_1,
    )

    # Task 6: Transform to database 2
    transform_to_database_2_task = PythonOperator(
        task_id='transform_to_database_2',
        python_callable=transform_to_database_2,
    )

    # Task 7: Load JSON to DB 2
    load_json_to_db_2_task = PythonOperator(
        task_id='load_json_to_db_2',
        python_callable=load_json_to_db_2,
    )

    # Task 8: Transform to database 3
    transform_to_database_3_task = PythonOperator(
        task_id='transform_to_database_3',
        python_callable=transform_to_database_3,
    )

    # Task 9: Load JSON to DB 3
    load_json_to_db_3_task = PythonOperator(
        task_id='load_json_to_db_3',
        python_callable=load_json_to_db_3,
    )
    
    # Task 10: task điều kiện
    trigger_etl_dag = TriggerDagRunOperator(
        task_id="trigger_elt_dag",
        trigger_dag_id='ELT_to_Data_Warehouse',
    )

    # Định nghĩa thứ tự chạy các task
    [crawl_companies_task,crawl_index_task,crawl_industries_task] >> transform_to_database_1_task \
        >> load_json_to_db_1_task >> transform_to_database_2_task \
        >> load_json_to_db_2_task >> transform_to_database_3_task \
        >> load_json_to_db_3_task >> trigger_etl_dag
    