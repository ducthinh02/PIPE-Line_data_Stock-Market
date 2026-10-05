from pyspark.sql.functions import to_timestamp
import pyarrow as pa 
import duckdb
from pyspark.sql import SparkSession


def get_latest_parquet_file(hdfs_directory):
    
    spark = SparkSession(
        SparkSession.builder
        .appName("get latest parquet")
        .getOrCreate()
    )
    
    # lấy danh sách các file parquet trong hdfs directory
    file_df = spark.read.format("binaryFile").load(
        hdfs_directory+ "/*.parquet"
    )
    
    latest_file = (file_df
                .orderBy("modificationTime",ascending=False)
                .limit(1)
                .collect()[0]
                .path 
                )
    
    print(latest_file)
    
    spark.stop()
    return latest_file

def process_company(parquet_file_path):
    
    spark = (SparkSession.builder
            .appName("INSERT parquet INTO DUCKDB dim_company")
            .getOrCreate()
            )
    
    df_spark = spark.read.parquet(parquet_file_path)
    
    # Hiển thị dữ liệu df spark
    df_spark.printSchema()
    
    df_spark.show()
    
    pandas_dataframe = df_spark.toPandas()
    
    arrow_table = pa.Table.from_pandas(pandas_dataframe)
    
    # đường dẫn datawarehouse.duckdb
    database_path = 'D:/ETL-pipeline data analytics securities/datawarehouse.duckdb'
    
    conn = duckdb.connect(database=database_path)
    
    conn.register("arrow_table",arrow_table)
    
    conn.execute(
        '''
            INSERT INTO dim_company(
                industry_of_company, 
                
                company_update_time_stamp ,
                
                ticker_company ,
                founded_date ,
                charter_capital,
                number_of_employees ,
                exchange_of_company ,
                company_type ,
                listing_date ,
                listing_price ,                            
                listed_volume ,                       
                outstanding_shares,
                ceo_name
            )
            SELECT
            industry_of_company, 

            company_update_time_stamp ,

            ticker_company ,
            founded_date ,
            charter_capital,
            number_of_employees ,
            exchange_of_company ,
            company_type ,
            listing_date ,
            listing_price ,                            
            listed_volume ,                       
            outstanding_shares,
            ceo_name
            FORM arrow_table
        '''
        
    )
    conn.close()
    
    spark.stop()
    
def process_index(parquet_file_path):
    
    spark = (SparkSession.builder
            .appName("INSERT parquet INTO DUCKDB dim_index")
            .getOrCreate()
            )
    
    df_spark = spark.read.parquet(parquet_file_path)
    
    # Hiển thị dữ liệu df spark
    df_spark.printSchema()
    
    df_spark.show()
    
    pandas_dataframe = df_spark.toPandas()
    
    arrow_table = pa.Table.from_pandas(pandas_dataframe)
    
    # đường dẫn datawarehouse.duckdb
    database_path = 'D:/ETL-pipeline data analytics securities/datawarehouse.duckdb'
    
    conn = duckdb.connect(database=database_path)
    
    conn.register("arrow_table",arrow_table)
    
    conn.execute(
        '''
            INSERT INTO dim_index(
                index_code,
                index_name,
                index_description,
                group_name
            )
            SELECT
            index_code,
            index_name,
            index_description,
            group_name
            FORM arrow_table
        '''
        
    )
    conn.close()
    
    spark.stop()
    
def transform_to_datawarehouse_1():

    company_file = get_latest_parquet_file(
        "/user/anhcu/datalake/companies/"
    )

    index_file = get_latest_parquet_file(
        "/user/anhcu/datalake/indexes/"
    )

    process_company(company_file)

    process_index(index_file)
    
    