import sys
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
from urllib.parse import urlparse

from pyspark.sql.functions import col

def get_latest_parquet_file(hdfs_directory):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Get Latest Parquet File") \
        .getOrCreate()

    # lấy danh sách các file parquet dưới dạng metadata
    files_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet")

    # tạo cột file_name với dữ liệu là tên path
    files_df = files_df.withColumn("file_name", input_file_name())

    # sắp xếp file theo thứ tứ thời gian gần nhất và lấy file đứng đầu
    latest_file = files_df.orderBy(
        "modificationTime", ascending=False).limit(1).collect()[0].file_name

    print(latest_file)

    spark.stop()

    return latest_file

# def is_parquet_file_empty(hdfs_url, threshold_size=2000):
#     # Extract the HDFS path from the URL
#     hdfs_path = urlparse((hdfs_url)).path
    
#     # Create an HDFS client
#     hdfs = fs.HadoopFileSystem('0.0.0.0', port=9000, user='anhcu')
    
#     # Get the file status
#     file_info = hdfs.get_file_info(hdfs_path)
    
#     # Get the file size
#     file_size = file_info.size
    
#     # Check if the file size is greater than the threshold
#     return file_size > threshold_size


def process(parquet_file_path):

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_times, fact_candles)") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()  
    
    # Đọc parquet 
    df_spark = spark.read.parquet(parquet_file_path)
    
    if df_spark.isEmpty():
        raise ValueError("không có dữ liệu")
    
    
    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()

    df_spark_ohlcv = df_spark.filter(
        col("ticker").isNotNull() &
        (col("ticker") !="" )
    )

    df_spark_ohlcv = df_spark_ohlcv.filter(
            col("open").isNotNull() &
            col("high").isNotNull() &
            col("low").isNotNull() &
            col("close").isNotNull() &
            col("volume").isNotNull() &
            col("time").isNotNull()
        )
    
    df_spark_ohlcv.show()

    # lấy giá trị ngày hôm qua
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")
    
    # Connect to DuckDB
    database_path = 'D:/ETL-pipeline data analytics securities/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)
    
    conn.execute(
        f'''
            INSERT INTO dim_time(date, day_of_week, month, quarter, year)
            SELECT
                '{yesterday}',
                '{yesterday.strftime("%A")}',
                '{yesterday.strftime("%B")}',
                '{((yesterday.month -1 ) // 3) + 1}',
                '{yesterday.year}'
                
            WHERE NOT EXISTS(
                SELECT 1 FROM dim_time 
                WHERE date = '{yesterday}'
            )
        '''
    )
    df_dim_time_id = conn.execute(
        f'''
            SELECT time_id FROM dim_time
            WHERE date = '{yesterday}'
        '''
    ).fetchdf()
    
    
    if "ohlcv_index" in parquet_file_path:
        
        df_index_id = conn.execute(
            '''
            SELECT index_id, index_code
            FROM dim_index
            '''
        ).fetchdf()

        df_spark_index_id = spark.createDataFrame(
            df_index_id
        )
        
        df_spark_ohlcv= df_spark_ohlcv.join(
            df_spark_index_id,
            on='index_code',
            how='left'
        )
        
        df_spark_ohlcv = df_spark_ohlcv.filter(
            df_spark_ohlcv["index_code"].isNotNull()
        )
        
        candles_index_time_id = df_dim_time_id["time_id"][0]
        
        df_spark_ohlcv = df_spark_ohlcv.withColumn(
            "trande_time_id",
            lit(candles_index_time_id)
        )

        arrow_table_index_ohlcv = pa.Table.from_pandas(
            df_spark_ohlcv.toPandas()
        )
        
        conn.register(
            "arrow_table",
            arrow_table_index_ohlcv
        )
        
        conn.execute(
            '''
                INSERT INTO fct_candles_index(
                    candles_dim_index_id  ,
                    candles_index_volume,
                    candles_index_open  ,
                    candles_index_close ,
                    candles_index_high ,
                    candles_index_low  ,
                    candles_index_timestamp ,
                    trande_time_id ,
                )
                SELECT 
                    index_id,
                    volume,
                    open, 
                    close, 
                    high, 
                    low, 
                    time_stamp,
                    trande_time_id
                FROM arrow_table
            '''
            
        )
    elif "ohlcv_companies" in parquet_file_path:
        
        # Rename columns for clarity
        df_spark_ohlcv = df_spark_ohlcv.withColumnRenamed("ticker", "company_ticket") \
            .withColumnRenamed("time", "time_stamp") 
    
        df_company_id = conn.execute(
            '''SELECT company_id, ticker_company FROM dim_companies'''
        ).fetchdf()
        
        
        # tạo pandas dataframe của companies
        companies_df= df_company_id.drop_duplicates(
            subset=['ticker_company'],
            keep="last"
        )
        
        # tạo spark dataframe của companies
        
        spark_df_companies = spark.createDataFrame(companies_df)
        
        df_spark_ohlcv = df_spark_ohlcv.join(
            spark_df_companies,
            on='ticker_company',
            how='left'
        )
        
        df_spark_ohlcv = df_spark_ohlcv.filter(
            df_spark_ohlcv['company_id'].isNotNull()
        )
        
        candles_time_id = df_dim_time_id['time_id'][0]
        
        df_spark_ohlcv = df_spark_ohlcv.withColumn(
            'candles_time_id',
            lit(candles_time_id)
        )
        
        arrow_table_company_ohlcv = pa.Table.from_pandas(
            df_spark_ohlcv.toPandas()
        )
        
        conn.register(view_name= "arrow_table",python_object= arrow_table_company_ohlcv)
        
        query = '''
            INSERT INTO fact_candles (
                candle_company_id ,

                candle_volume ,

                candle_open ,

                candle_close,

                candle_high ,

                candle_low ,

                candle_time_stamp,

                candle_time_id 
            )
            SELECT 
                company_id,
                volume,
                open, 
                close, 
                high, 
                low, 
                time_stamp,
                candles_time_id
            FROM arrow_table
        '''
        
        conn.execute(query=query)
    
def transform_to_datawarehouse_2():
    # tạo fact_candles:
    
    company_ohlcv_file_path = '/user/ubuntu/datalake/ohlcv/ohlcv_companies'
    company_parquet_file_path = get_latest_parquet_file(company_ohlcv_file_path)
    process(company_parquet_file_path)
    
    # tạo fact_index_candles
    index_ohlcv_file_path = '/user/ubuntu/datalake/ohlcv/ohlcv_index'
    index_parquet_file_path = get_latest_parquet_file(index_ohlcv_file_path)
    process(parquet_file_path=index_parquet_file_path)
