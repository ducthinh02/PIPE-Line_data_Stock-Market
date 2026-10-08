import sys
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit, col, regexp_replace, to_date
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


def process_retail(parquet_file_path):

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_retail)") \
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
            col("time").isNotNull()
        )
    
    df_spark_ohlcv.show()
    
    # Connect to DuckDB
    database_path = 'D:/ETL-pipeline data analytics securities/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)
    
    
    df_dim_retail = df_spark.select(col("ticker").alias("ticker")).distinct()
    
    arrow_table_dim_retail = pa.Table.from_Pandas( df_dim_retail.toPandas())
    
    conn.register("arrow_table_dim_retail", arrow_table_dim_retail)
    
    conn.execute(
        """
            INSERT INTO dim_retail(retail_name)
            
            SELECT ticker FROM arrow_table_dim_retail
            WHERE ticker NOT IN (SELECT retail_name FROM dim_retail)
        """
    )
    
    
    # Tạo dim_time
    
    # # lấy giá trị ngày hôm qua
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")
    
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
    
        
    # Rename columns for clarity
    df_spark_ohlcv = df_spark_ohlcv.withColumnRenamed("ticker", "retail_name") \
        .withColumnRenamed("time", "time_stamp") 
    
    df_retail_id = conn.execute(
        '''SELECT retail_id , retail_name  FROM dim_retail'''
    ).fetchdf()
        
        
    # tạo pandas dataframe của companies
    retail_id_df= df_retail_id.drop_duplicates(
            subset=['retail_name'],
            keep="last"
        )
        
        # tạo spark dataframe của companies
        
    spark_df_retail = spark.createDataFrame(retail_id_df)
        
    df_spark_ohlcv = df_spark_ohlcv.join(
            spark_df_retail,
            on='retail_name',
            how='left'
        )
        
    df_spark_ohlcv = df_spark_ohlcv.filter(
            df_spark_ohlcv['retail_id'].isNotNull()
        )
        
    time_id = df_dim_time_id['time_id'][0]
        
    df_spark_ohlcv = df_spark_ohlcv.withColumn(
            'time_id',
            lit(time_id)
        )
        
    arrow_table_retail_ohlcv = pa.Table.from_pandas(
            df_spark_ohlcv.toPandas()
        )
        
    conn.register(view_name= "arrow_table",python_object= arrow_table_retail_ohlcv)
        
    query = '''
            INSERT INTO fact_retail (
                retail_id ,
                time_id ,

                open ,
                high ,
                low ,
                close ,
                volume ,
                time_stamp
            )
            SELECT 
                retail_id ,
                time_id ,
                open, 
                high, 
                low,
                close, 
                volume,
                time_stamp
                
            FROM arrow_table
        '''
        
    conn.execute(query=query)
    conn.close()
        
    spark.stop()

def process_rate(parquet_file_path):
    

    spark = SparkSession.builder \
        .appName("Insert Rate into DuckDB") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()

    # =========================
    # Đọc Parquet
    # =========================

    df_spark = spark.read.parquet(
        parquet_file_path
    )

    if df_spark.isEmpty():
        raise ValueError("không có dữ liệu")

    df_spark.printSchema()
    df_spark.show()

    # =========================
    # Cleaning
    # =========================

    df_spark_rate = df_spark.filter(
        col("currency_code").isNotNull() &
        (col("currency_code") != "") &
        col("currency_name").isNotNull() &
        (col("date").isNotNull())
    )

    # =========================
    # Chuyển dữ liệu giá thành số
    # =========================

    df_spark_rate = df_spark_rate.withColumn(
        "buy_cash",
        regexp_replace(
            col("buy_cash"),
            ",",
            ""
        ).cast("double")
    )

    df_spark_rate = df_spark_rate.withColumn(
        "buy_transfer",
        regexp_replace(
            col("buy_transfer"),
            ",",
            ""
        ).cast("double")
    )

    df_spark_rate = df_spark_rate.withColumn(
        "sell",
        regexp_replace(
            col("sell"),
            ",",
            ""
        ).cast("double")
    )

    # Chuyển date từ string → date
    df_spark_rate = df_spark_rate.withColumn(
        "date",
        to_date(col("date"))
    )

    df_spark_rate.show()

    # =========================
    # Connect DuckDB
    # =========================

    database_path = (
        "D:/ETL-pipeline data analytics securities/"
        "datawarehouse.duckdb"
    )

    conn = duckdb.connect(
        database=database_path
    )

    # =========================
    # dim_currency
    # =========================

    df_dim_currency = df_spark_rate.select(
        col("currency_code"),
        col("currency_name")
    ).distinct()

    arrow_table_dim_currency = pa.Table.from_pandas(
        df_dim_currency.toPandas()
    )

    conn.register(
        "arrow_table_dim_currency",
        arrow_table_dim_currency
    )

    conn.execute(
        """
        INSERT INTO dim_currency (
            currency_code,
            currency_name
        )

        SELECT
            currency_code,
            currency_name

        FROM arrow_table_dim_currency

        WHERE currency_code NOT IN (
            SELECT currency_code
            FROM dim_currency
        )
        """
    )

    # =========================
    # Lấy currency_id
    # =========================

    df_currency_id = conn.execute(
        """
        SELECT
            currency_id,
            currency_code
        FROM dim_currency
        """
    ).fetchdf()

    # =========================
    # Spark DataFrame currency
    # =========================

    spark_df_currency = spark.createDataFrame(
        df_currency_id
    )

    # =========================
    # JOIN currency_id
    # =========================

    df_spark_rate = df_spark_rate.join(
        spark_df_currency,
        on="currency_code",
        how="left"
    )

    # Chỉ giữ những currency map được
    df_spark_rate = df_spark_rate.filter(
        col("currency_id").isNotNull()
    )

    # =========================
    # dim_time
    # =========================

    df_rate_dates = df_spark_rate.select(
        col("date")
    ).distinct()

    rate_dates = [
        row["date"]
        for row in df_rate_dates.collect()
    ]

    for rate_date in rate_dates:

        conn.execute(
            f"""
            INSERT INTO dim_time (
                date,
                day_of_week,
                month,
                quarter,
                year
            )

            SELECT
                '{rate_date}',
                '{rate_date.strftime("%A")}',
                '{rate_date.strftime("%B")}',
                '{((rate_date.month - 1) // 3) + 1}',
                '{rate_date.year}'

            WHERE NOT EXISTS (
                SELECT 1
                FROM dim_time
                WHERE date = '{rate_date}'
            )
            """
        )

    # =========================
    # Lấy time_id
    # =========================

    df_time_id = conn.execute(
        """
        SELECT
            time_id,
            date
        FROM dim_time
        """
    ).fetchdf()

    spark_df_time = spark.createDataFrame(
        df_time_id
    )

    # =========================
    # JOIN time_id
    # =========================

    df_spark_rate = df_spark_rate.join(
        spark_df_time,
        on="date",
        how="left"
    )

    # =========================
    # Kiểm tra time_id
    # =========================

    df_spark_rate = df_spark_rate.filter(
        col("time_id").isNotNull()
    )

    df_spark_rate.show()

    # =========================
    # Spark → Pandas → Arrow
    # =========================

    arrow_table_rate = pa.Table.from_pandas(
        df_spark_rate.toPandas()
    )

    conn.register(
        "arrow_table_rate",
        arrow_table_rate
    )

    # =========================
    # fact_rate
    # =========================

    query = """
        INSERT INTO fact_rate (
            currency_id,
            time_id,
            buy_cash,
            buy_transfer,
            sell
        )

        SELECT
            currency_id,
            time_id,
            buy_cash,
            buy_transfer,
            sell

        FROM arrow_table_rate
    """

    conn.execute(query)

    conn.close()

    spark.stop()
    
def transform_to_datawarehouse_3():
    # tạo fact_candles:
    
    retail_ohlcv_file_path = '/datalake/ohlcv/ohlcv_retail'
    retail_parquet_file_path = get_latest_parquet_file(retail_ohlcv_file_path)
    process_retail(retail_parquet_file_path)
    
    rate_ohlcv_file_path = '/user/ubuntu/datalake/ohlcv/ohlcv_rate'
    rate_parquet_file_path = get_latest_parquet_file(rate_ohlcv_file_path)
    process_rate(rate_parquet_file_path)
    
