from pyspark.sql.functions import to_timestamp
import pyarrow as pa 
import duckdb
from pyspark.sql import SparkSession


def get_latest_parquet_file(hdfs_directory):
    if not hdfs_directory.startswith("hdfs://"):
        hdfs_directory ="hdfs://localhost:9000" + f"{hdfs_directory}" 

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
    
    print(arrow_table.schema)
    
    # đường dẫn datawarehouse.duckdb
    database_path = '/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb'
    
    conn = duckdb.connect(database=database_path)
    
    conn.register("arrow_table",arrow_table)
    
    conn.execute(
        '''
            INSERT INTO dim_companies(
                industry_of_company, 
                company_name,
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
            company_name,
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
            ceo_name,
            FROM arrow_table
            ON CONFLICT (ticker_company)
            DO UPDATE SET 
                industry_of_company = EXCLUDED.industry_of_company,
                company_name = EXCLUDED.company_name,
                founded_date = EXCLUDED.founded_date,
                charter_capital = EXCLUDED.charter_capital,
                number_of_employees = EXCLUDED.number_of_employees,
                exchange_of_company = EXCLUDED.exchange_of_company,
                company_type = EXCLUDED.company_type,
                listing_date = EXCLUDED.listing_date,
                listing_price = EXCLUDED.listing_price,
                listed_volume = EXCLUDED.listed_volume,
                outstanding_shares = EXCLUDED.outstanding_shares,
                ceo_name = EXCLUDED.ceo_name;
        '''
        
    )
    conn.close()
    
    spark.stop()
    
def process_index(parquet_file_path):
    
    spark = (SparkSession.builder
            .appName("INSERT parquet INTO DUCKDB dim_index")
            .getOrCreate()
            )
    
    print("HDFS path:", parquet_file_path)
    
    df_spark = spark.read.parquet(parquet_file_path)
    
    # Hiển thị dữ liệu df spark
    print("Số dòng Spark đọc được:", df_spark.count())
    df_spark.printSchema()
    df_spark.show(10, truncate=False)
        
    pandas_dataframe = df_spark.toPandas()
    
    arrow_table = pa.Table.from_pandas(pandas_dataframe)
    
    print(arrow_table.schema)

    
    # đường dẫn datawarehouse.duckdb
    database_path = '/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb'
    
    conn = duckdb.connect(database=database_path)
    
    print("Database đang sử dụng:", database_path)

    print(
        "Số dòng trong Arrow:",
        arrow_table.num_rows
    )

    print(
        "Số dòng hiện có trong dim_index:",
        conn.execute(
            "SELECT COUNT(*) FROM dim_index"
        ).fetchone()[0]
    )
    
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
            FROM arrow_table
            ON CONFLICT(index_code)
            DO UPDATE SET
                index_name = EXCLUDED.index_name,
                index_description = EXCLUDED.index_description,
                group_name = EXCLUDED.group_name;
        '''
        
    )
    conn.close()
    
    spark.stop()
    
def transform_to_datawarehouse_1():

    company_file = get_latest_parquet_file(
        "/user/ubuntu/datalake/companies"
    )
    process_company(company_file)
    

    index_file = get_latest_parquet_file(
        "/user/ubuntu/datalake/index"
    )
    process_index(index_file)
    
    