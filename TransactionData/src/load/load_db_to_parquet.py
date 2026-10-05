from sqlalchemy import create_engine
import pandas as pd 
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

def read_query_from_file(file_path):
    
    with open(file_path,'r') as file:
        sql = file.read()
    
    queries = [
        query.strip()
        for query in sql.split(";")
        if query.strip()
    ]

    return queries

def query_to_parquet(query,conn, path_query_file):
    
    df= pd.read_sql( query, conn)
    print(df.info())
    df.to_parquet(path_query_file, engine="pyarrow")
    
def load_db_to_parquet():
    
    user= os.getenv("POSTGRES_USER")
    password= os.getenv("POSTGRES_PASSWORD")
    host= os.getenv("POSTGRES_HOST")
    post= os.getenv("POSTGRES_PORT")
    database= os.getenv("POSTGRES_DB")
    
    conn = create_engine(f"postgresql://{user}:{password}@{host}:{post}/{database}")
    
    file_path = 'D:/ETL-pipeline data analytics securities/TransactionData/src/extract/extract_db_to_parquet.sql'
    
    date = datetime.date.today().strftime("%Y_%m_%d")
    query = read_query_from_file(file_path=file_path)
    
    company_output = 'TransactionData/data/completed/load_db_to_parquet/load_companny_to_parquet_'+f"{date}.parquet"
    
    query_to_parquet(query=query[0],conn=conn, 
                        path_query_file=company_output)
    
    index_output = 'TransactionData/data/completed/load_db_to_parquet/load_index_to_parquet_'+f"{date}.parquet"
    
    query_to_parquet(query=query[1],conn=conn, 
                    path_query_file=index_output)
    
load_db_to_parquet()
    