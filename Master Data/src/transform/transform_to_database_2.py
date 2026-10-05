import pandas as pd
import numpy as np 
from sqlalchemy import create_engine
import os
import json
import datetime
from dotenv import load_dotenv

load_dotenv()

def get_latest_file_in_directory(directory, extension):
    file = [
        os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)
    ]
    if not file:
        return None
    
    latest_file = max(file, key=os.path.getmtime)
    
    return latest_file

def read_latest_file_in_directory(directory):
    
    extension = '.json'
    latest_file = get_latest_file_in_directory(directory,extension)
    
    if latest_file:
        with open(latest_file,'r') as files:
            data_json= json.load(files)
    else:
        data_json = []
    
    return data_json

def clean_dataframe(dataframe):
    return dataframe.replace(r'^\s*$', np.nan, regex=True).drop_duplicates().dropna()

def save_to_json(dataframe, filename):
    os.makedirs(os.path.dirname(filename),exist_ok= True)
    dataframe.to_json(filename,orient='records', lines= True)

def transform_to_database_2():
    
    index_raw  = read_latest_file_in_directory('D:/ETL-pipeline data analytics securities/Master Data/data/raw/index')
    date = datetime.date.today().strftime("%Y_%m_%d")
    
    index = clean_dataframe(
        pd.DataFrame([
            {
                "symbol": item["symbol"],
                "description": item["description"],
                "full_name": item["full_name"],
                "group": item["group"]
            }   
            for item in index_raw
        ])
    )
    
    username = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    database = os.getenv("POSTGRES_DB")
    
    connection_str = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    engine= create_engine(connection_str)
    
    query = """SELECT * FROM INDEX_GROUPS"""
    
    index_groups= pd.read_sql(query,engine)
    
    index = pd.merge(
        index,
        index_groups,
        left_on= "group",
        right_on= "group_name"
    )[
        ["symbol","description","full_name","group_id"]
    ]
    
    new_columns = {"symbol": "index_code",
                    "description": "index_description",
                    "full_name": "index_name",
                    "group_id": "index_group_id"}
    
    index.rename(columns=new_columns,inplace=True)
    
    index_path = (f"D:/ETL-pipeline data analytics securities/Master Data/data/processed/transformed_to_database_index/"
                    f"process_index_{date}.json")
    
    save_to_json(index,index_path)
    
# transform_to_database_2()