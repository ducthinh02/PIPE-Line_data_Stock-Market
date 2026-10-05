import os
import json
import pandas as pd 
import numpy as np
import datetime

def get_latest_file_in_directory(directory, extension):
    
    files= [
        os.path.join(directory,f)
        for f in os.listdir(directory)
        if f.endswith(extension)]
    
    if not files:
        return None
    
    latest_file= max(files, key=os.path.getmtime)
    
    return latest_file

def read_latest_file_in_directory(directory):
    
    extension = ".json"
    
    latest_file = get_latest_file_in_directory(directory=directory, extension=extension)
    
    if latest_file:
        
        with open(latest_file, "r") as file:
            
            data_json = json.load(file)
        
    else:
        data_json= []

    return data_json

def clean_dataframe(dataframe):
    
    return dataframe.replace(r"^\s*$",np.nan, regex=True).drop_duplicates().dropna()

def save_to_json(dataframe, filename):
    os.makedirs(
        os.path.dirname(filename),
        exist_ok=True
    )
    
    dataframe.to_json(
        filename,
        orient="records",
        lines=True
    )
    
    print(f"Saved dataframe to {filename}")
    
def transform_to_database_1():
    
    market = read_latest_file_in_directory('D:/ETL-pipeline data analytics securities/Master Data/data/raw/company')
    industries = read_latest_file_in_directory('D:/ETL-pipeline data analytics securities/Master Data/data/raw/industries')
    market_group = read_latest_file_in_directory('D:/ETL-pipeline data analytics securities/Master Data/data/raw/index')

    date = datetime.date.today().strftime("%Y_%m_%d")
    
    icb_industries = clean_dataframe(pd.DataFrame([
        {
            "icb_code": item["industry_code"],
            "industry_name": item["industry_name"]
        }
        for item in industries
    ]))
    industries_path=(f"D:/ETL-pipeline data analytics securities/Master Data/data/processed/transformed_to_database_industries/"
                    f"process_industries_{date}.json")
    save_to_json(icb_industries,industries_path)
    
    exchanges = clean_dataframe(pd.DataFrame([
            {
                "exchange_name": item["exchange"]
            }
            for item in market
        ]))
    exchanges_path=(f"D:/ETL-pipeline data analytics securities/Master Data/data/processed/transformed_to_database_exchanges/"
                    f"process_exchanges_{date}.json")
    save_to_json(exchanges,exchanges_path)
    
    index_groups = clean_dataframe(pd.DataFrame([
            {
                "group_name": item["group"]
            }
            for item in market_group
    ]))
    
    groups_path = (f"D:/ETL-pipeline data analytics securities/Master Data/data/processed/transformed_to_database_index_group/"
                    f"process_index_groups_{date}.json")
    
    save_to_json(index_groups,groups_path)
    
# transform_to_database_1()
