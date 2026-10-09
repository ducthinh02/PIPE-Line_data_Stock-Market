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

# def inspect_data_type(name, data):
#     print(f"\n--- Kiểm tra {name} ---")
#     print("Kiểu dữ liệu:", type(data).__name__)

#     if isinstance(data, list):
#         print("Số phần tử:", len(data))

#         for i, item in enumerate(data[:5]):
#             print(f"Phần tử [{i}]: {type(item).__name__}")
#             if not isinstance(item, dict):
#                 print("Giá trị:", repr(item)[:200])
    
def transform_to_database_1():
    
    market_data = read_latest_file_in_directory('/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/raw/company')
    market = market_data.get("Company",[])
    industries= read_latest_file_in_directory('/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/raw/industries')
    market_group_data = read_latest_file_in_directory('/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/raw/index')
    market_group = market_group_data.get("index",[])
    
    # inspect_data_type("market", market)
    # inspect_data_type("industries", industries)
    # inspect_data_type("market_group", market_group)
    
    date = datetime.date.today().strftime("%Y_%m_%d")
    
    icb_industries = clean_dataframe(pd.DataFrame([
        {
            "icb_code": item["icb_code"],
            "industry_name": item["icb_name"]
        }
        for item in industries
        if item.get("icb_level") == 1
    ]).drop_duplicates())
    
    industries_path=(f"/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/processed/transformed_to_database_industries/"
                    f"process_industries_{date}.json")
    save_to_json(icb_industries,industries_path)
    
    exchanges = clean_dataframe(pd.DataFrame([
            {
                "exchange_name": item["exchange"]
            }
            for item in market
        ]))
    exchanges_path=(f"/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/processed/transformed_to_database_exchanges/"
                    f"process_exchanges_{date}.json")
    save_to_json(exchanges,exchanges_path)
    
    index_groups = clean_dataframe(pd.DataFrame([
            {
                "group_name": item["group"]
            }
            for item in market_group
    ]))
    
    groups_path = (f"/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/processed/transformed_to_database_index_group/"
                    f"process_index_groups_{date}.json")
    
    save_to_json(index_groups,groups_path)
    
# transform_to_database_1()
