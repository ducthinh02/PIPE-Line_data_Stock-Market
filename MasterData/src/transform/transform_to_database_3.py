import pandas as pd
import numpy as np
import json
import os
from dotenv import load_dotenv
import datetime
from sqlalchemy import create_engine, URL

load_dotenv()

def get_latest_file_in_directory(directory, extension):
    
    file = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]

    if not file:
        return None
    
    latest_file= max(file,key=os.path.getmtime)
    return latest_file

def read_latest_file_in_directory(directory):
    
    extension = '.json'
    latest_file = get_latest_file_in_directory(directory,extension)
    
    if latest_file:
        with open(latest_file,'r') as file:
            data_json = json.load(file)
    else:
        data_json=[]
        
    return data_json

def clean_dataframe(dataframe):
    return dataframe.replace(r'^\s*$', np.nan, regex=True) .drop_duplicates().dropna()

def save_to_json(dataframe, filename):
    
    os.makedirs(os.path.dirname(filename),exist_ok=True)
    dataframe.to_json(filename,orient="records",lines=True)

def transform_to_database_3():
    company_raw  = read_latest_file_in_directory('/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/raw/company')
    company_data = company_raw.get("Company",[])
    date = datetime.date.today().strftime("%Y_%m_%d")
    
    industries_data = read_latest_file_in_directory('/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/raw/industries')
    
    industries_for_symbol = clean_dataframe(
        pd.DataFrame([
            {   
                "symbol":item["symbol"],
                "industry_name": item["icb_name"]
            }
            for item in industries_data
                    if item.get("icb_level") == 1
                ]).drop_duplicates()
        )
    
    
    company_join_exchange = clean_dataframe(
        pd.DataFrame([
            {
                "ticker_company": item["symbol"],
                "company_name" : item["company_name"],
                "founded_date": datetime.datetime.strptime(item["founded_date"],"%d/%m/%Y").date().isoformat(),
                "charter_capital": item["charter_capital"],
                "number_of_employees": item["number_of_employees"],
                "exchange": item["exchange"],
                "company_type": item["company_type"],
                "listing_date": datetime.datetime.strptime(item["listing_date"],"%d/%m/%Y").date().isoformat(),
                "listing_price": item["listing_price"],
                "listed_volume":item["listed_volume"],
                "outstanding_shares": item["outstanding_shares"],
                "ceo_name": item["ceo_name"]
            }   
            for item in company_data
        ])
        )
        
    db_url = URL.create(
            drivername = "postgresql+psycopg2",
            username = os.getenv("POSTGRES_USER"),
            password = os.getenv("POSTGRES_PASSWORD"),
            host = os.getenv("POSTGRES_HOST"),
            port = os.getenv("POSTGRES_PORT"),
            database = os.getenv("POSTGRES_DB"),
            
        )
        
    engine= create_engine(db_url)
        
    query_exchange = """SELECT * FROM EXCHANGE"""
    query_industries = """SELECT * FROM industries"""
    
    exchange= pd.read_sql(query_exchange,engine)
    industries = pd.read_sql(query_industries,engine)
    
    company_join_exchange = pd.merge(
        company_join_exchange,
        exchange,
        left_on= "exchange",
        right_on= "exchange_name"
    )[
        ["exchange_id","ticker_company","company_name","founded_date","charter_capital","number_of_employees",
        "company_type","listing_date","listing_price","listed_volume",
        "outstanding_shares","ceo_name"]
    ]
    
    industries_for_symbol = pd.merge(
        industries_for_symbol,
        industries,
        left_on= "industry_name",
        right_on= "industry_name"
    )[["symbol","industry_id"]]
    
    company_join_to_exchange_industries = pd.merge(
        company_join_exchange,
        industries_for_symbol,
        left_on= "ticker_company",
        right_on= "symbol"
    )[
        ["industry_id","exchange_id","ticker_company","company_name","founded_date","charter_capital","number_of_employees",
        "company_type","listing_date","listing_price","listed_volume",
        "outstanding_shares","ceo_name"]
    ]
                
    company_path = (f"/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/processed/transformed_to_database_company/"
                f"process_company_{date}.json")
        
    save_to_json(company_join_to_exchange_industries,company_path)
    
# transform_to_database_3()