import os
import json
import pandas as pd 
from dotenv import load_dotenv
import pyarrow as pa 
import pyarrow.parquet as pp
import numpy as np 
load_dotenv()

def get_latest_file_in_directory(directory, extension):
    
    file = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    
    if not file:
        return None
    latest_file = max(file,key=os.path.getmtime)
    
    return latest_file

def read_latest_file_in_directory(directory):
    
    with open(directory,'r', encoding='utf-8') as file:
            data_json = json.load(file)
    
    return data_json

def convert_ohlcv_json_to_dataframe(data):
    
    rows = []
    
    for ticker,candles in data.items():
        for candle in candles:
            
            rows.append({
                "ticker": ticker,
                "open": candle.get("open", candle.get("Open")),
                "high": candle.get("high", candle.get("High")),
                "low": candle.get("low", candle.get("Low")),
                "close": candle.get("close", candle.get("Close")),
                "volume": candle.get("volume", candle.get("Volume")),
                "time": candle.get("time"),
                
            })
    
    df = pd.DataFrame(rows)
    
    return df

def save_json_to_parquet(data, output_filepath):
    # Convert JSON data to a pyarrow Table
    table = pa.Table.from_pandas(pd.DataFrame(data))
    
    # Save the pyarrow Table as a Parquet file
    pp.write_table(table, output_filepath)
    
    
def save_dataframe_to_parquet(df, output_filepath):

    table = pa.Table.from_pandas(
        df,
        preserve_index=False
    )

    pp.write_table(
        table,
        output_filepath
    )


def load_db_to_dl(input_directory,output_directory):
    
    extension = '.json'
    
    latest_file = get_latest_file_in_directory(input_directory,extension)
    
    if latest_file:
        data = read_latest_file_in_directory(latest_file)

        filename = os.path.basename(latest_file).replace('.json', ".parquet")
        output_filepath = os.path.join(output_directory, filename)
            
            # Save the JSON data as a Parquet file
        save_json_to_parquet(data, output_filepath)
        print(f"Saved Parquet file: {output_filepath}")
    else:
        print("No json")
        
def load_db_ohlcv_to_dl(input_directory, output_directory):

    extension = ".json"

    latest_file = get_latest_file_in_directory(
        input_directory,
        extension
    )

    if latest_file is None:
        print("No JSON file found.")
        return

    # Đọc JSON
    data = read_latest_file_in_directory(
        latest_file
    )

    # JSON → DataFrame
    df = convert_ohlcv_json_to_dataframe(
        data
    )

    print(df.info())
    print(df.head())

    # Giữ nguyên tên file JSON, đổi extension thành parquet
    filename = os.path.basename(
        latest_file
    ).replace(".json", ".parquet")

    output_filepath = os.path.join(
        output_directory,
        filename
    )

    # DataFrame → Parquet
    save_dataframe_to_parquet(
        df,
        output_filepath
    )

    print(
        f"Saved Parquet file: {output_filepath}"
    )
        
def load_api_to_parquet():
    
    # chuyển json sang dataframe sang parquet
    input_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/ohlcv_company'
    # Path to the directory to save the Parquet files
    output_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_api_ohlcv_company_to_dl'
    
    
    load_db_ohlcv_to_dl(input_directory, output_directory)

    # Convert News JSON files to Parquet
    # Path to the directory containing the JSON files
    input_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/news'
    # Path to the directory to save the Parquet files
    output_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_api_news_to_dl'
    load_db_to_dl(input_directory, output_directory)


    # Convert Retail JSON files to Parquet
    # Path to the directory containing the JSON files
    input_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/ohlcv_retail'
    # Path to the directory to save the Parquet files
    output_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_api_retail_to_dl'
    load_db_to_dl(input_directory, output_directory)
    
    
    # Convert Rate JSON files to Parquet
    # Path to the directory containing the JSON files
    input_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/ohlcv_rate'
    # Path to the directory to save the Parquet files
    output_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_api_rate_to_dl'
    load_db_to_dl(input_directory, output_directory)
    

    # chuyển json index ohlcv sang parquet
    input_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/index_ohlcv'
    # Path to the directory to save the Parquet files
    output_directory = r'/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_api_index_ohlcv_to_dl'
    load_db_ohlcv_to_dl(input_directory, output_directory)
# load_api_to_parquet()