from vnstock import Reference, Market
import pandas as pd
import time
from datetime import datetime, timezone, date, timedelta
import json
from pathlib import Path
from tenacity import RetryError

ref = Reference()
mkt = Market()

path = "/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/ohlcv_index"

def crawl_ohlcv_index():
    
    date_crawl = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    list_index = ["VNINDEX", "HNXINDEX", "UPCOMINDEX", "VN30", "HNX30", "VN100"]
    dict_index = {
        
    }
    
    for index in list_index:
        try:
            data_index_ohlcv = mkt.index(symbol=index).ohlcv(start=date_crawl,
                                            end=date_crawl,
                                            interval="1D")
            if data_index_ohlcv is None :
                dict_index[index] = []
            else:
                dict_index[index] = data_index_ohlcv.to_dict(orient= "records")
        except (ValueError,RetryError) as e:
            dict_index[index] = []
            continue
        time.sleep(10)

    date_json = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    path_raw = f"{path}/crawl_index_ohlcv_{date_json}.json"

    with open(path_raw,"w", encoding="utf-8") as outfile:
        json.dump(dict_index,
                    outfile,
                    ensure_ascii=False,
                    indent=2,
                    default= str)
    
crawl_ohlcv_index()
    
