from vnstock import Reference, Market
import pandas as pd
import time
from datetime import datetime, timezone, date, timedelta
import json
from pathlib import Path
from tenacity import RetryError

ref = Reference()
mkt = Market()

path = "/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/ohlcv_company"

def crawl_ohlcv_company():
    
    date_crawl = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    list_ticker = ref.equity.list()["symbol"].tolist()
    dict_equity = {
        
    }
    
    for ticker in list_ticker:
        try:
            data_ohlcv = mkt.equity(symbol=ticker).ohlcv(start=date_crawl,
                                            end=date_crawl,
                                            interval="1D")
            if data_ohlcv is None :
                dict_equity[ticker] = []
            else:
                dict_equity[ticker] = data_ohlcv.to_dict(orient= "records")
        except (ValueError,RetryError) as e:
            dict_equity[ticker] = []
            continue
        time.sleep(3)

    date_json = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    path_raw = f"{path}/crawl_ohlcv_{date_json}.json"

    with open(path_raw,"w", encoding="utf-8") as outfile:
        json.dump(dict_equity,
                    outfile,
                    ensure_ascii=False,
                    indent=2,
                    default= str)
    
# crawl_ohlcv_company()
    
