from vnstock import Reference, Market
import pandas as pd
import time
from datetime import datetime, timezone, date, timedelta
import json
from pathlib import Path
from tenacity import RetryError

ref = Reference()
mkt = Market()

# path = "/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/ohlcv_company"

path ="D:/ETL-pipeline data analytics securities/TransactionData/data/raw/ohlcv"

def crawl_ohlcv_company():
    
    date_crawl = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    print("[1] Bắt đầu lấy danh sách ticker", flush=True)

    list_ticker = ref.equity.list()["symbol"].tolist()
    print(f"[2] Lấy được {len(list_ticker)} ticker", flush=True)

    dict_equity = {
        
    }
    
    for i, ticker in enumerate(list_ticker, start=1):
        try:
            print(f"[3] Đang gọi OHLCV: {i}/{len(list_ticker)} - {ticker}", flush=True)

            data_ohlcv = mkt.equity(symbol=ticker).ohlcv(start=date_crawl,
                                            end=date_crawl,
                                            interval="1D")
            
            print(f"[4] Đã nhận kết quả: {ticker}", flush=True)
            
            if data_ohlcv is None :
                dict_equity[ticker] = []
            else:
                dict_equity[ticker] = data_ohlcv.to_dict(orient= "records")
        except (ValueError,RetryError) as e:
            print(f"[ERROR] {ticker}: {type(e).__name__}: {e}", flush=True)

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
    
crawl_ohlcv_company()
    
