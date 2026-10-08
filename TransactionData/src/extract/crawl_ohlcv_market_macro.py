from vnstock import Reference, Retail, Market
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import yfinance as yf

mkt = Market()
ret = Retail()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

path = "/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/ohlcv_retail"

path_rate = "/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/raw/ohlcv_rate"

def crawl_ohlcv_market_macro() -> dict:

    date_crawl = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")

    max_retries = 3
    for attemp in range(max_retries):
        try:
            data_gold = mkt.commodity("gold").ohlcv(start= date_crawl, 
                                        end= date_crawl,
                                        interval="1D")
            
        except Exception as e:
            if attemp < max_retries -1 :
                time.sleep(5)
                
            else:
                return None
            
    
    data_rate = ret.exchange_rate(date='')
    
    oil= yf.Ticker("BZ=F")
    
    today_data_oil = oil.history(period= "1d", interval="1D")
    today_data_oil = today_data_oil.reset_index()
    today_data_oil = today_data_oil.rename(
    columns={
        "Date": "time",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Dividends": "dividends",
        "Stock Splits": "stock_splits"
    }
)
    
    today_data_iron = yf.Ticker("TIO=F").history(period= "1d", interval="1D")
    today_data_iron = today_data_iron.reset_index()
    today_data_iron = today_data_iron.rename(
        columns={
            "Date": "time",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits"
        })
    
    forex = mkt.forex("USDVND").ohlcv(start= date_crawl, 
                                        end= date_crawl,
                                        interval="1D")

    data_marco = {    
        "gold": data_gold.to_dict(orient="records"),
        "oil": today_data_oil.to_dict(orient="records"),
        "steel": today_data_iron.to_dict(orient="records"),
        "forex": forex.to_dict(orient="records"),
    }
    
    data_marco_rate = {
        "rate": data_rate.to_dict(orient="records"),
    }
        
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    raw_file = f"{path}/crawl_market_macro_{date}.json"
    
    
    with open(
        raw_file,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            data_marco,
            f,
            ensure_ascii=False,
            indent=2,
            default=str
        )
    raw_file =f"{path_rate}/crawl_market_macro_rate_{date}.json"
        
    with open(
            raw_file,
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(
                data_marco_rate,
                f,
                ensure_ascii=False,
                indent=2,
                default=str
            )
            
crawl_ohlcv_market_macro()