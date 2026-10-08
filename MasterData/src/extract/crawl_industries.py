from vnstock import Reference, Market
import pandas as pd
import time
from datetime import datetime, timezone, date, timedelta
import json
from pathlib import Path

ref = Reference()

path = "/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/raw/industries"

# path_test = "D:/ETL-pipeline data analytics securities/MasterData/data/raw/industries"

def crawl_industries():
    symbol_industries = ref.equity().list_by_industry()
    data_json = symbol_industries.to_dict(orient= "records")
    
    crawl_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # path_test.mkdir(parents=True,exist_ok=True)
    
    path_raw = f"{path}/crawl_symbol_industries_{crawl_date}.json"
    with open(path_raw,"w",encoding= "utf-8") as f:
        json.dump(data_json,
                f,
                ensure_ascii=False,
                indent=2)
        
# crawl_industries()