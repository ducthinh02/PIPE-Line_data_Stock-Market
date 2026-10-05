from vnstock import Reference, Market
import pandas as pd
import time
from datetime import datetime, timezone, date, timedelta
import json
from pathlib import Path

ref = Reference()

path = Path("D:/ETL-pipeline data analytics securities/Master Data/data/raw/industries")


def crawl_industries():
    data_industries = ref.industry.list()
    json_industries = data_industries.to_dict(orient= "records")
    symbol_industries = ref.equity().list_by_industry()
    data_json = symbol_industries.to_dict(orient= "records")
    
    crawl_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    path.mkdir(parents=True,exist_ok=True)
    
    path_raw = path/ f"crawl_symbol_industries_{crawl_date}.json"
    path_industries = path/ f"crawl_industries_{crawl_date}.json"
    with open(path_raw,"w",encoding= "utf-8") as f:
        json.dump(data_json,
                f,
                ensure_ascii=False,
                indent=2)
    
    with open(path_industries,"w",encoding="utf-8") as o:
        json.dump(json_industries,
                o,
                ensure_ascii=False,
                indent=2)
        
crawl_industries()