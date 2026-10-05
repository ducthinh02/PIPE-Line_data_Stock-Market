from vnstock import Reference
import pandas as pd
import time
from datetime import datetime, timezone
import json
from pathlib import Path


ref = Reference()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = (PROJECT_ROOT
            /"data"
            /"raw"
            /"company"
            )


def extract_companies() -> dict:

    data = ref.equity.list()
    list_symbol = data["symbol"].tolist()

    list_company = []
    list_name_company = []
    
    data_name = ref.equity.list()
    
    list_name_company.extend(
                    data_name.to_dict(orient="records")
                        )
    print(type(list_name_company))
    print(list_name_company.keys() if isinstance(list_name_company, dict) else "LIST")
    
    
    print("BEFORE MAP")
    print("company_reference type:", type(list_name_company))
    print("first item:", repr(list_name_company[0]))
    print("first item type:", type(list_name_company[0]))

    company_reference_map = {
        item["symbol"]: item
        for item in list_name_company
    }
    
    for symbol_company in list_symbol:

        while True:
            try:
                print(f"Processing: {symbol_company}")

                data_company = ref.company(symbol_company).info()

                list_company.extend(
                    data_company.to_dict(orient="records")
                )

                break

            except Exception as e:
                print(f"Rate limit/error: {e}")
                print("Waiting 60 seconds...")
                time.sleep(60)

        time.sleep(2)
    # data_name = ref.equity.list()
    # list_name_company.extend(
    #                     data_name.to_dict(orient="records")
    #                 )
    # company_reference_map = {
    #     item["symbol"]: item
    #     for item in data_name
    # }
    
    detail_company_list = []
    
    for company in list_company:
        symbol = company.get("symbol")
        reference = company_reference_map.get(symbol)
        
        if reference:
            company["company_name"] = reference.get("organ_name")     
        
        detail_company_list.append(company)
    
    return {
        "Total": len(list_company),
        "Company": list_company,
    }
        

def save_raw_news(data):
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    raw_file = RAW_FILE/ f"crawl_companies_{date}.json"
    
    raw_file.parent.mkdir(parents=True, exist_ok= True)

    
    with open(
        raw_file,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
        )


def crawl_extract_companies():
    data= extract_companies()

    save_raw_news(data)
    
crawl_extract_companies()
