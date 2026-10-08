from vnstock import Reference
import pandas as pd
import time
from datetime import datetime, timezone
import json
from pathlib import Path


ref = Reference()

path = "/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/raw/index"

def crawl_index() -> dict:

    data = ref.index.list()

    list_symbols = data["symbol"].tolist()
    
    list_company = {}

    for symbol in list_symbols:
        success = False
        while not success:
            try:
                member = ref.index.members(symbol=symbol)
                
                if member is not None and not (hasattr(member, "empty") and member.empty):
                    if isinstance(member, pd.Series):
                        list_company[symbol] = [member.to_dict()]
                    elif isinstance(member, pd.DataFrame):
                        list_company[symbol] = [member.to_dict(orient="records")]
                                    
                else:
                    print(f"Mã {symbol} không có dữ liệu thành viên.")
                
                success = True  # Đánh dấu thành công để thoát vòng lặp while
                
            except Exception as e:
                # Nếu gặp lỗi không hỗ trợ nhóm hoặc lỗi dữ liệu, in ra và bỏ qua luôn mã này
                print(f"Bỏ qua mã {symbol} do không hỗ trợ hoặc lỗi: {e}")
                break  # Thoát khỏi while True, chuyển sang symbol tiếp theo ngay lập tức

        time.sleep(2)
    
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    raw_file = f"{path}/crawl_index_{date}.json"    
    
    data_json = {
        "total": len(data),
        "index": data.to_dict(orient="records"),
        "member": list_company
    }
    
    with open(
        raw_file,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            data_json,
            f,
            ensure_ascii=False,
            indent=2,
        )
    
# crawl_index()
