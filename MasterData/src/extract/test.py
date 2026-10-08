import json
import pandas as pd
from vnstock import Reference, Retail

ref = Reference()
ret = Retail()
# 1. Lấy dữ liệu (trả về pandas DataFrame)
data =  ref.equity().list_by_industry()
news = ref.company('F88').news()

# 2. Chuyển DataFrame thành cấu trúc dict phù hợp để ghi JSON
data_dict = {
    "industries": data.to_dict(orient="records"),
    "news": news.to_dict(orient="records")
}

# 3. Sửa lại tham số trong open("tên_file.json", "w")
with open("rate.json", "w", encoding="utf-8") as f:
  json.dump(
      data_dict,
      f,
      ensure_ascii=False,
      indent=2,
  )