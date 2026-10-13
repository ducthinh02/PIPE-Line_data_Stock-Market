import duckdb

database_path = '/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb'

conn = duckdb.connect(database=database_path)

conn.execute("""
    ALTER TABLE dim_time
    ALTER COLUMN day_of_week SET DATA TYPE VARCHAR
""")

conn.execute("""
    ALTER TABLE dim_time
    ALTER COLUMN month SET DATA TYPE VARCHAR
""")

conn.close()

print("Đã đổi kiểu dữ liệu day_of_week và month sang VARCHAR.")

import duckdb

conn = duckdb.connect(
    '/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb',
    read_only=True
)

print(conn.execute("DESCRIBE dim_time").fetchdf())
conn.close()