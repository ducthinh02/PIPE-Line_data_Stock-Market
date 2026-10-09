import duckdb
import os

path_duckdb = '/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb'

if os.path.exists(path_duckdb):
    os.remove(path_duckdb)
    
conn = duckdb.connect(database=path_duckdb)

path_sql = '/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/src/extract/extract_db_to_parquet.sql'

with open(path_sql,'r', encoding='utf-8') as db:
    sql_script = db.read()
    
conn.execute(sql_script)

conn.close()