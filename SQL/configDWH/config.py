import duckdb
import os

database_path = '/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb'

if os.path.exists(database_path):
    os.remove(database_path)
    
conn = duckdb.connect(database=database_path)

path_sql = '/home/ubuntu/PIPE-Line_data_Stock-Market/SQL/configDWH/datawarehouse.sql'

with open(path_sql,'r', encoding='utf-8') as db:
    sql_script = db.read()
    
conn.execute(sql_script)

conn.close()