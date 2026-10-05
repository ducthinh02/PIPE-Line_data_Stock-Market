import duckdb
import os

path_duckdb = 'D:/ETL-pipeline data analytics securities/datawarehouse.duckdb'

if os.path.exists(path_duckdb):
    os.remove(path_duckdb)
    
conn = duckdb.connect(database=path_duckdb)

path_sql = 'D:/ETL-pipeline data analytics securities/SQL/config DWH/datawarehouse.sql'

with open(path_sql,'r', encoding='utf-8') as db:
    sql_script = db.read()
    
conn.execute(sql_script)

conn.close()