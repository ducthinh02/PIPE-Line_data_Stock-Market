import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()


def get_latest_file_in_directory(directory, extension):
    
    files = [
        os.path.join(directory,f)
        for f in os.listdir(directory)
        if f.endswith(extension)
    ]
    if not files:
        return None
    
    latest_file = max(files, key= os.path.getmtime)
    
    return latest_file

def insert_data_from_json(file_path, table_name, columns, conflict_columns):
    
    with open(file_path,"r") as file:
        data = [json.loads(line) for line in file]
    
    if not data:
        print(f"No data found in {file_path}")
        return None
    
    placeholder = ','.join(['%s'] * len(columns))
    columns_str = ','.join(columns)
    conflict_columns_str = ','.join(conflict_columns)
    
    query = f"""INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholder})
            ON CONFLICT ({conflict_columns_str}) 
            DO NOTHING"""
    
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    ) 
    cur = conn.cursor()
    try:
        for record in data:
            values = [record[col] for col in columns]
            cur.execute(query, values)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Load failed: {e}")
        raise
    finally:    
        cur.close()
        conn.close()
    
def load_json_to_db_2():

    # Insert data into 'CK_INDEX' table
    insert_data_from_json(
        get_latest_file_in_directory(
            '/home/ubuntu/PIPE-Line_data_Stock-Market/MasterData/data/processed/transformed_to_database_index/', 
            '.json'
        ),
        'CK_INDEX',
        ['index_code', 'index_name','index_description','group_id'],
        ['index_code','index_name']
    )

    
# load_json_to_db_2()