#!/bin/bash

# Variable to store the latest file names
latest_files=""

# load companies

local_directory=/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_db_companny_to_parquet
hdfs_directory=/user/ubuntu/datalake/companies

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi
#done

#load index
local_directory=/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_db_index_to_parquet
hdfs_directory=/user/ubuntu/datalake/index

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi
#done

# load ohlcv
local_directory=/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_api_ohlcv_company_to_dl
hdfs_directory=/user/ubuntu/datalake/ohlcv_companies

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi
# done

# load news
local_directory=/home/anhcu/Final_ETL_App/etl-app/elt/data/completed/load_api_news_to_dl
hdfs_directory=/user/ubuntu/datalake/news

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi

# load ohlcv index
local_directory=/home/anhcu/Final_ETL_App/etl-app/elt/data/completed/load_api_index_ohlcv_to_dl
hdfs_directory=/user/ubuntu/datalake/ohlcv/ohlcv_index

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi


# load market marco
local_directory=/home/anhcu/Final_ETL_App/etl-app/elt/data/completed/load_api_retail_to_dl
hdfs_directory=/user/ubuntu/datalake/ohlcv/ohlcv_retail

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi

# load rate
local_directory=/home/ubuntu/PIPE-Line_data_Stock-Market/TransactionData/data/completed/load_api_rate_to_dl
hdfs_directory=/user/ubuntu/datalake/ohlcv/ohlcv_rate

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi


# Print the latest file names (for Airflow XCom)
echo -e "$latest_files"

# bash /home/anhcu/Final_ETL_App/etl-app/elt/scripts/load/load_parquet_to_hdfs.sh