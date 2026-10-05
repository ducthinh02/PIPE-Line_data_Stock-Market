#!/bin/bash

# Variable to store the latest file names
latest_files=""

# load ohlcv
local_directory=/Final_ETL_App/etl-app/elt/data/completed/load_api_ohlcv_to_dl
hdfs_directory= chưa tạo

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
hdfs_directory=chưa tạo

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
hdfs_directory=chưa tạo

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
hdfs_directory=chưa tạo

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