import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit, col, regexp_replace, to_date
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
from transformers import pipeline


def get_late_parquet_file(hdfs_directory):
    spark = SparkSession.builder\
            .appName("Get late parquet file")\
                .getOrCreate()
    
    # lấy metadata của parquet file 
    file_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet")
    
    file_df = file_df.withColumn("file_name" , input_file_name())
    
    latest_file = file_df.orderBy(
        "modificationTime", ascending = False).limit(1).collect()[0].file_name
    
    spark.stop()
    
    return latest_file

classifier = pipeline(
    "text-classification",
    model="FiinGroup/phobert-finetuned",
    tokenizer="FiinGroup/phobert-finetuned"
)


LABEL_MAP = {
    "LABEL_0": "negative",
    "LABEL_1": "neutral",
    "LABEL_2": "positive"
}


# ============================================================
# CHUNK TEXT
# ============================================================

def split_text_into_chunks(text, chunk_size=240):

    tokenizer = classifier.tokenizer

    token_ids = tokenizer(
        text,
        add_special_tokens=False
    )["input_ids"]

    chunks = []

    for i in range(0, len(token_ids), chunk_size):

        chunk_ids = token_ids[i:i + chunk_size]

        chunk_text = tokenizer.decode(
            chunk_ids,
            skip_special_tokens=True
        )

        if chunk_text.strip():
            chunks.append(chunk_text)

    return chunks


# ============================================================
# SENTIMENT ONE ARTICLE
# ============================================================

def analyze_article_sentiment(text):

    chunks = split_text_into_chunks(text)

    if not chunks:
        return None

    total_scores = {
        "negative": 0.0,
        "neutral": 0.0,
        "positive": 0.0
    }

    total_tokens = 0

    for chunk in chunks:

        results = classifier(
            chunk,
            top_k=None
        )

        # Lấy số token của chunk
        token_count = len(
            classifier.tokenizer(
                chunk,
                add_special_tokens=False
            )["input_ids"]
        )

        total_tokens += token_count

        for result in results:

            sentiment = LABEL_MAP[result["label"]]

            score = result["score"]

            total_scores[sentiment] += (
                score * token_count
            )

    # ============================================
    # Weighted average
    # ============================================

    if total_tokens == 0:
        return None

    negative_score = (
        total_scores["negative"] / total_tokens
    )

    neutral_score = (
        total_scores["neutral"] / total_tokens
    )

    positive_score = (
        total_scores["positive"] / total_tokens
    )

    scores = {
        "negative": negative_score,
        "neutral": neutral_score,
        "positive": positive_score
    }

    article_sentiment = max(
        scores,
        key=scores.get
    )

    sentiment_score = scores[
        article_sentiment
    ]

    return {
        "sentiment": article_sentiment,
        "sentiment_score": round(
            sentiment_score,
            6
        ),
        "negative_score": round(
            negative_score,
            6
        ),
        "neutral_score": round(
            neutral_score,
            6
        ),
        "positive_score": round(
            positive_score,
            6
        ),
        "chunk_count": len(chunks)
    }

def process_news_dim_news (parquet_file_path):
    
    spark = SparkSession.builder\
        .appName("Insert parquet into (dim_news, fact_news_company)")\
            .config("spark.sql.caseSensitive", "true")\
            .getOrCreate()
    
    dfspark_news = spark.read.parquet(parquet_file_path)
    
    if dfspark_news.isEmpty():
        raise ValueError("Error!")
    
    
    dfspark_news.printSchema()
    dfspark_news.show()


    df_news_sentiment = dfspark_news.select(
            col("article_id"),
            col("title"),
            col("full_content")
        )
    
    df_news_sentiment = df_news_sentiment.filter(
    col("full_content").isNotNull() &
    (col("full_content") != "")
    )   
    
    print(f"Articles available for sentiment: "
        f"{df_news_sentiment.count()}")
    
    
    # chuyển dataframe spark thành python list
    rows = df_news_sentiment.collect()
    
    sentiment_data = []
    total = len(rows)
    
    for index, row in enumerate(rows, start=1):
        
        article_id = row["article_id"]
        title = row["title"]
        full_content = row["full_content"]
        
        print(
            f"Processing {index}/{total}:{title}"
        )
        
        if full_content.strip() in  [
            "ONLY AVAILABLE IN PAID PLANS",
            "ONLY AVAILABLE IN PAID PLAN"
        ]:
            print( "Skip: NewsData placeholder")
            
            continue
            
        # phân tích sentiment
        
        sentiment_result = analyze_article_sentiment(full_content)
        
        if sentiment_result is None:
            print(
                " Skip: sentiment analysis failed"
            )
            continue
        
        process_article = {
            "article_id": article_id,

            "title": title,

            "sentiment":
                sentiment_result["sentiment"],

            "sentiment_score":
                sentiment_result["sentiment_score"],

        }
        sentiment_data.append(
            process_article
        )
    
    df_sentiment_data = spark.createDataFrame(
        sentiment_data
        )
    
    df_sentiment_data.show(
        truncate=False
    )
    
    # Connect to DuckDB
    database_path = 'D:/ETL-pipeline data analytics securities/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)
    
    # Tạo dim_time
    
    # # lấy giá trị ngày hôm qua
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")
    
    conn.execute(
        f'''
            INSERT INTO dim_time(date, day_of_week, month, quarter, year)
            SELECT
                '{yesterday}',
                '{yesterday.strftime("%A")}',
                '{yesterday.strftime("%B")}',
                '{((yesterday.month -1 ) // 3) + 1}',
                '{yesterday.year}'
                
            WHERE NOT EXISTS(
                SELECT 1 FROM dim_time 
                WHERE date = '{yesterday}'
            )
        '''
    )
    
    df_dim_time_id = conn.execute(
        f'''
            SELECT time_id FROM dim_time
            WHERE date = '{yesterday}'
        '''
    ).fetchdf()
    
    
    time_id = df_dim_time_id["time_id"][0]

    df_news = dfspark_news
    
    df_dim_news = df_news.filter(
                col("full_content").isNotNull() 
            )
        
    df_dim_news.show()
        
    df_dim_news = df_dim_news.select(
        col("article_id"),
        col("title"),
        col("link").alias("news_url"),
        col("source_name"),
        col("category"),
        col("datatype"),
        col("pub_date").alias("time_stamp")
        ).withColumn("news_time_id", lit(time_id))
    
    df_dim_news_final = df_dim_news.join(
        df_sentiment_data.select(
            col("article_id"),
            col("sentiment").alias("news_overall_sentiment_label"),
            col("sentiment_score").alias("news_overall_sentiment_score")
        ),
        on = "article_id",
        how= "left"
    )
    df_dim_news_final.show()
    
    arrow_table_dim_news = pa.Table.from_Pandas( df_dim_news_final.toPandas())
        
    conn.register("arrow_table_dim_news", arrow_table_dim_news)
        
    conn.execute(
            """
                INSERT INTO dim_news(
                    news_time_id,
                    news_article_id ,
                    news_title ,
                    news_url ,
                    news_source_name,
                    news_category ,
                    news_datatype ,
                    news_overall_sentiment_score,
                    news_overall_sentiment_score,
                    time_stamp
            )
            SELECT
                news_time_id,
                article_id,
                title,
                news_url,
                source_name,
                category,
                datatype,
                news_overall_sentiment_score,
                news_overall_sentiment_score,
                time_stamp
                
            FROM arrow_table_dim_news
            WHERE article_id NOT IN (
                SELECT article_id
                FROM dim_news
            )
            """
        )
    
    conn.close()
    
    spark.stop()
    
def transform_to_datawarehouse_4():
    
    hdfs_path = ''
    
    latest_file = get_late_parquet_file(hdfs_directory=hdfs_path)
    
    process_news_dim_news(latest_file)