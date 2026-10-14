import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name,lower, trim, lit, col, regexp_replace, to_date
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
import json
from datetime import datetime
import re
import pandas as pd 
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transformers import pipeline
import itertools

def get_late_parquet_file(hdfs_directory):
    if not hdfs_directory.startswith("hdfs://"):
            hdfs_directory ="hdfs://localhost:9000" + f"{hdfs_directory}" 
    
    spark = SparkSession.builder\
            .appName("Get late parquet file")\
                .getOrCreate()
    
    # lấy metadata của parquet file 
    files_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet")
    
    # sắp xếp file theo thứ tứ thời gian gần nhất và lấy file đứng đầu
    latest_file = (
            files_df
            .orderBy("modificationTime", ascending=False)
            .limit(1)
            .collect()[0]["path"]
            )
    
    spark.stop()
    
    return latest_file


# ============================================================
# NER MODEL
# ============================================================

BATCH_SIZE = 10

LABEL_MAP = {
    "LABEL_0": "negative",
    "LABEL_1": "neutral",
    "LABEL_2": "positive"
}

# Hàm lọc keyword liên quan đến tên công ty
def generate_company_aliases(company_name):

    aliases = set()

    if not company_name:
        return aliases

    name = company_name.strip()

    # --------------------------------------------------------
    # Tên gốc
    # --------------------------------------------------------

    aliases.add(name)

    # --------------------------------------------------------
    # Bỏ "CTCP"
    #
    # CTCP Tập đoàn Hòa Phát
    # -> Tập đoàn Hòa Phát
    # --------------------------------------------------------

    alias = re.sub(
        r"^CTCP\s+",
        "",
        name,
        flags=re.IGNORECASE
    ).strip()

    aliases.add(alias)

    # --------------------------------------------------------
    # Bỏ "Công ty cổ phần"
    # --------------------------------------------------------

    alias = re.sub(
        r"^Công ty cổ phần\s+",
        "",
        name,
        flags=re.IGNORECASE
    ).strip()

    aliases.add(alias)

    # --------------------------------------------------------
    # Bỏ "Công ty CP"
    # --------------------------------------------------------

    alias = re.sub(
        r"^Công ty CP\s+",
        "",
        name,
        flags=re.IGNORECASE
    ).strip()

    aliases.add(alias)

    alias = re.sub(
        r"^Tập đoàn\s+",
        "",
        alias,
        flags=re.IGNORECASE
    ).strip()

    aliases.add(alias)

    # --------------------------------------------------------
    # Normalize
    # --------------------------------------------------------

    return {
        x.lower().strip()
        for x in aliases
        if x.strip()
    }




def analyze_company_sentiment(context, classifier):

    if not context:
        return None

    results = classifier(
        context,
        top_k=1
    )

    best_result = results[0]

    return {
        "sentiment_label": LABEL_MAP[
            best_result["label"]
        ],
        "sentiment_score": float(
            best_result["score"]
        )
    }

def get_company_context(text, aliases, window=1):
    if not text or not aliases:
        return None

    # Tách câu
    sentences = re.split(
        r'(?<=[.!?])\s+',
        text
    )

    matched_indexes = []

    # --------------------------------------------------------
    # Tìm câu có chứa BẤT KỲ alias nào của công ty
    # --------------------------------------------------------

    for i, sentence in enumerate(sentences):

        sentence_lower = sentence.lower()

        for alias in aliases:

            if alias in sentence_lower:
                matched_indexes.append(i)
                break

    if not matched_indexes:
        return None

    # --------------------------------------------------------
    # Lấy context xung quanh
    # --------------------------------------------------------

    context = []

    for index in matched_indexes:

        start = max(
            0,
            index - window
        )

        end = min(
            len(sentences),
            index + window + 1
        )

        context.extend(
            sentences[start:end]
        )

    # --------------------------------------------------------
    # Loại duplicate
    # --------------------------------------------------------

    context = list(
        dict.fromkeys(context)
    )

    return " ".join(context)

# ============================================================
# PROCESS NEWS
# ============================================================


def process_fact_company_news (parquet_file_path, ner, classifier):

    # --------------------------------------------------------
    # Read JSON
    # --------------------------------------------------------
        
    spark = (
        SparkSession.builder
        .appName("News NER Analysis")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )

    duckdb_path = (
        "/home/ubuntu/PIPE-Line_data_Stock-Market/"
        "datawarehouse.duckdb"
    )

    conn = None

    try:
        # 1. Đọc dữ liệu tin tức từ Parquet
        df_news = spark.read.parquet(parquet_file_path)

        df_news = df_news.select(
            col("article_id"),
            col("title"),
            col("full_content")
        ).filter(
            col("full_content").isNotNull()
            & (trim(col("full_content")) != "")
        )

        total = df_news.count()
        print(f"Articles available for sentiment: {total}")

        if total == 0:
            print("Không có bài báo để xử lý.")
            return

        # 2. Lấy thông tin công ty từ DuckDB
        conn = duckdb.connect(database=duckdb_path)

        company_pd = conn.execute("""
            SELECT company_id, ticker_company, company_name
            FROM dim_companies
            WHERE company_name IS NOT NULL
        """).fetchdf()

        if company_pd.empty:
            raise ValueError("dim_company không có dữ liệu công ty.")

        # Chuẩn hóa tên công ty
        company_pd["company_name_normalized"] = (
            company_pd["company_name"]
            .str.strip()
            .str.lower()
        )

        # 3. Xử lý tin tức theo từng nhóm 10 bài
        news_iterator = df_news.toLocalIterator()

        batch_number = 0
        processed = 0

        while True:
            # Chỉ lấy tối đa 10 bài cho nhóm hiện tại
            batch = list(itertools.islice(news_iterator, BATCH_SIZE))

            if not batch:
                break

            batch_number += 1
            print(
                f"\nBatch {batch_number}: "
                f"{len(batch)} bài báo"
            )

            company_data = []

            # 4. NER cho từng bài trong nhóm
            for row in batch:
                article_id = row["article_id"]
                full_content = row["full_content"]

                print(f"NER: {article_id}")

                if full_content.strip() in [
                    "ONLY AVAILABLE IN PAID PLANS",
                    "ONLY AVAILABLE IN PAID PLAN"
                ]:
                    continue

                ner_result = ner(full_content)

                if not ner_result:
                    continue

                for entity in ner_result:
                    if entity.get("entity_group") != "ORGANIZATION":
                        continue

                    organization = entity.get("word", "").strip()

                    if organization:
                        company_data.append({
                            "article_id": article_id,
                            "organization": organization,
                            "ner_score": float(entity["score"])
                        })

            # Không phát hiện tổ chức trong nhóm này
            if not company_data:
                processed += len(batch)
                continue

            # 5. Chuyển kết quả NER thành Spark DataFrame
            df_ner = spark.createDataFrame(company_data)

            df_ner = df_ner.withColumn(
                "organization_normalized",
                lower(trim(col("organization")))
            )

            # 6. Đối chiếu tổ chức với danh sách công ty
            df_company = spark.createDataFrame(company_pd)

            df_matched = (
                df_ner.join(
                    df_company,
                    df_ner.organization_normalized
                    == df_company.company_name_normalized,
                    "inner"
                )
                .select(
                    df_ner.article_id,
                    df_ner.organization,
                    df_company.company_id,
                    df_company.ticker_company,
                    df_company.company_name
                )
            )

            # Bỏ các kết quả không ghép được với công ty
            matched_rows = df_matched.toLocalIterator()

            # Lấy nội dung bài báo trong nhóm hiện tại
            content_by_article = {
                row["article_id"]: row["full_content"]
                for row in batch
            }

            sentiment_data = []

            # 7. Phân tích sentiment cho từng công ty đã ghép
            for matched in matched_rows:
                article_id = matched["article_id"]
                company_name = matched["company_name"]
                company_id = matched["company_id"]
                ticker_company = matched["ticker_company"]
                organization = matched["organization"]

                full_content = content_by_article.get(article_id)

                aliases = generate_company_aliases(company_name)

                context = get_company_context(
                    full_content,
                    aliases
                )

                if not context:
                    print(
                        f"Không tìm thấy context: "
                        f"{organization} / {ticker_company}"
                    )
                    continue

                sentiment_result = analyze_company_sentiment(
                    context,
                    classifier
                )

                if sentiment_result is None:
                    continue

                sentiment_data.append({
                    "article_id": article_id,
                    "company_id": company_id,
                    "ticker_company": ticker_company,
                    "company_name": company_name,
                    "sentiment_label":
                        sentiment_result["sentiment_label"],
                    "sentiment_score":
                        sentiment_result["sentiment_score"]
                })

            # 8. Ghi kết quả của nhóm hiện tại vào DuckDB
            if sentiment_data:
                df_sentiment = spark.createDataFrame(sentiment_data)

                df_sentiment = df_sentiment.select(
                    col("article_id"),
                    col("company_id"),
                    col("sentiment_label"),
                    col("sentiment_score")
                )

                sentiment_pd = df_sentiment.toPandas()

                news_ids = conn.execute("""
                    SELECT
                        news_id,
                        news_article_id AS article_id
                    FROM dim_news
                """).fetchdf()

                fact_pd = sentiment_pd.merge(
                    news_ids,
                    on="article_id",
                    how="left"
                )

                fact_pd = fact_pd[
                    fact_pd["news_id"].notnull()
                    & fact_pd["company_id"].notnull()
                ]

                if not fact_pd.empty:
                    fact_pd = fact_pd[
                        [
                            "company_id",
                            "news_id",
                            "sentiment_score",
                            "sentiment_label"
                        ]
                    ]

                    arrow_table = pa.Table.from_pandas(
                        fact_pd,
                        preserve_index=False
                    )

                    conn.register(
                        "batch_fact_news",
                        arrow_table
                    )

                    conn.execute("""
                        INSERT INTO fact_news_companies (
                            news_company_company_id,
                            news_company_news_id,
                            news_company_ticker_sentiment_score,
                            news_company_ticker_sentiment_label
                        )
                        SELECT
                            company_id,
                            news_id,
                            sentiment_score,
                            sentiment_label
                        FROM batch_fact_news AS b
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM fact_news_companies AS f
                            WHERE
                                f.news_company_news_id = b.news_id
                                AND f.news_company_company_id = b.company_id
                        )
                    """)

                    conn.unregister("batch_fact_news")

                    print(
                        f"Đã ghi {len(fact_pd)} kết quả "
                        f"từ batch {batch_number}"
                    )

            processed += len(batch)
            print(f"Đã xử lý {processed}/{total} bài báo")

        print("Hoàn thành process_fact_company_news.")

    finally:
        if conn is not None:
            conn.close()

        spark.stop()

    
def transform_to_datawarehouse_5():
    
    ner = pipeline(
    "token-classification",
    model="dathuynh1108/vi-ner-videberta",
    aggregation_strategy="simple"
    )

    classifier = pipeline(
        "text-classification",
        model="FiinGroup/phobert-finetuned",
        tokenizer="FiinGroup/phobert-finetuned"
    )
    news_hdfs_path = '/user/ubuntu/datalake/news'
    
    latest_file = get_late_parquet_file(hdfs_directory=news_hdfs_path)
    
    process_fact_company_news(latest_file, ner, classifier)