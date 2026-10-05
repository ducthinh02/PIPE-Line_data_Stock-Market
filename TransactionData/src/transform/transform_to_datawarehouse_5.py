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


# ============================================================
# NER MODEL
# ============================================================

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

    # --------------------------------------------------------
    # Bỏ "Tập đoàn"
    #
    # Tập đoàn Hòa Phát
    # -> Hòa Phát
    # --------------------------------------------------------

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




def analyze_company_sentiment(context):

    if not context:
        return None

    results = classifier(
        context,
        top_k=None
    )

    best_result = max(
        results,
        key=lambda x: x["score"]
    )

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


def process_fact_company_news (parquet_file_path):

    # --------------------------------------------------------
    # Read JSON
    # --------------------------------------------------------
    
    spark = SparkSession.builder\
        .appName("News NER Analysis")\
            .config("spark.sql.caseSensitive", "true")\
            .getOrCreate()
    
    dfspark_news = spark.read.parquet(parquet_file_path)
    
    if dfspark_news.isEmpty():
        raise ValueError("ERROR!")

    dfspark_news.printSchema()
    dfspark_news.show()
    
    dfspark_news = dfspark_news.select(
            col("article_id"),
            col("title"),
            col("full_content")
        )
    
    dfspark_news = dfspark_news.filter(
    col("full_content").isNotNull() &
    (col("full_content") != "")
    )   
    
    print(f"Articles available for sentiment: "
        f"{dfspark_news.count()}")
    
    
    # chuyển dataframe spark thành python list
    rows = dfspark_news.collect()
    
    company_data = []
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
        
        ner_result = ner(full_content)
        
        if ner_result is None:
            print(
                " Skip: sentiment analysis failed"
            )
            continue
        
        # ============================================================
        # Lấy ORGANIZATION
        # ============================================================ 
        
        organizations = []
        for entity in ner_result:
            if entity["entity_group"] == "ORGANIZATION":
                
                organizations.append(
                    {
                        "article_id": article_id,
                        "organization": entity["word"],
                        "ner_score": float(entity["score"])
                    }
                )
            
        if not organizations:
            print("Không tìm thấy company")
        
        company_data.extend(organizations)
    
    
    df_company_news = spark.createDataFrame(company_data)
    
    df_company_news.show(
        truncate=False
    )

    df_company_news = df_company_news.withColumn(
        "organization_normalized",
        lower(
            trim(
                col("organization")
            )
        )
    )
    
    duckdb_path = "D:/ETL-pipeline data analytics securities/datawarehouse.duckdb"
    conn = duckdb.connect(duckdb_path)

    id_company_pd = conn.execute(
        """  SELECT
                company_id,
                ticker_company,
                company_name
                FROM dim_company """
        ).fetchdf()

    id_company_df = spark.createDataFrame(id_company_pd)
    id_company_df = id_company_df.withColumn(
            "company_name_normalized",
            lower(
                trim(
                    col("company_name")
                )
            )
        )
    
    df_compamy_news_secord = df_company_news.join(
        id_company_df,
        on= df_company_news.organization_normalized == id_company_df.company_name_normalized,
        how= "left"
        ).select(
            df_company_news["article_id"],
            df_company_news["organization"],
            id_company_df["company_id"],
            id_company_df["ticker_company"],
            id_company_df["company_name"]
    )
    df_company_news_third = dfspark_news.join(
        df_compamy_news_secord,
        on= "article_id",
        how= "left"
    ).select(
        dfspark_news["article_id"],
        df_compamy_news_secord["organization"],
        dfspark_news["full_content"],
        df_compamy_news_secord["company_id"],
        df_compamy_news_secord["ticker_company"],
        df_compamy_news_secord["company_name"],
        )
    
    df_company_news_third.show(
    truncate=False
    )
    
    df_company_news_third.printSchema()
    
    df_company_news_third.filter(
        col("company_id").isNull()
    ).show(
        truncate= False
    )
    
    rows_company_news = df_company_news_third.collect()
    
    sentiment_data = []    
    
    for row in rows_company_news:

        article_id = row[
            "article_id"
        ]

        organization = row[
            "organization"
        ]

        company_id = row[
            "company_id"
        ]

        ticker_company = row[
            "ticker_company"
        ]

        company_name = row[
            "company_name"
        ]

        full_content = row[
            "full_content"
        ]

        # ----------------------------------------------------
        # Tạo alias của company
        # ----------------------------------------------------

        aliases = generate_company_aliases(
            company_name
        )

        # ----------------------------------------------------
        # Lấy context
        # ----------------------------------------------------

        context = get_company_context(
            full_content,
            aliases
        )

        if not context:

            print(
                f"Không tìm thấy context: "
                f"{organization} / "
                f"{ticker_company}"
            )

            continue

        # ----------------------------------------------------
        # PhoBERT
        # ----------------------------------------------------

        sentiment_result = (
            analyze_company_sentiment(
                context
            )
        )

        if sentiment_result is None:

            continue

        # ----------------------------------------------------
        # Fact record
        # ----------------------------------------------------

        sentiment_data.append(
            {
                "article_id": article_id,

                "company_id": company_id,

                "ticker_company":
                    ticker_company,

                "company_name":
                    company_name,

                "sentiment_label":
                    sentiment_result[
                        "sentiment_label"
                    ],

                "sentiment_score":
                    sentiment_result[
                        "sentiment_score"
                    ]
            }
        )

    # ========================================================
    # CREATE FINAL SPARK DATAFRAME
    # ========================================================

    if not sentiment_data:

        raise ValueError(
            "Không tạo được fact company news!"
        )

    df_fact_company_news = (
        spark.createDataFrame(
            sentiment_data
        )
    )

    # ========================================================
    # RESULT
    # ========================================================

    print(
        "Final fact_company_news:"
    )

    df_fact_company_news.show(
        truncate=False
    )

    df_fact_company_news.printSchema()
    
    df_fact_company_news = df_fact_company_news.select(
        col("article_id"),
        col("company_id"),
        col("sentiment_label"),
        col("sentiment_score")
    )
    df_fact_company_news_final = df_fact_company_news.toPandas()
    
    # join news_id từ dim_news vào fact_company_news
    
    id_time_news_df = conn.execute(
    """
        SELECT news_id,
                news_article_id AS article_id 
        FROM dim_news
    """
    ).fetchdf()
    
    arrow_table_fact_company_news = df_fact_company_news_final.merge(
        id_time_news_df,
        on = "article_id",
        how = 'left') 
    
    arrow_table_fact_company_news = arrow_table_fact_company_news[
        [
            "news_id",
            "company_id",
            "sentiment_label",
            "sentiment_score"
        ]
    ]
    
    arrow_table_fact_company_news = arrow_table_fact_company_news[
        arrow_table_fact_company_news["news_id"].notnull() &
        arrow_table_fact_company_news["company_id"].notnull()
    ]
    
    arrow_table_fact_news_company= pa.Table.from_pandas(
        arrow_table_fact_company_news
    )

    conn.register("arrow_table_fact_news_company",
                arrow_table_fact_news_company)
    
    conn.execute(
    """
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
        FROM arrow_table_fact_news_company
        WHERE NOT EXISTS (
            SELECT 1
            FROM fact_news_companies f
            WHERE f.news_company_news_id = news_id
            AND f.news_company_company_id = company_id
            )
    """
    )
    conn.close()
    
    spark.stop()
    
def transform_to_datawarehouse_5():
    hdfs = ''
    
    latest_file = get_late_parquet_file(hdfs_directory=hdfs)
    
    process_fact_company_news(latest_file)