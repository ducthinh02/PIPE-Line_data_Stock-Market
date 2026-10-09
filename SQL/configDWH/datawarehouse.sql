CREATE SEQUENCE company_id_seq;
CREATE SEQUENCE time_id_seq;
CREATE SEQUENCE news_id_seq;
CREATE SEQUENCE topic_id_seq;
CREATE SEQUENCE candles_id_seq;
CREATE SEQUENCE news_company_id_seq;
CREATE SEQUENCE news_topic_id_seq;
CREATE SEQUENCE candles_index_id_seq;
CREATE SEQUENCE dim_index_id_seq;
CREATE SEQUENCE retail_id_seq;
CREATE SEQUENCE fact_retail_id_seq;
CREATE SEQUENCE currency_id_seq;
CREATE SEQUENCE fact_rate_id_seq;

CREATE TABLE IF NOT EXISTS dim_companies (
    company_id INTEGER DEFAULT NEXTVAL('company_id_seq') PRIMARY KEY,

    industry_id INTEGER NOT NULL,

    company_name TEXT NOT NULL,

    ticker_company VARCHAR(10) UNIQUE NOT NULL,

    founded_date DATE,

    number_of_employees INTEGER NOT NULL,

    exchange_id INTEGER NOT NULL,

    company_type VARCHAR(10),

    listing_date DATE,

    listing_price NUMERIC,

    listed_volume NUMERIC,

    outstanding_shares NUMERIC,

    ceo_name VARCHAR(50),
    company_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =========================================================
-- 3. DIMENSION: TIME
-- =========================================================

CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER DEFAULT NEXTVAL('time_id_seq') PRIMARY KEY,

    date DATE NOT NULL,

    day_of_week INTEGER,

    month INTEGER,

    quarter INTEGER,

    year INTEGER
);


-- =========================================================
-- 4. DIMENSION: NEWS
-- =========================================================

CREATE TABLE IF NOT EXISTS dim_news (
    news_id INTEGER DEFAULT NEXTVAL('new_id_seq') PRIMARY KEY,

    news_time_id INTEGER,

    news_article_id VARCHAR NOT NULL,
    news_title VARCHAR NOT NULL,
    news_url VARCHAR NOT NULL,
    news_source_name VARCHAR NOT NULL,
    news_category VARCHAR,
    news_datatype VARCHAR,
    news_overall_sentiment_label VARCHAR,
    news_overall_sentiment_score DOUBLE NOT NULL, 
    time_stamp TIMESTAMP

    FOREIGN KEY (news_time_id)
        REFERENCES dim_time(time_id)
);


-- =========================================================
-- 5. DIMENSION: TOPICS
-- =========================================================

CREATE TABLE IF NOT EXISTS dim_topics (
    topic_id INTEGER DEFAULT NEXTVAL('topic_id_seq') PRIMARY KEY,

    topic_name VARCHAR NOT NULL,

    CONSTRAINT unique_topic_name UNIQUE (topic_name)

);

-- =========================================================
-- 7. FACT: CANDLES
-- =========================================================

CREATE TABLE IF NOT EXISTS fact_candles (
    candle_id INTEGER DEFAULT NEXTVAL('candle_id_seq') PRIMARY KEY,

    candle_company_id INTEGER NOT NULL,

    candle_volume FLOAT NOT NULL,

    candle_open FLOAT NOT NULL,

    candle_close FLOAT NOT NULL,

    candle_high FLOAT NOT NULL,

    candle_low FLOAT NOT NULL,

    candle_time_stamp TIMESTAMP NOT NULL,

    candle_time_id INTEGER,

    FOREIGN KEY (candle_company_id)
        REFERENCES dim_companies(company_id),

    FOREIGN KEY (candle_time_id)
        REFERENCES dim_time(time_id)
);


-- =========================================================
-- 8. FACT: NEWS - COMPANIES
-- =========================================================

CREATE TABLE IF NOT EXISTS fact_news_companies (
    news_company_id INTEGER DEFAULT NEXTVAL('new_company_id_seq') PRIMARY KEY,

    news_company_company_id INTEGER NOT NULL,

    news_company_news_id INTEGER NOT NULL,

    news_company_ticker_sentiment_score FLOAT,

    news_company_ticker_sentiment_label VARCHAR,

    FOREIGN KEY (news_company_company_id)
        REFERENCES dim_companies(company_id),

    FOREIGN KEY (news_company_news_id)
        REFERENCES dim_news(news_id)
);


-- =========================================================
-- 9. FACT: NEWS - TOPICS
-- =========================================================

CREATE TABLE IF NOT EXISTS fact_news_topics (
    news_topic_id INTEGER DEFAULT NEXTVAL('new_topic_id_seq') PRIMARY KEY,

    news_topic_news_id INTEGER NOT NULL,

    news_topic_topic_id INTEGER NOT NULL,

    news_topic_relevance_score FLOAT,

    FOREIGN KEY (news_topic_news_id)
        REFERENCES dim_news(news_id),

    FOREIGN KEY (news_topic_topic_id)
        REFERENCES dim_topics(topic_id)
);

CREATE TABLE IF NOT EXISTS fct_candles_index {
    candles_index_id INTEGER DEFAULT NEXTVAL('candles_index_id_seq') PRIMARY KEY,
    candles_dim_index_id INTEGER ,
    candles_index_volume float NOT NULL,
    candles_index_open float NOT NULL,
    candles_index_close float NOT NULL,
    candles_index_high float NOT NULL,
    candles_index_low float NOT NULL,
    candles_index_timestamp timestamp,
    trande_time_id integer,

    FOREIGN KEY(candles_dim_index_id)
        REFERENCES dim_index(dim_index_id),

    FOREIGN KEY(trande_time_id)
        REFERENCES dim_time(time_id)
};

CREATE TABLE IF NOT EXISTS dim_index {
    dim_index_id INTEGER DEFAULT NEXTVAL('dim_index_id_seq') PRIMARY KEY,
    index_code varchar NOT NULL,
    index_name varchar NOT NULL,
    index_description varchar ,
    group_name varchar NOT NULL
}

-- =========================================
-- DIM RETAIL
-- =========================================

CREATE TABLE IF NOT EXISTS dim_retail (
    retail_id INTEGER DEFAULT NEXTVAL('retail_id_seq') PRIMARY KEY,
    retail_name VARCHAR NOT NULL UNIQUE
);


-- =========================================
-- FACT RETAIL
-- =========================================

CREATE TABLE IF NOT EXISTS fact_retail (
    fact_retail_id INTEGER DEFAULT NEXTVAL('fact_retail_id_seq') PRIMARY KEY,

    retail_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,

    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    time_stamp timestamp,

    FOREIGN KEY (retail_id) REFERENCES dim_retail(retail_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);


CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id INTEGER DEFAULT NEXVAL('currency_id_seq') PRIMARY KEY,
    currency_code VARCHAR NOT NULL UNIQUE,
    currency_name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_rate (
    fact_rate_id INTEGER DEFAULT NEXVAL('fact_rate_id_seq') PRIMARY KEY
    currency_id INTEGER NOT NULL,
    time_id INTEGER NOT NULL,

    buy_cash DOUBLE,
    buy_transfer DOUBLE,
    sell DOUBLE,

    FOREIGN KEY (currency_id)
        REFERENCES dim_currency(currency_id),

    FOREIGN KEY (time_id)
        REFERENCES dim_time(time_id)
);