CREATE DATABASE masterdata;

\c masterdata;

CREATE TABLE IF NOT EXISTS industries(
    industry_id SERIAL PRIMARY KEY,
    industries_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    icb_code INTEGER NOT NULL,
    industry_name VARCHAR(50) NOT NULL,
    CONSTRAINT unique_industries UNIQUE(icb_code,industry_name)
);

CREATE TABLE IF NOT EXISTS EXCHANGE(
    exchange_id SERIAL PRIMARY KEY,
    exchange_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    exchange_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS COMPANIES(
    company_id SERIAL PRIMARY KEY,
    industry_id INTEGER NOT NULL,
    company_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    ticker_company VARCHAR(10) UNIQUE NOT NULL,
    founded_date date,
    charter_capital INTEGER NOT NULL,
    number_of_employees integer NOT NULL,
    exchange_id INTEGER NOT NULL,
    company_type varchar(10) ,
    listing_date date,
    listing_price NUMERIC,                            
    listed_volume NUMERIC,                       
    outstanding_shares NUMERIC,
    ceo_name VARCHAR(50),
    FOREIGN KEY (industry_id) REFERENCES industries(industry_id),
    FOREIGN KEY (exchange_id) REFERENCES EXCHANGE(exchange_id)
);

CREATE TABLE IF NOT EXISTS CK_INDEX(
    index_id SERIAL PRIMARY KEY,
    ck_index_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    index_code VARCHAR(10)  NOT NULL,
    index_name VARCHAR(50)  NOT NULL,
    index_description TEXT NOT NULL,
    index_group_id INTEGER NOT NULL,
    CONSTRAINT unique_ck_index UNIQUE (index_code,index_name),
    CONSTRAINT fk_ck_index_group_id
        FOREIGN KEY (index_group_id) 
        REFERENCES MARKET_GROUP(index_group_id)
);

CREATE TABLE IF NOT EXISTS INDEX_GROUPS(
    index_group_id SERIAL PRIMARY KEY,
    INDEX_GROUPS_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    group_name VARCHAR(20) UNIQUE NOT NULL
)

CREATE INDEX idx_company_time_stamp ON companies(company_update_time_stamp);
CREATE INDEX idx_company_exchange_id ON companies(exchange_id);
CREATE INDEX idx_company_industry_id ON companies(industry_id);
CREATE INDEX idx_ck_index_time_stamp ON CK_INDEX(ck_index_update_time_stamp);
CREATE INDEX idx_ck_index_group_id ON CK_INDEX(index_group_id);


