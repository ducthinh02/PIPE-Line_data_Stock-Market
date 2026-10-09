import duckdb

path_duckdb = '/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb'

conn = duckdb.connect(database=path_duckdb)

conn.sql("SELECT * FROM dim_companies LIMIT 10;").show()
conn.sql("SELECT COUNT(*) FROM dim_companies;").show()

conn.sql("SELECT * FROM dim_time LIMIT 10;").show()
conn.sql("SELECT COUNT(*) FROM dim_time;").show()

conn.sql("SELECT * FROM dim_news LIMIT 10;").show()
conn.sql("SELECT COUNT(*) FROM dim_news;").show()

conn.sql("SELECT * FROM dim_topics LIMIT 10;").show()
conn.sql("SELECT COUNT(*) FROM dim_topics;").show()

conn.sql("SELECT * FROM fact_candles LIMIT 10;").show()
conn.sql("SELECT COUNT(*) FROM fact_candles;").show()

conn.sql("SELECT * FROM fact_news_companies LIMIT 10;").show()
conn.sql("SELECT COUNT(*) FROM fact_news_companies;").show()

conn.sql("SELECT * FROM fact_news_topics LIMIT 10;").show()
conn.sql("SELECT COUNT(*) FROM fact_news_topics;").show()