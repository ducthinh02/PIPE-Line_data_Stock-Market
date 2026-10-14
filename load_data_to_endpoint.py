from flask import Flask, jsonify, request
import duckdb

app = Flask(__name__)

@app.route('/dim_time', methods=['GET'])
def get_dim_time():
    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb"
    )
    
    query = """
        SELECT * FROM dim_time;
        """
    result = duck_conn.execute(query).fetchall()
    
    columns = [desc[0] for desc in duck_conn.description] 
    data = [dict(zip(columns,row)) for row in result]
    
    duck_conn.close()
    return jsonify(data)

@app.route('/dim_companies', methods=['GET'])
def get_dim_companies():
    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb"
    )
    
    query = """
        SELECT * FROM dim_companies;
        """
    result = duck_conn.execute(query).fetchall()
    
    columns = [desc[0] for desc in duck_conn.description] 
    data = [dict(zip(columns,row)) for row in result]
    
    duck_conn.close()
    return jsonify(data)

@app.route('/dim_currency', methods=['GET'])
def get_dim_currency():
    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb"
    )
    
    query = """
        SELECT * FROM dim_currency;
        """
    result = duck_conn.execute(query).fetchall()
    
    columns = [desc[0] for desc in duck_conn.description] 
    data = [dict(zip(columns,row)) for row in result]
    
    duck_conn.close()
    return jsonify(data)

@app.route('/dim_index', methods=['GET'])
def get_dim_index():
    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb"
    )
    
    query = """
        SELECT * FROM dim_index;
        """
    result = duck_conn.execute(query).fetchall()
    
    columns = [desc[0] for desc in duck_conn.description] 
    data = [dict(zip(columns,row)) for row in result]
    
    duck_conn.close()
    return jsonify(data)

@app.route('/dim_news', methods=['GET'])
def get_dim_news():
    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb"
    )
    
    query = """
        SELECT * FROM dim_news;
        """
    result = duck_conn.execute(query).fetchall()
    
    columns = [desc[0] for desc in duck_conn.description] 
    data = [dict(zip(columns,row)) for row in result]
    
    duck_conn.close()
    return jsonify(data)

@app.route('/dim_retail', methods=['GET'])
def get_dim_retail():
    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb"
    )
    
    query = """
        SELECT * FROM dim_retail;
        """
    result = duck_conn.execute(query).fetchall()
    
    columns = [desc[0] for desc in duck_conn.description] 
    data = [dict(zip(columns,row)) for row in result]
    
    duck_conn.close()
    return jsonify(data)

@app.route('/fact_candles', methods=['GET'])
def get_fact_candles():
    
    candle_time_id = request.args.get("candle_time_id")
    
    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb",
        read_only=True
    )
    try:
        if candle_time_id:
            try:
                candle_time_id = int(candle_time_id)
            except ValueError:
                return jsonify({
                    "error": "candle_time_id must be an integer"
                }), 400

            query = """
                SELECT *
                FROM fact_candles
                WHERE candle_time_id = ?;
            """
            result = duck_conn.execute(
                query, (candle_time_id,)
            ).fetchall()
        else:
            result = duck_conn.execute(
                "SELECT * FROM fact_candles;"
            ).fetchall()

        columns = [desc[0] for desc in duck_conn.description]
        return jsonify([
            dict(zip(columns, row)) for row in result
        ])
    finally:
        duck_conn.close()

@app.route('/fact_news_companies', methods=['GET'])
def get_fact_news_companies():
    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb"
    )
    
    query = """
        SELECT * FROM fact_news_companies;
        """
    result = duck_conn.execute(query).fetchall()
    
    columns = [desc[0] for desc in duck_conn.description] 
    data = [dict(zip(columns,row)) for row in result]
    
    duck_conn.close()
    return jsonify(data)

@app.route('/fact_rate', methods=['GET'])
def get_fact_rate():
    time_id = request.args.get("time_id")

    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb",
        read_only=True
    )

    try:
        if time_id:
            try:
                time_id = int(time_id)
            except ValueError:
                return jsonify({
                    "error": "time_id parameter must be an integer"
                }), 400

            query = """
                SELECT *
                FROM fact_rate
                WHERE time_id = ?;
            """
            result = duck_conn.execute(
                query, (time_id,)
            ).fetchall()
        else:
            result = duck_conn.execute(
                "SELECT * FROM fact_rate;"
            ).fetchall()

        columns = [desc[0] for desc in duck_conn.description]
        data = [dict(zip(columns, row)) for row in result]

        return jsonify(data)

    finally:
        duck_conn.close()


@app.route('/fact_retail', methods=['GET'])
def get_fact_retail():
    time_id = request.args.get("time_id")

    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb",
        read_only=True
    )

    try:
        if time_id:
            try:
                time_id = int(time_id)
            except ValueError:
                return jsonify({
                    "error": "time_id parameter must be an integer"
                }), 400

            query = """
                SELECT *
                FROM fact_retail
                WHERE time_id = ?;
            """
            result = duck_conn.execute(
                query, (time_id,)
            ).fetchall()
        else:
            result = duck_conn.execute(
                "SELECT * FROM fact_retail;"
            ).fetchall()

        columns = [desc[0] for desc in duck_conn.description]
        data = [dict(zip(columns, row)) for row in result]

        return jsonify(data)

    finally:
        duck_conn.close()


@app.route('/fct_candles_index', methods=['GET'])
def get_fct_candles_index():
    trande_time_id = request.args.get("trande_time_id")

    duck_conn = duckdb.connect(
        "/home/ubuntu/PIPE-Line_data_Stock-Market/datawarehouse.duckdb",
        read_only=True
    )

    try:
        if trande_time_id:
            try:
                trande_time_id = int(trande_time_id)
            except ValueError:
                return jsonify({
                    "error": "trande_time_id parameter must be an integer"
                }), 400

            query = """
                SELECT *
                FROM fct_candles_index
                WHERE trande_time_id = ?;
            """
            result = duck_conn.execute(
                query, (trande_time_id,)
            ).fetchall()
        else:
            result = duck_conn.execute(
                "SELECT * FROM fct_candles_index;"
            ).fetchall()

        columns = [desc[0] for desc in duck_conn.description]
        data = [dict(zip(columns, row)) for row in result]

        return jsonify(data)

    finally:
        duck_conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)