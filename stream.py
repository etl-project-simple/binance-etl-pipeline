import os, json, psycopg2, websocket
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT')
)



conn_cur = conn.cursor()
    
conn_cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        trade_id BIGINT PRIMARY KEY,
        symbol VARCHAR(10),
        price FLOAT,
        quantity FLOAT,
        trade_time TIMESTAMP,
        is_buyer_maker BOOLEAN
    )
""")
conn.commit()
    
def on_message(ws, message):
    trade = json.loads(message)
    trade_data = (
        trade['t'],
        trade['s'],
        float(trade['p']),
        float(trade['q']),
        datetime.fromtimestamp(trade['T'] / 1000.0),
        trade['m']
    )
    conn_cur.execute("""
        INSERT INTO trades (trade_id, symbol, price, quantity, trade_time, is_buyer_maker)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO NOTHING
    """, trade_data)
    print(f"Inserted trade: {trade_data}")
    conn.commit()
    
ws = websocket.WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_message=on_message
)
ws.run_forever()

