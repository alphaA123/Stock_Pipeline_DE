import time
import json
import yfinance as yf
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'stock_prices'
TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

def create_producer():
    """Creates a Kafka producer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("Kafka Producer connected successfully.")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def fetch_stock_data(ticker):
    """Fetches real-time stock data from Yahoo Finance."""
    try:
        # Get data for the last day, 1-minute interval
        stock = yf.Ticker(ticker)
        data = stock.history(period="1d", interval="1m")
        
        if not data.empty:
            latest = data.iloc[-1]
            return {
                'ticker': ticker,
                'timestamp': str(latest.name),
                'open': round(latest['Open'], 2),
                'close': round(latest['Close'], 2),
                'high': round(latest['High'], 2),
                'low': round(latest['Low'], 2),
                'volume': int(latest['Volume'])
            }
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
    return None

def run():
    producer = create_producer()
    if not producer:
        print("Could not connect to Kafka. Is Docker running?")
        return

    print(f"Starting data stream for: {TICKERS}")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            for ticker in TICKERS:
                stock_data = fetch_stock_data(ticker)
                if stock_data:
                    producer.send(KAFKA_TOPIC, value=stock_data)
                    print(f"Sent: {stock_data['ticker']} - ${stock_data['close']}")
            
            # Send data to Kafka immediately
            producer.flush()
            
            # Wait 60 seconds before next fetch (Yahoo limits requests)
            print("Waiting 60 seconds for next tick...")
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
        producer.close()

if __name__ == "__main__":
    run()