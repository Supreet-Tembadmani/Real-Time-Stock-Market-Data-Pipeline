from confluent_kafka import Producer
import json

def kafka_producer(data, topic, broker):
    p = Producer({'bootstrap.servers': broker})
    p.produce(topic, json.dumps(data).encode('utf-8'))
    p.flush()

if __name__ == "__main__":
    broker = 'localhost:9092'
    topic = 'stock_data'
    ticker = "AAPL"
    
    while True:
        data = fetch_stock_data(ticker)
        kafka_producer(data.to_dict(), topic, broker)
        time.sleep(60)
