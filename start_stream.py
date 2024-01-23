from producer import WebSocketProducer, KafkaProducer

if __name__ == "__main__":
    # Define configurations
    websocket_url = "wss://ws-feed.pro.coinbase.com"
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["ETH-USD", "ETH-EUR"],
        "channels": [
            "level2",
            "heartbeat",
            {
                "name": "ticker",
                "product_ids": ["ETH-BTC", "ETH-USD"]
            }
        ]
    }
    kafka_bootstrap_servers = 'localhost:29092'  # Replace with your Kafka bootstrap servers

    # Initialize Kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    # Initialize and run WebSocket producer
    websocket_producer = WebSocketProducer(websocket_url, subscribe_message, kafka_producer)
    websocket_producer.run()
