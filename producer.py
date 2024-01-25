from confluent_kafka import Producer
import json
import websocket
import threading
import time

class KafkaProducer:
    def __init__(self, bootstrap_servers):
        producer_conf = {'bootstrap.servers': bootstrap_servers, 'client.id': 'eth_price_producer'}
        self.producer = Producer(producer_conf)

    def produce(self, topic, key, value):
        self.producer.produce(topic, key=key, value=value)

    def flush(self):
        self.producer.flush()


class WebSocketProducer:
    def __init__(self, websocket_url, subscribe_message, kafka_producer):
        self.websocket_url = websocket_url
        self.subscribe_message = subscribe_message
        self.kafka_producer = kafka_producer
        self.ws = None
        self.last_activity_timestamp = time.time()

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            product_id = data.get('product_id')

            if product_id in ["ETH-BTC", "ETH-USD"]:
                kafka_topic = f"coinbase_{product_id.lower()}_topic"
                self.kafka_producer.produce(kafka_topic, key=product_id, value=json.dumps(data))
                self.kafka_producer.flush()

                # Update last activity timestamp on message receive
                self.last_activity_timestamp = time.time()

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

    def on_open(self, ws):
        print("WebSocket connection opened")
        ws.send(json.dumps(self.subscribe_message))

    def send_subscribe_message(self):
        print("Sending subscribe message")
        self.ws.send(json.dumps(self.subscribe_message))

    def check_inactivity(self):
        # Check inactivity and send subscribe message if needed
        current_timestamp = time.time()
        if current_timestamp - self.last_activity_timestamp >= 5:
            self.send_subscribe_message()

        # Schedule the next check
        self.timer = threading.Timer(1, self.check_inactivity)
        self.timer.start()

    def run(self):
        self.ws = websocket.WebSocketApp(self.websocket_url, on_message=self.on_message)
        self.ws.on_open = self.on_open

        # Start the inactivity check timer
        self.timer = threading.Timer(1, self.check_inactivity)
        self.timer.start()

        # Start the WebSocket connection
        self.ws.run_forever()

    def stop(self):
        # Stop the WebSocket connection and the inactivity check timer
        if self.ws:
            self.ws.close()
        if self.timer:
            self.timer.cancel()

