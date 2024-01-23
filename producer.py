from confluent_kafka import Producer
import json
import websocket

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

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            print(data)
            product_id = data.get('product_id')

            if product_id in ["ETH-BTC", "ETH-USD"]:
                kafka_topic = f"coinbase_{product_id.lower()}_topic"
                self.kafka_producer.produce(kafka_topic, key=product_id, value=json.dumps(data))
                self.kafka_producer.flush()

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

    def on_open(self, ws):
        print("WebSocket connection opened")
        ws.send(json.dumps(self.subscribe_message))

    def run(self):
        ws = websocket.WebSocketApp(self.websocket_url, on_message=self.on_message)
        ws.on_open = self.on_open
        ws.run_forever()
