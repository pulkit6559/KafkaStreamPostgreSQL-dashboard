from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2 import sql
import json

class KafkaConsumer:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, postgres_connection_string):
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'
        })
        self.kafka_topic = kafka_topic
        self.postgres_connection_string = postgres_connection_string

    def connect_to_postgres(self):
        return psycopg2.connect(self.postgres_connection_string)

    def create_table_if_not_exists(self, postgres_conn, cursor, topic):
        print(topic)
        create_table_query = f"""
        -- DROP TABLE IF EXISTS {topic}_data;
        
        CREATE TABLE IF NOT EXISTS {topic}_data (
            id SERIAL PRIMARY KEY,
            signal VARCHAR(10) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            price FLOAT(1)
        );

        CREATE INDEX IF NOT EXISTS idx_stock_data_timestamp_symbol ON {topic}_data (timestamp, signal);
        """
        cursor.execute(create_table_query)
        postgres_conn.commit()

    def consume_and_store(self):
        self.consumer.subscribe([self.kafka_topic])

        # # Connect to PostgreSQL
        postgres_conn = self.connect_to_postgres()
        cursor = postgres_conn.cursor()

        topic = self.kafka_topic.replace("-","_").lower().replace("_topic", "")
        # # Create table if not exists
        self.create_table_if_not_exists(postgres_conn, cursor, topic)
        
        cursor.execute(f"""SELECT * FROM {topic}_data;""")
        result = cursor.fetchall()
        print([row for row in result])

        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event, not an error
                        continue
                    else:
                        print(msg.error())
                        break

                # Process the message and store it in PostgreSQL
                # print(msg.value().decode('utf-8'))
                self.process_and_store_message(msg.value().decode('utf-8'), cursor, postgres_conn)
                cursor.execute(f"""SELECT * FROM {topic}_data;""")
                result = cursor.fetchall()
                print([row for row in result])
        except KeyboardInterrupt:
            pass
        finally:
            # Close connections
            cursor.close()
            postgres_conn.close()

    def process_and_store_message(self, message, cursor, postgres_conn):
        # Parse JSON message
        data = json.loads(message)

        print(data)
        # Extract data fields as needed
        product_id = data.get('product_id').replace("-","_").lower()
        signal = data.get('side')
        timestamp = data.get('time')
        price = data.get('price')
        
        # Extract other fields...

        # Insert data into PostgreSQL
        insert_query = sql.SQL(f"""
            INSERT INTO coinbase_{product_id}_data (signal, timestamp, price)
            VALUES (%s, %s, %s);
        """)

        cursor.execute(insert_query, (signal, timestamp, price))

        # Commit the transaction
        postgres_conn.commit()


if __name__ == "__main__":
    # Define configurations
    kafka_bootstrap_servers = 'localhost:29092'  # Replace with your Kafka bootstrap servers
    kafka_topic = 'coinbase_eth-btc_topic'  # Replace with your Kafka topic

    username = 'postgres'
    password = 'password'
    host = 'localhost'
    port = '5432'
    database_name = 'COINBASE_DB'

    # Construct the PostgreSQL connection string
    postgres_connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'


    # Initialize and run Kafka consumer
    kafka_consumer = KafkaConsumer(kafka_bootstrap_servers, kafka_topic, postgres_connection_string)
    kafka_consumer.consume_and_store()