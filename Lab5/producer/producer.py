import csv
import time
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def setup_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=['broker1:9092', 'broker2:9093'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except NoBrokersAvailable:
        print('[WARN] Kafka brokers not available yet.')
        return None
    except Exception as e:
        print(f"[ERROR] Unexpected error while creating producer: {e}")
        return None

print('[INFO] Setting up producer. Waiting for brokers...')

producer = None
while producer is None:
    producer = setup_producer()
    if producer is None:
        time.sleep(5)

print('[INFO] Kafka brokers available. Starting to produce messages.')

try:
    with open('Divvy_Trips_2019_Q4.csv', mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send('Topic1', row)
            producer.send('Topic2', row)
            print(f"[INFO] Sent: {row}")
            time.sleep(1)
except FileNotFoundError:
    print('[ERROR] data.csv not found inside the container!')

producer.flush()