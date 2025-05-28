import os
import time
import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

def setup_consumer():
    try:
        return KafkaConsumer(
           'Topic1',
            bootstrap_servers=os.getenv('BOOTSTRAP_SERVERS').split(','), # Отримує адресу(и) Kafka-брокера зі змінної середовища
            value_deserializer=lambda v: json.loads(v.decode('utf-8')), # десеріалізації (розпакування) даних
            auto_offset_reset='earliest', # починаємо з першого повідомлення
            #group_id='group1' # Ідентифікатор групи консьюмерів. Усі консьюмери з однаковим group_id ділять між собою партиції топіка.
        )
    except NoBrokersAvailable:
        print('[WARN] Kafka brokers not available yet.')
        return None
    except Exception as e:
        print(f"[ERROR] Unexpected error while creating consumer: {e}")
        return None

print('[INFO] Setting up consumer. Waiting for brokers...')

consumer = None
while consumer is None:
    consumer = setup_consumer()
    if consumer is None:
        time.sleep(5)

print('[INFO] Kafka brokers available. Starting to consume messages.')

# Виведення повідомлень з Topic1 у консоль
for message in consumer:
    print(f"[Consumer1] Received: {message.value}")
