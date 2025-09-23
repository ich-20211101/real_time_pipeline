import csv
import json
import time

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'], # 리스트 or 튜플 형태여야 함
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

source_file = '../data/Sample-Superstore.csv'

topic_name = 'sales_topic'

with open(source_file, newline='', encoding='cp1252') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print(f"SendingL {row}")
        producer.send(topic_name, value=row)
        time.sleep(1) # 1초 간격으로 메시지 보내기 (실시간 시뮬)

producer.flush()
producer.close()