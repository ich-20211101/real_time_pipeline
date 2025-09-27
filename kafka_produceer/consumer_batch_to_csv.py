from kafka import KafkaConsumer
import pandas as pd
import json
import os

TOPIC_NAME = "sales_topic"
BOOTSTRAP_SERVER = "localhost:9092"
BATCH_SIZE = 100

BASE_DIR = os.path.dirname(os.path.abspath(os.path.abspath(__file__)))
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers = BOOTSTRAP_SERVER,
    auto_offset_reset = 'earliest', # consumer 켜기 전에 보낸 메시지도 전부 받기
    enable_auto_commit = True,
    group_id = 'batch-csv-consumer',
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
)

records = []


print("📥 Kafka Consumer Started")
print("📡 Listening for messages...\n")

for message in consumer:
    records.append(message.value)

    if len(records) == BATCH_SIZE:
        df = pd.DataFrame(records)

        # 배치 번호
        existing_batches = [name for name in os.listdir(DATA_DIR) if name.startswith('batch_') and name.endswith('.csv')]
        batch_number = len(existing_batches) + 1
        # 파일 경로
        file_path = os.path.join(DATA_DIR, f'batch_{batch_number}.csv')
        # 파일 저장
        df.to_csv(file_path, index = False)

        print(f"💾 Batch {batch_number:02} → ✅ Saved {len(records)} records to: {file_path}")

        # 다음 배치 준비
        records = []