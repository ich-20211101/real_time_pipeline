from kafka import KafkaConsumer
import json
import pandas as pd

topic_name = 'sales_topic'
records = []

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers = 'localhost:9092',
    auto_offset_reset = 'earliest', # 첫 메시지부터 읽기
    enable_auto_commit = True,
    group_id = 'test-group',
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
)

print("🔎 Waiting for messages...")

for message in consumer:
    record = message.value
    records.append(record)

    # 일단 10개만 받아보자
    if len(records) == 10:
        break

# pandas data frame으로 변환
df = pd.DataFrame(records)

print("\n✅ DataFrame Preview:")
print(df.head())