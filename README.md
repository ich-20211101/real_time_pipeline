# Real-time Data Pipeline Simulation  
**Kafka + Airflow + PostgreSQL + Tableau**

This project demonstrates a real-time data pipeline that simulates streaming sales data from a CSV file into a PostgreSQL database using Kafka and Airflow, with final visualization in Tableau.

---

## 🎯 Goals

- Understand how to build a full real-time ETL pipeline
- Practice with Kafka, Airflow DAGs, PostgreSQL, and Tableau
- Use as a practical portfolio project when applying for co-op / internship

---

## 🔧 Stack Overview

| Step            | Tool            | Role                                 |
|-----------------|------------------|--------------------------------------|
| Data Ingestion  | Kafka Producer   | Sends records from CSV to Kafka topic |
| Stream Engine   | Apache Kafka     | Transports messages in real time     |
| ETL Workflow    | Airflow + Pandas | Pulls data → cleans it → loads into DB |
| Storage         | PostgreSQL       | Stores structured sales data         |
| Visualization   | Tableau          | Analyzes sales trends & patterns     |
| Dev Environment | Docker Compose   | Local orchestration of services      |

---

## 🔄 Data Flow

1. **CSV → Kafka**  
   Python producer reads `Sample-Superstore.csv` line-by-line and pushes messages to Kafka topic `sales_topic`.

2. **Kafka → Airflow DAG**  
   Airflow DAG reads from Kafka (via CSV or consumer logic), processes messages with Pandas.

3. **Transform → PostgreSQL**  
   Cleaned data is inserted into a single table: `superstore_sales`.

4. **PostgreSQL → Tableau**  
   Tableau connects to the DB and visualizes revenue, product performance, customer patterns, etc.

---

## 🐍 Kafka Producer Script (producer.py)

This script simulates real-time data ingestion by reading the Superstore CSV file line-by-line and sending each row as a JSON message to the Kafka topic sales_topic.

🔹 Features:
	•	Uses Python csv.DictReader to parse structured rows  
	•	Sends each record to Kafka with a 1-second interval (time.sleep(1))  
	•	Converts each row into JSON format via KafkaProducer with custom serializer  
	•	Controlled stream to help visualize data movement through the pipeline  

📌 Code Summary:

```python
with open(source_file, newline='', encoding='cp1252') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        producer.send(topic_name, value=row)
        time.sleep(1)
```

✅ Helps simulate a real-time stream from static CSV  
✅ Makes data movement transparent for ETL pipeline validation  
✅ Supports understanding of Kafka message publishing mechanics  

---

## 📥 Kafka Consumer (Batch to CSV)

The consumer_batch_to_csv.py script listens to the sales_topic Kafka topic and collects messages in batches of 100 records. Once a batch is complete, it saves the data as a new CSV file in the data/ folder.
This simulates a mini-batch ETL pipeline and allows for clear inspection of message flow from Kafka before insertion into the database.

🔹 Core Logic

```python
if len(records) == BATCH_SIZE:
    df = pd.DataFrame(records)
    file_path = os.path.join(DATA_DIR, f'batch_{batch_number}.csv')
    df.to_csv(file_path, index=False)
```

✅ Why it’s included  
	•	Enables inspection of data before transformation  
	•	Breaks down real-time stream into manageable files  
	•	Useful for debugging and batch-based processing with Airflow DAGs

---

## 🧱 PostgreSQL Table Schema

The cleaned data from Airflow is stored in the superstore_sales table.
This table was designed to capture essential sales metrics and metadata for analysis in Tableau.

```sql
CREATE TABLE users (
    customer_id VARCHAR(20) PRIMARY KEY,
    segment VARCHAR(50),
    region VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50)
);

CREATE TABLE products (
    product_id VARCHAR(30) PRIMARY KEY,
    category VARCHAR(50),
    sub_category VARCHAR(50)
);

CREATE TABLE orders (
    row_id SERIAL PRIMARY KEY,
    order_id VARCHAR(30) NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE,
    ship_mode VARCHAR(50),
    order_priority VARCHAR(30),
    customer_id VARCHAR(20) REFERENCES users(customer_id),
    product_id VARCHAR(30) REFERENCES products(product_id),
    sales NUMERIC(10, 2),
    quantity INTEGER,
    discount NUMERIC(5, 2),
    profit NUMERIC(10, 4)
);
```

✅ This schema is based on the Superstore dataset  
✅ Optimized for business intelligence queries: revenue trends, product performance, and customer segmentation

---

## ⚠️ Notes
	•	Docker compose is used for local development
	•	docker-compose down -v will delete all volumes including PostgreSQL data
	•	Set KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 for host access

---

### 👤 Author

Suyeon Kim
Data Engineering Student | Ex-Java Backend Dev  
📍 Vancouver, Canada