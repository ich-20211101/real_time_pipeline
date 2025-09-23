# Real-time Data Pipeline Simulation (Kafka + Airflow + PostgreSQL + Tableau)

This side project demonstrates a complete **real-time data pipeline** architecture using:

> **Kafka (stream ingestion)** + **Airflow + Pandas (ETL orchestration)** + **PostgreSQL (storage)** + **Tableau (visualization)**

---

## 📌 Project Goals

- Gain hands-on experience with modern data engineering tools
- Build a full-stack real-time data pipeline from ingestion to visualization
- Use this project as a portfolio to show practical understanding of ETL and streaming architecture
- Support future extensions such as machine learning and feedback loops via Kafka

---

## 🔧 Tech Stack

| Layer            | Tool                      | Description                                  |
|------------------|---------------------------|----------------------------------------------|
| Data Ingestion   | **Apache Kafka**          | Real-time streaming via Kafka Producer       |
| Workflow Engine  | **Apache Airflow**        | DAG-based ETL orchestration                  |
| Transformation   | **Pandas** (Python)       | Cleansing, transformation, enrichment        |
| Data Storage     | **PostgreSQL**            | Relational database for structured data      |
| Visualization    | **Tableau**               | Dashboards for sales & revenue insights      |
| Containerization | **Docker Compose**        | Isolated, reproducible local dev environment |

---

## 📁 Project Structure

real_time_pipeline/
│
├── dags/               # Airflow DAG scripts
│   └── etl_dag.py
│
├── kafka_producer/     # Kafka Producer (Python script)
│   └── producer.py
│
├── data/               # Raw CSV batches from producer
│   └── superstore.csv
│
├── docker-compose.yml  # Kafka + Airflow + PostgreSQL setup
│
└── README.md

---

## 🔄 Data Flow Overview

### 1. **Data Source: CSV → Kafka Producer**
- 📦 Source: [Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) from Kaggle  
- 🐍 Python reads CSV line-by-line and sends each record as a JSON message to `sales_topic` in Kafka

### 2. **Stream Ingest: Kafka**
- Kafka handles real-time delivery via topic `sales_topic`
- Broker address: `localhost:9092` (Dockerized)

### 3. **ETL: Airflow + Pandas**
- Airflow DAG reads from Kafka (via intermediate CSV)
- Pandas cleans & transforms data (e.g., column filtering, enrichment)
- Result is inserted into PostgreSQL

### 4. **Storage: PostgreSQL**
- Tables: `orders`, `products`, `customers` (TBD)
- Structured schema for downstream analytics
- Easy integration with BI tools (Tableau)

### 5. **Visualization: Tableau**
- PostgreSQL is connected as a data source
- Example dashboards:
  - 📊 Top-Selling Products
  - 📈 Daily / Monthly Revenue Trends
  - 👤 Customer Purchase Patterns

---

## 🧪 Local Setup (Coming Soon)

> Full instructions to build and test this pipeline will be provided after DAG and DB logic is finalized.

---

## 🚀 Milestone Checklist

- [x] Kafka Producer working and sending messages  
- [x] Kafka topic connection verified  
- [ ] Airflow DAG parses and transforms messages  
- [ ] PostgreSQL ingestion tested  
- [ ] Tableau visualization connected  
- [ ] Optional: ML-based predictions via Kafka loopback

---

## ⚠️ Notes

- Make sure `KAFKA_ADVERTISED_LISTENERS` is set to `PLAINTEXT://localhost:9092` for Mac/host access
- Running `docker-compose down -v` will also delete volumes (including PostgreSQL data)
- Use `docker exec` to test Kafka topic creation:  
  ```bash
  docker exec -it real_time_pipeline-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
  
## ⚠️ ✨ Future Plans
	•	Add ml_predictor.py that listens to cleaned data → runs model → sends predictions back to Kafka
	•	Join with user profile data for richer insight
	•	Use Apache Spark for scaling transformation logic

⸻

## ⚠️ 👤 Author

Suyeon Kim
Data Engineering Student | Ex-Java Backend Dev | Passionate about streaming systems, ETL, and ML
📍 Vancouver, Canada
🔗 LinkedIn