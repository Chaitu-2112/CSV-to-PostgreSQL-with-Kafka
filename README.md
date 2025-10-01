# CSV-to-PostgreSQL-with-Kafka


This project demonstrates a **data pipeline in Java** that ingests data from a **CSV file**, publishes it to a **Kafka topic**, consumes it, and then stores it in a **PostgreSQL database** with reliable offset tracking.

---

## 🚀 Features
- Reads data from a **CSV file** (e.g., employee records).
- Publishes each row as a Kafka message (Producer).
- Kafka Consumer:
  - Inserts rows into PostgreSQL.
  - Tracks offsets in a database table for **fault tolerance**.
  - Resumes from the last committed offset after restart (no duplicates, no data loss).
- Simulates a **controlled stop** after the 4th record → verifies recovery on rerun.

---

## 🛠️ Tech Stack
- **Java** (Kafka client API, JDBC)
- **Apache Kafka**
- **PostgreSQL**
- **CSV File Handling** (FileReader + JSON)

---

## 📂 Project Structure
/src
└── org.example
├── Producer.java # Reads CSV and pushes rows to Kafka
├── Consumer.java # Consumes from Kafka, inserts into PostgreSQL
└── employees.csv # Input data


---

## ⚡ How to Run

### 1. Setup
- Start **Kafka** (Zookeeper + Kafka broker).
- Start **PostgreSQL** and create required tables:

```sql
-- For storing employee data
CREATE TABLE IF NOT EXISTS employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(100),
    salary NUMERIC
);

-- For tracking consumer offsets
CREATE TABLE IF NOT EXISTS consumer_offsets (
    topic VARCHAR(255),
    partition_id INT,
    last_offset BIGINT,
    PRIMARY KEY (topic, partition_id)
);

```
---




