# Sensor Data Pipeline

An automated **real-time sensor data pipeline** built in Python that ingests CSV sensor files, validates and transforms records, stores them in a relational database, and computes aggregated metrics for analytics.

---

##  Features
-  Watches a folder for incoming **CSV sensor data** (streaming-like ingestion).
-  Validates each record (null checks, sensor ranges).
-  Stores **raw sensor data** into a partitioned MySQL table.
-  Computes aggregated metrics per device & sensor:
-  Minimum, Maximum, Average, Standard Deviation.
-  Fault tolerance with **retry mechanism** (`tenacity`).
-  Quarantines invalid/corrupt files into a separate folder.
-  Logs all pipeline activity to a rotating log file.

---

##  Architecture Overview
```text
CSV Files --> Pipeline Watcher --> Validation --> MySQL (raw data)
                                   |--> Quarantine (invalid)
                                   |--> Aggregated Metrics (analytics)

# Ingestion Layer: Monitors data/ folder for new CSV files.

# Validation Layer: Applies schema + value checks (temperature range, nulls).

# Storage Layer: Writes raw data into raw_sensor_data_partitioned (partitioned by timestamp).

# Analytics Layer: Updates aggregated_metrics with min/max/avg/stddev.

# Error Handling: Retries transient failures, quarantines invalid rows/files, logs all activity.
```

## Setup Instructions
1. Clone Repository
```bash
git clone https://github.com/Jyothi-Surla/Data-Pipeline.git
cd Data-Pipeline
```

2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate      
venv\Scripts\activate
```

3. Install Dependencies
```bash
 pip install -r requirements.txt
```

4. Initialize Database
-Start a MySQL instance (local or Docker).
-Create schema using:
```bash
mysql -u <user> -p < db_name < sql/sensor_data_schema.sql
 ```

5. Run the Pipeline
```bash
mkdir -p data quarantine failed logs
python pipeline_automated.py
``` 

-Drop CSVs into data/ → pipeline processes automatically.
-Invalid rows → quarantine/
-Corrupt files → failed/
-Logs → logs/pipeline_automated.log

## Database Schema
Defined in sql/sensor_data_schema.sql

-files → metadata of ingested files
-raw_sensor_data_partitioned → raw data (partitioned by timestamp)
-aggregated_metrics → per-device & per-sensor metrics

## Scalability Considerations
For production use, the following extensions are recommended:

-Kafka / RabbitMQ → real-time ingestion at scale.
-Airflow / Luigi → workflow orchestration & scheduling.
-Docker / Kubernetes → containerized, distributed deployment.
-Spark / Flink → large-scale streaming data processing.
-Prometheus + Grafana → monitoring & alerting.
More details in - Scalability Considerations file.

## Repository Structure
```bash
├── pipeline_automated.py       # Main pipeline script
├── requirements.txt            # Python dependencies
├── sensor_data_schema.sql      # MySQL schema (DDL)
├── Data Pipeline Documentation.pdf
├── Scalability Considerations.pdf
├── .gitignore
└── README.md
```

