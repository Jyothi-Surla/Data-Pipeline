# Sensor Data Pipeline

An automated **real-time sensor data pipeline** in Python that watches a folder for CSV files, validates and transforms records, stores raw data in PostgreSQL (time-partitioned), and computes aggregated metrics for analytics. Built for **fault tolerance, performance, and scalability**.

---

## Quickstart

### Requirements
- Python 3.10+
- PostgreSQL 13+
- Install dependencies:
  ```bash
  pip install pandas psycopg2-binary watchdog tenacity


