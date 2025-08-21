import pandas as pd
import os
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from pytz import timezone
from tenacity import retry, stop_after_attempt, wait_exponential
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

# Set up logging
logging.basicConfig(filename='logs/pipeline_automated.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# PostgreSQL connection
try:
    conn = psycopg2.connect(
        dbname="sensor_data",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
except psycopg2.Error as e:
    logger.error(f"Database connection failed: {e}")
    raise

os.makedirs('quarantine', exist_ok=True)
os.makedirs('failed', exist_ok=True)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def execute_db_operation(cur, query, params):
    cur.execute(query, params)

def process_file(file_path):
    logger.info(f"Processing file: {file_path}")
    try:
        df = pd.read_csv(file_path, quotechar='"', na_values=['NaN'], skipinitialspace=True)
        
        key_columns = ['ts', 'device', 'temp']
        total_dropped = 0
        
        null_df = df[df[key_columns].isna().any(axis=1)]
        if not null_df.empty:
            null_count = len(null_df)
            quarantine_file_null = f"quarantine/invalid_{os.path.basename(file_path)}_null_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            null_df.to_csv(quarantine_file_null, index=False, quoting=1)
            logger.warning(f"Quarantined {null_count} rows due to nulls to {quarantine_file_null}")
            df = df.dropna(subset=key_columns)
            total_dropped += null_count

        df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
        type_failed_df = df[df['temp'].isna()]
        if not type_failed_df.empty:
            type_failed_count = len(type_failed_df)
            quarantine_file_type = f"quarantine/invalid_{os.path.basename(file_path)}_type_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            type_failed_df.to_csv(quarantine_file_type, index=False, quoting=1)
            logger.warning(f"Quarantined {type_failed_count} rows due to non-numeric temp to {quarantine_file_type}")
            df = df.dropna(subset=['temp'])
            total_dropped += type_failed_count

        range_failed_df = df[~((df['temp'] >= -50) & (df['temp'] <= 50))]
        if not range_failed_df.empty:
            range_failed_count = len(range_failed_df)
            quarantine_file_range = f"quarantine/invalid_{os.path.basename(file_path)}_range_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            range_failed_df.to_csv(quarantine_file_range, index=False, quoting=1)
            logger.warning(f"Quarantined {range_failed_count} rows with temp out of range to {quarantine_file_range}")
            df = df[(df['temp'] >= -50) & (df['temp'] <= 50)]
            total_dropped += range_failed_count

        df['ts'] = pd.to_datetime(df['ts']).dt.tz_localize('UTC')
        valid_rows = len(df)
        logger.info(f"Transformed and validated {valid_rows} rows (dropped {total_dropped}) invalid rows")

        conn.autocommit = False
        file_name = os.path.basename(file_path)
        execute_db_operation(cur, "INSERT INTO sensor_schema.files (file_name) VALUES (%s) ON CONFLICT (file_name) DO NOTHING RETURNING id", (file_name,))
        result = cur.fetchone()
        if result is None:
            logger.info(f"File {file_name} already exists, fetching existing ID")
            execute_db_operation(cur, "SELECT id FROM sensor_schema.files WHERE file_name = %s", (file_name,))
            result = cur.fetchone()
        file_id = result[0] if result else None
        if file_id is None:
            raise ValueError(f"No file_id found for {file_path}")
        df['file_id'] = file_id
        df['file_name'] = file_name
        df['ingested_at'] = datetime.now(timezone('UTC'))

        df['motion'] = df['motion'].astype(bool)
        df['light'] = df['light'].astype(bool)
        values = [tuple(x) for x in df[['file_name', 'ts', 'device', 'temp', 'humidity', 'co', 'lpg', 'smoke', 'motion', 'light', 'ingested_at', 'file_id']].values]
        try:
            execute_values(cur, """
                INSERT INTO sensor_schema.raw_sensor_data_partitioned (file_name, ts, device, temp, humidity, co, lpg, smoke, motion, light, ingested_at, file_id)
                VALUES %s
                ON CONFLICT ON CONSTRAINT raw_sensor_data_partitioned_pkey DO NOTHING
            """, values)
            inserted_rows = cur.rowcount
            logger.info(f"Inserted {inserted_rows} new raw rows for {file_path} (skipped duplicates)")
        except psycopg2.Error as e:
            logger.error(f"Insert error for {file_path}: {str(e)}")
            conn.rollback()
            return

        sensor_types = ['temp', 'humidity', 'co', 'lpg', 'smoke']
        aggregated_data = []
        processing_timestamp = datetime.now(timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S%z')
        for device in df['device'].unique():
            device_df = df[df['device'] == device]
            for stype in sensor_types:
                if stype in device_df.columns:
                    series = device_df[stype]
                    aggregated_data.append((
                        file_id, device, stype, series.min(), series.max(),
                        series.mean(), series.std(), processing_timestamp
                    ))
        agg_df = pd.DataFrame(aggregated_data, columns=['file_id', 'device', 'sensor_type', 'min_value', 'max_value', 'avg_value', 'std_value', 'processed_at'])
        logger.info(f"Aggregated metrics for {file_path}:\n{agg_df}")
        values_agg = [tuple(x) for x in agg_df.values]
        try:
            execute_values(cur, """
                INSERT INTO sensor_schema.aggregated_metrics (file_id, device, sensor_type, min_value, max_value, avg_value, std_value, processed_at)
                VALUES %s
                ON CONFLICT ON CONSTRAINT aggregated_metrics_pkey DO NOTHING
            """, values_agg)
            inserted_agg_rows = cur.rowcount
            logger.info(f"Inserted {inserted_agg_rows} new aggregated rows for {file_path} (skipped duplicates)")
        except psycopg2.Error as e:
            logger.error(f"Aggregated insert error for {file_path}: {str(e)}")
            conn.rollback()
            return

        conn.commit()
        logger.info(f"Committed transaction for {file_path}")
        os.remove(file_path)
        logger.info(f"Removed processed file: {file_path}")

    except Exception as e:
        logger.error(f"Failed to process {file_path}: {str(e)}")
        conn.rollback()
        os.makedirs('failed', exist_ok=True)
        os.rename(file_path, os.path.join('failed', os.path.basename(file_path)))

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path.endswith('.csv') and not event.is_directory:
            process_file(event.src_path)

if __name__ == '__main__':
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, path='data', recursive=False)
    observer.start()
    logger.info("Processing existing files...")
    for file_path in [os.path.join('data', f) for f in os.listdir('data') if f.endswith('.csv')]:
        process_file(file_path)
    logger.info("Pipeline started, monitoring data folder...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Pipeline stopped")