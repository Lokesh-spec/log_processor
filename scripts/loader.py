import os
import pandas as pd
import logging
from pandas import DataFrame
from google.cloud import bigquery
from utils.logger import setup_logger
from google.cloud.bigquery import SchemaField
from utils.config import BQ_CREDS_PATH, BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_CREDS_PATH

logger = setup_logger('LogProcessorMain', log_level=logging.INFO)

def bq_loder(apache_logs_df: DataFrame) -> str:
    try:
        apache_logs_df['timestamp'] = apache_logs_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S%z')

        apache_logs_df['timestamp'] = apache_logs_df['timestamp'].str.replace(r'(\+|\-)(\d{2})(\d{2})$', r'\1\2:\3', regex=True)

        apache_logs_df['size'] = pd.to_numeric(apache_logs_df['size'], errors='coerce').fillna(0).astype(int)

        client = bigquery.Client()

        table_id = f'{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}'

        schema = [
            SchemaField('host', 'STRING'),
            SchemaField('identity', 'STRING'),
            SchemaField('user', 'STRING'),
            SchemaField('timestamp', 'TIMESTAMP'),
            SchemaField('method', 'STRING'),
            SchemaField('endpoint', 'STRING'),
            SchemaField('protocol', 'STRING'),
            SchemaField('status_code', 'INTEGER'),
            SchemaField('size', 'INTEGER')
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition='WRITE_TRUNCATE'  
        )

        rows_to_insert = apache_logs_df.to_dict(orient='records')

        load_job = client.load_table_from_json(rows_to_insert, table_id, job_config=job_config)

        load_job.result()

        logger.info(f"Successfully loaded {len(rows_to_insert)} rows to {table_id}")
        return "SUCCESS"
    
    except Exception as e:
        logger.error(f"Failed to load data into BigQuery: {e}")
        return "FAILED"
