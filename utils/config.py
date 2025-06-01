import os


APACHE_LOG_URL = os.getenv("LOG_FILE_URL", "https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs")
STRUCTURED_LOG_FILE = os.getenv("STRUCTURED_LOG_FILE", "./output/structured_logs.csv")

BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "robust-analyst-454716-n2")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "apache_logs")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "logs")
BQ_CREDS_PATH = os.getenv("BQ_CREDS_PATH", "/Users/lokeshkv/Access-token/robust-analyst-454716-n2-b33d3fc69407.json")