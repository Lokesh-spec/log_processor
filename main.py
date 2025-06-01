import logging
from scripts.parser import parse_apache_log_data
from scripts.loader import bq_loder
from utils.logger import setup_logger
from utils.config import APACHE_LOG_URL, STRUCTURED_LOG_FILE, BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE

logger = setup_logger('LogProcessorMain', log_level=logging.INFO)

if __name__ == '__main__':
    try:
        logger.info("Starting the log processing.")

        logger.info(f"Parsing log file from URL: {APACHE_LOG_URL}")
        apache_log_df = parse_apache_log_data(APACHE_LOG_URL)
        logger.info(f"Parsed {len(apache_log_df)} rows of log data.")

        logger.info(f"Saving structured log data to: {STRUCTURED_LOG_FILE}")
        apache_log_df.to_csv(STRUCTURED_LOG_FILE, index=False)

        logger.info(f"Log processing completed successfully. Output saved to {STRUCTURED_LOG_FILE}")

        logger.info(bq_loder(apache_log_df))

    
    except Exception as e:
        logger.error(f"An error occurred during log processing: {str(e)}")
