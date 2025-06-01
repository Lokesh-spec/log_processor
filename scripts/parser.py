import re
import logging
import requests
import warnings
import pandas as pd
from pandas import DataFrame
from utils.logger import setup_logger
from urllib3.exceptions import NotOpenSSLWarning

warnings.simplefilter("ignore", NotOpenSSLWarning)

# Set up logger
logger = setup_logger('LogProcessorParser', log_level=logging.DEBUG)

def parse_apache_log_data(url: str) -> DataFrame:
    logger.info(f"Fetching logs from: {url}")
    response = requests.get(url)

    if response.status_code == 200:
        try:
            logger.info("Logs fetched successfully!")
            apache_log_response = response.text 
        except requests.exceptions.JSONDecodeError:
            apache_log_response = response.text
        except Exception as e:
            logger.error(f"Failed to fetch logs, status code: {response.status_code}")
            return None
    else:
        logger.error(f"Failed to fetch logs, status code: {response.status_code}")
        return None

    log_pattern = r'(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)'

    log_data_list = []

    for log_line in apache_log_response.split('\n'):
        match = re.match(log_pattern, log_line)

        if match:
            log_data = {
                'host': match.group(1),
                'identity': match.group(2),
                'user': match.group(3) if match.group(3) != '-' else 'Unknown',
                'timestamp': match.group(4),
                'method': match.group(5),
                'endpoint': match.group(6),
                'protocol': match.group(7),
                'status_code': match.group(8),
                'size': match.group(9)
            }
            log_data_list.append(log_data)
        else:
            # Fixing the logging issue
            logger.info(f'No Match Found for: {log_line}')
        
    apache_logs_df = pd.DataFrame(log_data_list)

    # Replace dash ('-') with 0 in 'size' and 'status_code'
    apache_logs_df['size'] = apache_logs_df['size'].replace('-', '0')
    apache_logs_df['status_code'] = apache_logs_df['status_code'].replace('-', '0')

    # Convert columns to appropriate types
    apache_logs_df = apache_logs_df.astype({
        'host': 'str', 
        'identity': 'str',
        'user': 'str',
        'method': 'str',
        'endpoint': 'str',
        'protocol': 'str',
        'status_code': 'int',
        'size': 'int'
    })

    # Parse the 'timestamp' column to datetime
    timestamp_format = "%d/%b/%Y:%H:%M:%S %z"
    apache_logs_df['timestamp'] = pd.to_datetime(apache_logs_df['timestamp'], format=timestamp_format, errors='coerce')

    # Check for any failed timestamp parsing
    if apache_logs_df['timestamp'].isnull().any():
        logger.info("Warning: Some timestamps couldn't be parsed correctly.")

    return apache_logs_df
