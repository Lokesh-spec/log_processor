# 🧾 Apache Log Processor

## 🚀 Overview

This project is a lightweight log processing pipeline that parses raw Apache server logs into structured CSV format using Python. It’s the first part of a full data engineering pipeline, intended for ingesting log data into BigQuery for analytics.

> ✅ **Built as part of a Data Engineering interview prep series (Week 1)**

---

## 🏗️ Features

- Parses standard Apache HTTP access logs
- Extracts fields such as IP, datetime, HTTP method, endpoint, protocol, status code, and response size
- Outputs cleaned data as CSV
- Modular and reusable code
- Ideal for ETL pipelines and cloud-based ingestion (e.g., BigQuery)

---

## 🧠 How It Works

1. `main.py` reads a raw Apache log file (`apache_logs.log`)
2. It uses `parse.py` to extract the following fields:
   - `host` – IP address of the requester
   - `timestamp` – Date and time of the request
   - `method` – HTTP method used (e.g., GET, POST)
   - `endpoint` – URL endpoint accessed
   - `protocol` – HTTP version
   - `status_code` – HTTP response status
   - `size` – Response size in bytes
3. The parsed data is written to `cleaned_logs.csv`

---

## 🧪 Sample Output

Example row in the `cleaned_logs.csv`:

```csv
host,timestamp,method,endpoint,protocol,status_code,size
127.0.0.1,2024-05-29 09:23:47,GET,/index.html,HTTP/1.1,200,1024
```

--- 

## ⚙️ Installation & Usage

🔧 1. Clone the repository

``` bash
git clone https://github.com/Lokesh-spec/log_processor.git
cd log_processor
```

📦 2. Install dependencies
``` bash
pip install -r requirements.txt
```

▶️ 3. Run the parser
```
python main.py
```

Make sure your apache_logs.log file is in the root folder or adjust the path accordingly.

💡 Use Case

This processor is ideal for:
	•	Preprocessing logs before ingestion into data warehouses (e.g., BigQuery)
	•	Building dashboards for traffic analysis, bot detection, and error monitoring
	•	Teaching log parsing and pipeline design in data engineering