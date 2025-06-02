-- 1. Top IP Addresses
-- Identify the top 10 IP addresses that made the most requests to the server.
SELECT host, count(*) requests
  FROM `robust-analyst-454716-n2.apache_logs.logs`
 GROUP BY host
 ORDER BY requests DESC
 LIMIT 10

-- 2. Peak Traffic Periods
-- Find the hour(s) of the day when traffic to the server is highest.
WITH extracted_datetime AS (
  SELECT 
    CAST(EXTRACT(DATE FROM TIMESTAMP) AS STRING) AS request_date,
    CAST(EXTRACT(HOUR FROM TIMESTAMP) AS STRING) AS request_hour  
  FROM `robust-analyst-454716-n2.apache_logs.logs`
),

hourly_request_counts AS (
  SELECT 
    request_date, 
    request_hour,
    COUNT(*) AS total_requests
  FROM extracted_datetime
  GROUP BY request_date, request_hour
),

ranked_hourly_traffic AS (
  SELECT 
    request_date, 
    request_hour, 
    total_requests,
    ROW_NUMBER() OVER (
      PARTITION BY request_date 
      ORDER BY total_requests DESC
    ) AS rank_per_day
  FROM hourly_request_counts
)

SELECT 
  request_date, 
  request_hour, 
  total_requests
FROM ranked_hourly_traffic
WHERE rank_per_day = 1;


-- 3. Response Code Distribution
-- Analyze the distribution of HTTP status codes (e.g., 200, 404, 500) across all requests.

WITH status_code_counts AS (
  SELECT 
    status_code, 
    COUNT(*) AS request_count
  FROM `robust-analyst-454716-n2.apache_logs.logs`
  GROUP BY status_code
),
total_requests AS (
  SELECT COUNT(*) AS overall_count
  FROM `robust-analyst-454716-n2.apache_logs.logs`
)

SELECT 
  s.status_code, 
  s.request_count, 
  ROUND(s.request_count / CAST(t.overall_count AS FLOAT64), 4) AS distribution
FROM status_code_counts s
CROSS JOIN total_requests t
ORDER BY distribution DESC;


-- 4. Requests Over Time

-- Plot daily request volume to observe traffic trends over time.
SELECT 
  TIMESTAMP_TRUNC(TIMESTAMP, HOUR) AS request_hour,
  COUNT(*) AS request_count
FROM `robust-analyst-454716-n2.apache_logs.logs`
GROUP BY request_hour
ORDER BY request_hour;


-- 5. URL Access by IP

-- For a given IP address, list all distinct URLs accessed.
SELECT 
  host,
  STRING_AGG(DISTINCT endpoint, ' | ') AS url_list
FROM `robust-analyst-454716-n2.apache_logs.logs`
GROUP BY host;


-- 6. Average Response Size

-- Calculate the average response size (in bytes) by status code..

SELECT status_code,
       ROUND(AVG(size), 2) avg_response_size
  FROM `robust-analyst-454716-n2.apache_logs.logs`
 GROUP BY status_code


-- 7. Status Code by URL

-- For each URL, get the count of each response code it returned (e.g., URL health check).

SELECT endpoint,
       status_code,
       COUNT(status_code) count_status_code
  FROM `robust-analyst-454716-n2.apache_logs.logs`
 GROUP BY endpoint, status_code


-- 8. Bot Detection (Heuristic)

-- Flag IPs that made more than X requests per minute — potential bots.

WITH per_minute_requests AS (
  SELECT 
    host,
    TIMESTAMP_TRUNC(TIMESTAMP, MINUTE) AS request_minute,
    COUNT(*) AS requests_per_minute
  FROM `robust-analyst-454716-n2.apache_logs.logs`
  GROUP BY host, request_minute
),

suspicious_ips AS (
  SELECT 
    host,
    MAX(requests_per_minute) AS peak_rpm  
  FROM per_minute_requests
  GROUP BY host
  HAVING peak_rpm > 30  
)

SELECT * FROM suspicious_ips
ORDER BY peak_rpm DESC;