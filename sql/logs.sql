CREATE OR REPLACE TABLE `robust-analyst-454716-n2.apache_logs.logs` (
  `host` STRING,
  `identity` STRING,
  `user` STRING,
  `timestamp` DATETIME,
  `method` STRING,
  `endpoint` STRING,
  `protocol` STRING,
  `status_code` INT64,
  `size` INT64
);