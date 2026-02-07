# Create both tables
CREATE OR REPLACE EXTERNAL TABLE `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_external`
OPTIONS (
 format = 'PARQUET',
 uris = ['gs://clear-variety-485314-u8-kestra-bucket/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_non_partitioned`
AS SELECT * FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_external`;

# Q1 

SELECT count(*) FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_external`;

# Q2 

SELECT count(distinct PULocationID) FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_external`;

SELECT count(distinct PULocationID) FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_non_partitioned`;

# Q3 

SELECT PULocationID FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_non_partitioned`;

SELECT PULocationID, DOLocationID FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_non_partitioned`;

# Q4 

SELECT count(*) FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_non_partitioned`
WHERE fare_amount = 0;

# Q5

CREATE OR REPLACE TABLE `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_non_partitioned`
);

# Q6 

SELECT distinct VendorID
FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_non_partitioned`
WHERE date(tpep_dropoff_datetime) >= '2024-03-01'
  AND date(tpep_dropoff_datetime) <= '2024-03-15';

SELECT distinct VendorID
FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_optimized`
WHERE date(tpep_dropoff_datetime) >= '2024-03-01'
  AND date(tpep_dropoff_datetime) <= '2024-03-15';

# Q9

SELECT count(*) FROM `clear-variety-485314-u8.zoomcamp_dwh.hw3_tripdata_non_partitioned`;