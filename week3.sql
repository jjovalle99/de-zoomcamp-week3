-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-413414.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc-green-taxi/green_tripdata_2022-*.parquet']
);

-- Q1: Create a non partitioned table from external table
CREATE OR REPLACE TABLE terraform-demo-413414.ny_taxi.green_tripdata_non_partitioned AS
SELECT * FROM terraform-demo-413414.ny_taxi.external_green_tripdata;

SELECT * FROM terraform-demo-413414.ny_taxi.external_green_tripdata LIMIT 10;
SELECT count(VendorID) from `terraform-demo-413414.ny_taxi.green_tripdata_non_partitioned`; -- 840402


-- Q2: distinct PULocationIDs
SELECT COUNT(DISTINCT PULocationID) FROM terraform-demo-413414.ny_taxi.external_green_tripdata; --258

SELECT COUNT(DISTINCT PULocationID) FROM terraform-demo-413414.ny_taxi.green_tripdata_non_partitioned; --258


-- Q3: fare_amount 0
SELECT COUNT(VendorID)
FROM `terraform-demo-413414.ny_taxi.green_tripdata_non_partitioned`
WHERE fare_amount = 0; --1622

-- Q4 Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE OR REPLACE TABLE `terraform-demo-413414.ny_taxi.green_tripdata_partitioned_clustered`
partition by date(lpep_pickup_datetime)
cluster by PUlocationID as
SELECT * FROM `terraform-demo-413414.ny_taxi.green_tripdata_non_partitioned`;

-- Q5
SELECT DISTINCT PULocationID
FROM `terraform-demo-413414.ny_taxi.green_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'; --12.82

SELECT DISTINCT PULocationID
FROM `terraform-demo-413414.ny_taxi.green_tripdata_partitioned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'; --1.12

-- Q6 gcp bucket
-- Q7 false
-- Q8 
select count(*)
from `terraform-demo-413414.ny_taxi.green_tripdata_non_partitioned`
