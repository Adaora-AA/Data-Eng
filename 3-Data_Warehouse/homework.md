# Module 3 Homework

<b><u>Important Note:</b></u> <p> For this homework we will be using the Yellow Taxi Trip Records for **January 2024 - June 2024 NOT the entire year of data** 
Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>

## Inserting data manually:

a: Create a yellow_2024 bucket in Google Cloud Storage

b: Upload parquet files to the bucket

c: Create external table for 2024

Run this query in Big Query:

```sql

CREATE OR REPLACE EXTERNAL TABLE adaora.ny_dataset.external_yellow_2024
OPTIONS(
  FORMAT='PARQUET',
  URIS=['gs://adaoraah-terra-bucket/*']

);

```

d: Create native table for 2024

```sql

CREATE OR REPLACE TABLE adaora.ny_dataset.native_yellow_2024
AS(
  SELECT * FROM `adaora.ny_dataset.external_yellow_2024`
);

```


## Question 1:

Question 1: What is count of records for the 2024 Yellow Taxi Data?

```sql

select count(1) from `adaora.ny_dataset.native_yellow_2024`;

```

Answer: 20,332,093


## Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?
Type this query:

```sql
SELECT COUNT(DISTINCT PULocationID)
FROM `adaora.ny_dataset.external_yellow_2024`;


SELECT COUNT(DISTINCT PULocationID)
FROM `adaora.ny_dataset.native_yellow_2024`;
 ```

Answer:  0 MB for the External Table and 155.12 MB for the Materialized Table

 ## Question 3:

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

 ```sql

SELECT PULocationID
FROM `adaora.ny_dataset.native_yellow_2024`;


SELECT PULocationID, DOLocationID
FROM `adaora.ny_dataset.native_yellow_2024`;
 ```

Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


## Question 4:

How many records have a fare_amount of 0?


```sql

SELECT COUNT(1)
FROM `adaora.ny_dataset.native_yellow_2024`
WHERE fare_amount = 0;
```

Answer: 8,333

## Question 5:

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
```sql

CREATE OR REPLACE TABLE adaora.ny_dataset.partitioned_yellow_2024
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
AS(
  SELECT * FROM `adaora.ny_dataset.native_yellow_2024`
);

```
Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID



## Question 6:

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

```sql

SELECT DISTINCT VendorID
FROM `adaora.ny_dataset.native_yellow_2024`
WHERE tpep_pickup_datetime >= '2024-03-01'
AND tpep_pickup_datetime <= '2024-03-15';


SELECT DISTINCT VendorID
FROM `adaora.ny_dataset.partitioned_yellow_2024`
WHERE tpep_pickup_datetime >= '2024-03-01'
AND tpep_pickup_datetime <= '2024-03-15';
```
Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

## Question 7:

Where is the data stored in the External Table you created?

Answer: GCP Bucket

## Question 8:

It is best practice in Big Query to always cluster your data:

Answer: False

## Question 9: (Bonus: Not worth points)

No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```sql

SELECT count (1)
FROM `adaora.ny_dataset.native_yellow_2024`;
```
Answer: This query will process 0 B when run </br> 
This is because the number of rows is stored in. So when you count all, it doesn't read the whole table instead, it gets the count from its metadata.