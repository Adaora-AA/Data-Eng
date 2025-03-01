# Module 5 Homework

## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version

What's the output?

```
3.3.2
```

## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe. 

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

```python

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName('test').getOrCreate()

df = spark.read.parquet('yellow_tripdata_2024-10.parquet')
df = df.repartition(4)
df.write.parquet('homework/pq')

```

Answer: 

```
25MB
```


## Question 3: Count records

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.


```sql

df =df \
    .withColumn("pickup_date", F.to_date(df.tpep_pickup_datetime)) \
    .show() 

df.registerTempTable('trips_1024')  

spark.sql("""

SELECT count(*) FROM trips_1024 
WHERE DATE(pickup_date) = '2024-10-15';
""").show()

```

Answer:

```
128097   
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

```python

spark.sql("""
SELECT 
    (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600 AS trip_duration
FROM trips_1024
ORDER BY trip_duration DESC
LIMIT 5;
""").show()                  

```

Answer: 

```
162
```

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

Answer

```
4040
```

## Question 6: Least frequent pickup location zone

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?


```python

lookup_df = spark.read.option("header", "true").csv('taxi_zone_lookup.csv')

lookup_df.registerTempTable('lookup_df')  

spark.sql("""

SELECT lookup_df.Zone , count(*) as count_trips FROM trips_1024 
INNER JOIN lookup_df ON lookup_df.LocationID = trips_1024.PULocationID
GROUP BY lookup_df.Zone
ORDER BY count_trips ASC;
""").show()  

```

Answer:

```
Governor's Island/Ellis Island/Liberty Island
```