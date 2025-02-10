> # Question 1: Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

```
Answer: --rm
```

> # Question 2: Understanding docker first run

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0
- 1.0.0
- 23.0.1
- 58.1.0

```
Answer: 0.45.1
```


# Prepare Postgres

Run Postgres and load data as shown in the videos We'll use the green taxi trips from January 2019:

```bash
wget wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
```
You will also need the dataset with zones:
```bash 
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```
Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

> ### Setting up the DB:
```bash
# run Docker compose
docker compose up

# Create an script that ingests both files called ingest_homework.py and dockerize it with
docker build -t homework:04 .

# Find the network where the docker-compose containers are running with
docker network ls 
network=adaora_default

# Finally, run the dockerized script
URL1="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"
URL2="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

docker run -it \
--network=adaora_default \
homework:04 \
--user=ada \
--password=adaroot \
--host=mydatabase \
--port=5432 \
--db=2023_homework \
--table_name1=green_taxi_data_0919 \
--table_name2=zones \
--url1="${URL1}" \
--url2="${URL2}"
```

> # Question 3 & 4: Count records & Longest trip for each day

```sql
WITH trip AS (
	SELECT
	*,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc",
	CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
	CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date
FROM
     green_taxi_data_0919 t JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
)

-- Q3: How many taxi trips were totally made on September 18th 2019? Tip: started and finished on 2019-09-18. Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.
 SELECT count(1)
 FROM trip
 WHERE pickup_date ='2019-09-18' AND dropoff_date = '2019-09-18';
-- Answer: "15612 trips"

-- Q4: Which was the pick up day with the longest trip distance? Use the pick up time for your calculations. Tip: For every trip on a single day, we only care about the trip with the longest distance.

 SELECT pickup_date,max(trip_distance) as M
 FROM trip
 GROUP BY 1
 ORDER BY 2 DESC
-- Answer: "2019-09-26"

```

# Question 5:  Three biggest pick up Boroughs

```sql
with trips AS
(SELECT
	*,
	zpu."Zone" AS "pickup_loc",
	zpu."Borough" AS "pickup_bou",
	zdo."Zone" AS "dropoff_loc",
	CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
	CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date
FROM
     green_taxi_data_0919 t JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
)

-- Q5:Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown. Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 SELECT pickup_bou, SUM(total_amount)
 FROM trips
 WHERE pickup_date = '2019-09-18' AND pickup_bou != 'Unknown'
 GROUP BY 1
 ORDER BY 2 DESC;
-- Answer: "Brooklyn" ,"Manhattan" ,"Queens"
```

> # Question 6:  Largest tip

```sql
with trips AS
(SELECT
	*,
	zpu."Zone" AS "pickup_loc",
	zdo."Zone" AS "dropoff_loc",
	CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
	CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date
FROM
     green_taxi_data_0919 t JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
)

-- Q6:For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?  We want the name of the zone, not the id. Note: it's not a typo, it's `tip` , not `trip`  - Central Park

 SELECT dropoff_loc, MAX(tip_amount)
 FROM trips
 WHERE pickup_loc = 'Astoria'
 GROUP BY 1
 ORDER BY 2 DESC;
-- Answer: "JFK Airport"
```