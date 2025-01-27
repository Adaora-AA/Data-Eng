# Prepare Postgres

Run Postgres and load data as shown in the videos using the green taxi trips from January 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

as well as the zones dataset:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

#### Run Docker compose
```bash
docker compose up
```

#### Create an script that ingests both files called ingest_homework.py and dockerize it with
```bash 
docker build -t homework:v001 .
```

#### Find the network where the docker-compose containers are running with
```bash 
docker network ls 
network=adaora_default
```

#### Finally, run the dockerized script
```bash
URL1="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
URL2="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

docker run -it \
--network=adaora_default \
homework:v001 \
--user=ada \
--password=adaroot \
--host=mydatabase \
--port=5432 \
--db=homework \
--table_name1=green_taxi_trips \
--table_name2=zones \
--url1="${URL1}" \
--url2="${URL2}"
```

> # Question 3 & 4: Trip Segmentation Count & Longest trip for each day

```sql
WITH trip AS (
	SELECT
	*,
	CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
	CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc",
	CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
	CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date
FROM
     green_taxi_trips t JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
)
```

#### Q3: During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
	1. Up to 1 mile
	2. In between 1 (exclusive) and 3 miles (inclusive),
	3. In between 3 (exclusive) and 7 miles (inclusive),
	4. In between 7 (exclusive) and 10 miles (inclusive),
	5. Over 10 miles

```sql
SELECT 
CASE 
WHEN trip_distance <= 1 THEN 'up to 1 mile'
WHEN trip_distance > 1 and trip_distance <= 3 THEN 'between 1 (exclusive) and 3 miles (inclusive)'
WHEN trip_distance > 3 and trip_distance <= 7 THEN 'between 3 (exclusive) and 7 miles (inclusive)'
WHEN trip_distance > 7 and trip_distance <= 10 THEN 'between 7 (exclusive) and 10 miles (inclusive)'
ELSE 'Over 10 miles'
END as trip_segmentation_cat, 
COUNT(1) as trip_segmentation_count
FROM trip
WHERE (pickup_date >='2019-10-01' AND pickup_date < '2019-11-01') AND (dropoff_date >='2019-10-01' AND dropoff_date < '2019-11-01')
GROUP BY trip_segmentation_cat;

-- Answer: "104,802; 198,924; 109,603; 27,678; 35,189"
```

#### Q4: Which was the pick up day with the longest trip distance? Use the pick up time for your calculations. Tip: For every trip on a single day, we only care about the trip with the longest distance.

```sql
SELECT pickup_date
FROM (
    SELECT pickup_date, MAX(trip_distance) AS M
    FROM trip
    GROUP BY pickup_date
    ORDER BY M DESC
) AS A
LIMIT 1;

-- Answer: "2019-10-31"
```

> # Question 5:  Three biggest pickup zones

```sql
with trip AS
(SELECT
	*,
	zpu."Zone" AS "pickup_loc",
	zdo."Zone" AS "dropoff_loc",
	CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
	CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date
FROM
     green_taxi_trips t JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
)
```

#### Q5: Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18? Consider only lpep_pickup_datetime when filtering by date.
```sql
 SELECT pickup_loc, SUM(total_amount)
 FROM trip
 WHERE pickup_date = '2019-10-18'
 GROUP BY 1
 ORDER BY 2 DESC;

-- Answer: "East Harlem North, East Harlem South, Morningside Heights"
```

> # Question 6:  Largest tip

```sql
with trip AS
(SELECT
	*,
	zpu."Zone" AS "pickup_loc",
	zdo."Zone" AS "dropoff_loc",
	CAST(lpep_pickup_datetime AS DATE) AS pickup_date,
	CAST(lpep_dropoff_datetime AS DATE) AS dropoff_date
FROM
     green_taxi_trips t JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
)
```

#### Q6: For the passengers picked up in October 2019 in the zone name "East Harlem North" which was the drop off zone that had the largest tip?  We want the name of the zone, not the id. Note: it's not a typo, it's `tip` , not `trip` We need the name of the zone, not the ID.
```sql
 SELECT dropoff_loc, MAX(tip_amount)
 FROM trip
 WHERE pickup_loc = 'East Harlem North'
 GROUP BY 1
 ORDER BY 2 DESC;
 
-- Answer: "JFK Airport"
```