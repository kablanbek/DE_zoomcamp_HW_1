public.green_taxi_dataselect min(lpep_pickup_datetime), max(lpep_pickup_datetime) from public.green_taxi_data

select * from public.green_taxi_data

select * from public.taxi_zone_data


-- #2.
SELECT
	COUNT(DISTINCT index)
FROM
	public.green_taxi_data
WHERE
	trip_distance <= 1
	AND
	DATE_PART('month',lpep_pickup_datetime) = 11


-- #3.
SELECT
	lpep_pickup_datetime::DATE
FROM
	public.green_taxi_data
WHERE
	trip_distance < 100
ORDER BY
	trip_distance DESC
LIMIT 1

-- #4. 
SELECT
	"Zone",
	COUNT(t1."index") AS counter
FROM
	public.green_taxi_data AS t1
	LEFT JOIN
	public.taxi_zone_data AS t2
	ON
	t1."PULocationID" = t2."LocationID"
GROUP BY
	"Zone"
ORDER BY
	counter DESC
LIMIT 1


-- #5. 
SELECT
	t5."Zone", *
FROM
(
SELECT
	*
FROM
	(
	SELECT
		*
	FROM
		public.green_taxi_data AS t1
		LEFT JOIN
		public.taxi_zone_data AS t2
		ON
		t1."PULocationID" = t2."LocationID"
	) AS t3
	WHERE
		"Zone" = 'East Harlem North'
	ORDER BY
		"tip_amount" DESC
	LIMIT 1
	) AS t4
	LEFT JOIN
	public.taxi_zone_data AS t5
	ON
	t4."DOLocationID" = t5."LocationID"


