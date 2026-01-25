--For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' 
--and '2025-12-01', exclusive of the upper bound), 
--how many trips had a trip_distance of less than or equal to 1 mile?

select count(*) from green_tripdata_2025_11 
where date(lpep_pickup_datetime) >= '2025-11-01'
and date(lpep_pickup_datetime) < '2025-12-01'
and trip_distance <= 1
and trip_distance is not null
and lpep_pickup_datetime is not null;
-- 8007

--Which was the pick up day with the longest trip distance? 
--Only consider trips with trip_distance less than 100 miles (to exclude data errors).

select 
	  date(lpep_pickup_datetime)
	, max(trip_distance)
from green_tripdata_2025_11 
where trip_distance < 100
group by 1
order by 2 desc;
--2025-11-14

--Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
select zones."Zone", sum(trips.total_amount) as sum_of_all_trips
from green_tripdata_2025_11 trips
inner join taxi_zone_lookup zones
 on trips."PULocationID" = zones."LocationID"
where date(lpep_pickup_datetime) = '2025-11-18'
group by 1
order by 2 desc;
-- East Harlem North

SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'green_tripdata_2025_11'; 

--For the passengers picked up in the zone named "East Harlem North" in November 2025, 
-- which was the drop off zone that had the largest tip?
-- Note: it's tip , not trip. We need the name of the zone, not the ID.
select 
  trips."DOLocationID", dozones."Zone", max(tip_amount) as largest_single_tip
from green_tripdata_2025_11 trips
left join taxi_zone_lookup puzones
 on trips."PULocationID" = puzones."LocationID"
left join taxi_zone_lookup dozones
 on trips."DOLocationID" = dozones."LocationID"
where date(lpep_pickup_datetime) >= '2025-11-01'
  and date(lpep_pickup_datetime) < '2025-12-01'
  and puzones."Zone" = 'East Harlem North'
  and puzones."Zone" is not null
  and dozones."Zone" is not null
group by 1, 2
order by 3 desc;