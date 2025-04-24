{{ config(materialized='table') }}

-- Check if collision_id always maps to one date/time pair
SELECT
  collision_id,
  COUNT(DISTINCT crash_date) AS num_dates,
  COUNT(DISTINCT crash_time) AS num_times
FROM
  `traffic-warehouse.raw_data.motor_vehicle_collisions`
GROUP BY
  collision_id
HAVING
  num_dates > 1 OR num_times > 1
