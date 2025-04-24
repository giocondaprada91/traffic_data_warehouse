{{ config(materialized='table') }}

-- Are there duplicate rows with the same key details?
SELECT
  collision_id,
  crash_date,
  crash_time,
  borough,
  COUNT(*) AS duplicate_count
FROM
  `traffic-warehouse.raw_data.motor_vehicle_collisions`
GROUP BY
  collision_id, crash_date, crash_time, borough
HAVING
  duplicate_count > 1
