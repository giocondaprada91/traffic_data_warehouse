{{ config(materialized='table') }}

-- See how many boroughs each ZIP is linked to
SELECT
  CAST(zip_code AS STRING) AS zip,
  COUNT(DISTINCT borough) AS borough_count
FROM
  `traffic-warehouse.raw_data.motor_vehicle_collisions`
WHERE
  borough IS NOT NULL AND zip_code IS NOT NULL
GROUP BY
  zip
HAVING
  borough_count > 1