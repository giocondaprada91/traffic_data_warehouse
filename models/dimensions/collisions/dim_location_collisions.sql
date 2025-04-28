{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    INITCAP(borough) AS borough,
    LPAD(CAST(zip_code AS STRING), 5, '0') AS zip_code,
    INITCAP(on_street_name) AS on_street_name,
    INITCAP(cross_street_name) AS cross_street_name,
    INITCAP(off_street_name) AS off_street_name,
    SAFE_CAST(latitude AS FLOAT64) AS latitude,     
    SAFE_CAST(longitude AS FLOAT64) AS longitude 
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE borough IS NOT NULL
    OR zip_code IS NOT NULL
    OR on_street_name IS NOT NULL
    OR cross_street_name IS NOT NULL
    OR off_street_name IS NOT NULL
    OR latitude IS NOT NULL
    OR longitude IS NOT NULL
),

dummy_location AS (
  SELECT
    CAST('UNKNOWN' AS STRING) AS borough,
    CAST('00000' AS STRING) AS zip_code,
    CAST(NULL AS STRING) AS on_street_name,
    CAST(NULL AS STRING) AS cross_street_name,
    CAST(NULL AS STRING) AS off_street_name,
    CAST(NULL AS FLOAT64) AS latitude,
    CAST(NULL AS FLOAT64) AS longitude
)

SELECT
  GENERATE_UUID() AS location_id,
  borough,
  zip_code,
  on_street_name,
  cross_street_name,
  off_street_name,
  latitude,
  longitude
FROM base

UNION ALL

SELECT
  GENERATE_UUID() AS location_id,
  borough,
  zip_code,
  on_street_name,
  cross_street_name,
  off_street_name,
  latitude,
  longitude
FROM dummy_location