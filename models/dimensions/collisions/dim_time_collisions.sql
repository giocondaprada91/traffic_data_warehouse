{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    crash_time
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE crash_time IS NOT NULL
),

parsed_times AS (
  SELECT
    SAFE.PARSE_TIME('%H:%M', crash_time) AS parsed_time
  FROM base
  WHERE crash_time != ''
)

SELECT
  -- Surrogate Key (make it clean)
  SAFE_CAST(FORMAT_TIME('%H%M', parsed_time) AS INT64) AS time_id,

  -- Natural fields
  parsed_time AS time_value,
  EXTRACT(HOUR FROM parsed_time) AS hour,
  EXTRACT(MINUTE FROM parsed_time) AS minute
FROM parsed_times
WHERE parsed_time IS NOT NULL
