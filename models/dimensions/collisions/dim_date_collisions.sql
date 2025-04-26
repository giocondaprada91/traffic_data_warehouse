{{ config(materialized='table') }}

WITH base AS (
  SELECT
    DISTINCT crash_date
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE crash_date IS NOT NULL
)

SELECT
  -- Surrogate Key
  SAFE_CAST(FORMAT_DATE('%Y%m%d', crash_date) AS INT64) AS date_id,

  -- Natural fields
  crash_date AS date_value,
  EXTRACT(YEAR FROM crash_date) AS year,
  EXTRACT(MONTH FROM crash_date) AS month,
  EXTRACT(DAY FROM crash_date) AS day,

FROM base
