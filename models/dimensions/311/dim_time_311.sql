{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    closed_date
  FROM {{ ref('stg_traffic_signal_311') }}
  WHERE closed_date IS NOT NULL
),

parsed_times AS (
  SELECT
    SAFE.PARSE_TIME('%H:%M', closed_date) AS parsed_time
  FROM base
  WHERE closed_date != ''
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