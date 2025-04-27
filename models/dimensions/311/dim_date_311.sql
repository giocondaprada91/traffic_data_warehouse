{{ config(materialized='table') }}

WITH base AS (
  SELECT
    DISTINCT closed_date
  FROM {{ ref('stg_traffic_signal_311') }}
  WHERE closed_date IS NOT NULL
)

SELECT
  -- Surrogate Key
  SAFE_CAST(FORMAT_DATE('%Y%m%d', closed_date) AS INT64) AS date_id,

  -- Natural fields
  closed_date AS date_value,
  EXTRACT(YEAR FROM closed_date) AS year,
  EXTRACT(MONTH FROM closed_date) AS month,
  EXTRACT(DAY FROM closed_date) AS day,

FROM base