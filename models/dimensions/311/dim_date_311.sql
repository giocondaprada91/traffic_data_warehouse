{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    created_date AS date_value
  FROM {{ ref('stg_traffic_signal_311') }}
  WHERE created_date IS NOT NULL

  UNION DISTINCT

  SELECT DISTINCT
    closed_date AS date_value
  FROM {{ ref('stg_traffic_signal_311') }}
  WHERE closed_date IS NOT NULL
)

SELECT
  -- Surrogate Key
  SAFE_CAST(FORMAT_DATE('%Y%m%d', date_value) AS INT64) AS date_id,

  -- Natural fields
  date_value,
  EXTRACT(YEAR FROM date_value) AS year,
  EXTRACT(MONTH FROM date_value) AS month,
  EXTRACT(DAY FROM date_value) AS day

FROM base
WHERE date_value IS NOT NULL