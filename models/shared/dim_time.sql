{{ config(materialized='table') }}

WITH union_times AS (

  SELECT
    SAFE_CAST(time_id AS STRING) AS time_id,
    time_value,
    FORMAT_TIME('%H:%M', time_value) AS time_string
  FROM {{ ref('dim_time_collisions') }}

  UNION DISTINCT

  SELECT
    SAFE_CAST(time_id AS STRING) AS time_id,
    time_value,
    FORMAT_TIME('%H:%M', time_value) AS time_string
  FROM {{ ref('dim_time_311') }}

)

SELECT *
FROM union_times
