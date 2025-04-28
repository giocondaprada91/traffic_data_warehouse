{{ config(materialized='table') }}

SELECT DISTINCT
  date_id,
  date_value,
  day,
  month,
  year
FROM {{ ref('dim_date_collisions') }}

UNION DISTINCT

SELECT DISTINCT
  date_id,
  date_value,
  day,
  month,
  year
FROM {{ ref('dim_date_311') }}
