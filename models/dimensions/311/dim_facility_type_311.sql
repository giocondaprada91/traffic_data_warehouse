{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    INITCAP(facility_type) AS facility_type
  FROM {{ ref('stg_traffic_signal_311') }}
)

SELECT
  GENERATE_UUID() AS facility_type_id,
  facility_type
FROM base
