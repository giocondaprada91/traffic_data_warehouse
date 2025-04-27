{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    INITCAP(park_facility_name) AS facility_type
  FROM {{ ref('stg_traffic_signal_311') }}
  WHERE park_facility_name IS NOT NULL
),

dummy_facility_type AS (
  SELECT
    CAST('Unknown Facility' AS STRING) AS facility_type
)

SELECT
  GENERATE_UUID() AS facility_type_id,
  facility_type
FROM base

UNION ALL

SELECT
  GENERATE_UUID() AS facility_type_id,
  facility_type
FROM dummy_facility_type
