{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    UPPER(agency) AS agency,
    INITCAP(agency_name) AS agency_name
  FROM {{ ref('stg_traffic_signal_311') }}
  WHERE agency IS NOT NULL
    OR agency_name IS NOT NULL
),

dummy_agency AS (
  SELECT
    CAST('UNKNOWN' AS STRING) AS agency,
    CAST('Unknown Agency' AS STRING) AS agency_name
)

SELECT
  GENERATE_UUID() AS agency_id,
  agency,
  agency_name
FROM base

UNION ALL

SELECT
  GENERATE_UUID() AS agency_id,
  agency,
  agency_name
FROM dummy_agency
