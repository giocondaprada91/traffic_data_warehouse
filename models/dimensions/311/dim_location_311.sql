{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    INITCAP(borough) AS borough,
    INITCAP(city) AS city,
    LPAD(CAST(incident_zip AS STRING), 5, '0') AS incident_zip,
    INITCAP(intersection_street_1) AS intersection_1,
    INITCAP(intersection_street_2) AS intersection_2,
    INITCAP(address_type) AS address_type,
    INITCAP(incident_address) AS incident_address,
    INITCAP(street_name) AS street_name,
    INITCAP(cross_street_1) AS cross_street_1,
    INITCAP(cross_street_2) AS cross_street_2,
    CAST(bbl AS STRING) AS bbl,
    SAFE_CAST(latitude AS FLOAT64) AS latitude,  -- ✅ safer for bad data
    SAFE_CAST(longitude AS FLOAT64) AS longitude,
    location,
    INITCAP(community_board) AS community_board
  FROM {{ ref('stg_traffic_signal_311') }}
  WHERE borough IS NOT NULL
    OR city IS NOT NULL
    OR incident_zip IS NOT NULL
    OR intersection_street_1 IS NOT NULL
    OR intersection_street_2 IS NOT NULL
    OR address_type IS NOT NULL
    OR incident_address IS NOT NULL
    OR street_name IS NOT NULL
    OR cross_street_1 IS NOT NULL
    OR cross_street_2 IS NOT NULL
    OR bbl IS NOT NULL
    OR latitude IS NOT NULL
    OR longitude IS NOT NULL
    OR location IS NOT NULL
    OR community_board IS NOT NULL
),

dummy_location AS (
  SELECT
    CAST('UNKNOWN' AS STRING) AS borough,
    CAST('UNKNOWN' AS STRING) AS city,
    CAST('00000' AS STRING) AS incident_zip,
    CAST(NULL AS STRING) AS intersection_1,
    CAST(NULL AS STRING) AS intersection_2,
    CAST(NULL AS STRING) AS address_type,
    CAST(NULL AS STRING) AS incident_address,
    CAST(NULL AS STRING) AS street_name,
    CAST(NULL AS STRING) AS cross_street_1,
    CAST(NULL AS STRING) AS cross_street_2,
    CAST(NULL AS STRING) AS bbl,
    CAST(NULL AS FLOAT64) AS latitude,
    CAST(NULL AS FLOAT64) AS longitude,
    CAST(NULL AS STRING) AS location,
    CAST(NULL AS STRING) AS community_board
)

SELECT
  GENERATE_UUID() AS location_id,
  borough,
  city,
  incident_zip AS zip_code,               -- ✅ renamed
  intersection_1 AS on_street_name,       -- ✅ renamed
  intersection_2 AS cross_street_name,    -- ✅ renamed
  address_type,
  incident_address,
  street_name,
  cross_street_1,
  cross_street_2,
  bbl,
  latitude,
  longitude,
  location,
  community_board
FROM base

UNION ALL

SELECT
  GENERATE_UUID() AS location_id,
  borough,
  city,
  incident_zip AS zip_code,               -- ✅ renamed
  intersection_1 AS on_street_name,       -- ✅ renamed
  intersection_2 AS cross_street_name,    -- ✅ renamed
  address_type,
  incident_address,
  street_name,
  cross_street_1,
  cross_street_2,
  bbl,
  latitude,
  longitude,
  location,
  community_board
FROM dummy_location
