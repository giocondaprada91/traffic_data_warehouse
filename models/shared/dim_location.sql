{{ config(materialized='table') }}

WITH union_locations AS (

  SELECT
    SAFE_CAST(location_id AS STRING) AS location_id,
    INITCAP(borough) AS borough,
    LPAD(CAST(zip_code AS STRING), 5, '0') AS zip_code,
    INITCAP(on_street_name) AS on_street_name,
    INITCAP(cross_street_name) AS cross_street_name,
    INITCAP(off_street_name) AS off_street_name,
    SAFE_CAST(latitude AS FLOAT64) AS latitude,
    SAFE_CAST(longitude AS FLOAT64) AS longitude
  FROM {{ ref('dim_location_collisions') }}

  UNION DISTINCT

  SELECT
    SAFE_CAST(location_id AS STRING) AS location_id,
    INITCAP(borough) AS borough,
    LPAD(CAST(zip_code AS STRING), 5, '0') AS zip_code,
    INITCAP(on_street_name) AS on_street_name,
    INITCAP(cross_street_name) AS cross_street_name,
    CAST(NULL AS STRING) AS off_street_name,  
    SAFE_CAST(latitude AS FLOAT64) AS latitude,
    SAFE_CAST(longitude AS FLOAT64) AS longitude
  FROM {{ ref('dim_location_311') }}

)

SELECT *
FROM union_locations
