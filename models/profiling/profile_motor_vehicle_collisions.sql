{{ config(materialized='table') }}

WITH base AS (
  SELECT * FROM `traffic-warehouse.raw_data.motor_vehicle_collisions`
),

null_and_common AS (
  SELECT
    COUNT(*) AS total_rows,

    -- NULL COUNTS
    COUNTIF(crash_date IS NULL) AS null_crash_date,
    COUNTIF(crash_time IS NULL OR crash_time = '') AS null_crash_time,
    COUNTIF(on_street_name IS NULL OR on_street_name = '') AS null_on_street,
    COUNTIF(cross_street_name IS NULL OR cross_street_name = '') AS null_cross_street,
    COUNTIF(borough IS NULL OR borough = '') AS null_borough,
    COUNTIF(CAST(zip_code AS STRING) IS NULL OR CAST(zip_code AS STRING) = '') AS null_zip_code,
    COUNTIF(contributing_factor_vehicle_1 IS NULL OR contributing_factor_vehicle_1 = '') AS null_cf_v1,
    COUNTIF(vehicle_type_code1 IS NULL OR vehicle_type_code1 = '') AS null_vehicle_type1,
    COUNTIF(latitude IS NULL) AS null_latitude,
    COUNTIF(longitude IS NULL) AS null_longitude,

    -- PERCENT NULLS
    ROUND(COUNTIF(crash_time IS NULL OR crash_time = '') / COUNT(*), 4) AS pct_null_crash_time,
    ROUND(COUNTIF(borough IS NULL OR borough = '') / COUNT(*), 4) AS pct_null_borough,
    ROUND(COUNTIF(vehicle_type_code1 IS NULL OR vehicle_type_code1 = '') / COUNT(*), 4) AS pct_null_vehicle_type1,
    ROUND(COUNTIF(contributing_factor_vehicle_1 IS NULL OR contributing_factor_vehicle_1 = '') / COUNT(*), 4) AS pct_null_cf_v1,

    -- MOST FREQUENT VALUES (MODES)
    APPROX_TOP_COUNT(borough, 1)[OFFSET(0)].value AS most_common_borough,
    APPROX_TOP_COUNT(on_street_name, 1)[OFFSET(0)].value AS most_common_on_street,
    APPROX_TOP_COUNT(vehicle_type_code1, 1)[OFFSET(0)].value AS most_common_vehicle_type1,
    APPROX_TOP_COUNT(contributing_factor_vehicle_1, 1)[OFFSET(0)].value AS most_common_contributing_factor

  FROM base
),

duplicates AS (
  SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT TO_JSON_STRING(t)) AS distinct_rows
  FROM base AS t
)

SELECT
  n.*,
  (n.total_rows - d.distinct_rows) AS duplicate_rows
FROM null_and_common n, duplicates d


