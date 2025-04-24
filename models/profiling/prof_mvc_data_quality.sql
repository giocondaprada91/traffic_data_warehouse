{{ config(materialized='table') }}

SELECT
  COUNT(*) AS total_rows,

  -- Crash info
  COUNTIF(crash_date IS NULL) AS null_crash_date,
  COUNTIF(crash_time IS NULL) AS null_crash_time,
  COUNTIF(collision_id IS NULL) AS null_collision_id,

  -- Location fields
  COUNTIF(on_street_name IS NULL OR on_street_name = '') AS null_on_street,
  COUNTIF(off_street_name IS NULL OR off_street_name = '') AS null_off_street,
  COUNTIF(cross_street_name IS NULL OR cross_street_name = '') AS null_cross_street,
  COUNTIF(borough IS NULL OR borough = '') AS null_borough,
  COUNTIF(zip_code IS NULL OR CAST(zip_code AS STRING) = '') AS null_zip,

  -- Coordinates
  COUNTIF(latitude IS NULL) AS null_latitude,
  COUNTIF(longitude IS NULL) AS null_longitude,

  -- Injury counts
  COUNTIF(number_of_persons_injured > 0) AS nonzero_injured_count,
  COUNTIF(number_of_persons_killed > 0) AS nonzero_killed_count,

  -- Contributing factors
  COUNTIF(contributing_factor_vehicle_1 IS NULL OR contributing_factor_vehicle_1 = '') AS null_cf_v1,
  COUNTIF(contributing_factor_vehicle_2 IS NULL OR contributing_factor_vehicle_2 = '') AS null_cf_v2,
  COUNTIF(contributing_factor_vehicle_3 IS NULL OR contributing_factor_vehicle_3 = '') AS null_cf_v3,

  -- Vehicle types
  COUNTIF(vehicle_type_code1 IS NULL OR vehicle_type_code1 = '') AS null_vehicle1,
  COUNTIF(vehicle_type_code2 IS NULL OR vehicle_type_code2 = '') AS null_vehicle2,
  COUNTIF(vehicle_type_code_3 IS NULL OR vehicle_type_code_3 = '') AS null_vehicle3

FROM
  `traffic-warehouse.raw_data.motor_vehicle_collisions`




