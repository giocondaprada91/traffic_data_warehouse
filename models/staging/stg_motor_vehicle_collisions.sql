{{ config(materialized='table') }}

SELECT
  -- Fact table identifiers
  collision_id,

  -- Date & Time
  DATE(crash_date) AS crash_date,
  crash_time,

  -- Location fields
  INITCAP(borough) AS borough,
  CAST(zip_code AS STRING) AS zip_code,
  on_street_name,
  cross_street_name,
  off_street_name,
  latitude,
  longitude,

  -- People involved
  number_of_persons_injured AS injured_persons,
  number_of_persons_killed AS killed_persons,
  number_of_pedestrians_injured,
  number_of_pedestrians_killed,
  number_of_cyclist_injured AS injured_cyclists,
  number_of_cyclist_killed AS killed_cyclists,
  number_of_motorist_injured AS injured_motorists,
  number_of_motorist_killed AS killed_motorists,

  -- Contributing factors (for bridge table)
  INITCAP(COALESCE(contributing_factor_vehicle_1, 'Unspecified')) AS contributing_factor_vehicle_1,
  INITCAP(COALESCE(contributing_factor_vehicle_2, 'Unspecified')) AS contributing_factor_vehicle_2,
  INITCAP(COALESCE(contributing_factor_vehicle_3, 'Unspecified')) AS contributing_factor_vehicle_3,
  INITCAP(COALESCE(contributing_factor_vehicle_4, 'Unspecified')) AS contributing_factor_vehicle_4,
  INITCAP(COALESCE(contributing_factor_vehicle_5, 'Unspecified')) AS contributing_factor_vehicle_5,

  -- Vehicle types (for bridge table)
  vehicle_type_code1,
  vehicle_type_code2,
  vehicle_type_code_3,
  vehicle_type_code_4,
  vehicle_type_code_5

FROM `traffic-warehouse.raw_data.motor_vehicle_collisions`
WHERE crash_date IS NOT NULL


