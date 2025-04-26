{{ config(materialized='view') }}

WITH source_data AS (
  SELECT
    collision_id,
    crash_date,
    crash_time,
    INITCAP(LOWER(borough)) AS borough,
    zip_code,
    number_of_persons_injured,
    number_of_persons_killed,
    contributing_factor_vehicle_1,
    vehicle_type_code1,
    contributing_factor_vehicle_2,
    vehicle_type_code2,
    CASE
      WHEN number_of_persons_injured > 0 THEN 'Injury'
      WHEN number_of_persons_killed > 0 THEN 'Fatal'
      ELSE 'No Injury'
    END AS crash_severity,

     FORMAT_DATE('%A', crash_date) AS day_of_week

  FROM
    `traffic-warehouse.raw_data.motor_vehicle_collisions`
  WHERE
    collision_id IS NOT NULL
    AND crash_date IS NOT NULL
    AND borough IS NOT NULL
)

SELECT *
FROM source_data