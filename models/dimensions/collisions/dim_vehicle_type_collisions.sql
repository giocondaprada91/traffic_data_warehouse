{{ config(materialized='table') }}

WITH base AS (
  SELECT
    vehicle_type_code1,
    vehicle_type_code2,
    vehicle_type_code_3,
    vehicle_type_code_4,
    vehicle_type_code_5
  FROM {{ ref('stg_motor_vehicle_collisions') }}
),

vehicle_types AS (
  SELECT vehicle_type_code1 AS vehicle_type FROM base
  UNION DISTINCT
  SELECT vehicle_type_code2 FROM base
  UNION DISTINCT
  SELECT vehicle_type_code_3 FROM base
  UNION DISTINCT
  SELECT vehicle_type_code_4 FROM base
  UNION DISTINCT
  SELECT vehicle_type_code_5 FROM base
)

SELECT
  ROW_NUMBER() OVER (ORDER BY INITCAP(TRIM(vehicle_type))) AS vehicle_type_id,
  INITCAP(TRIM(vehicle_type)) AS vehicle_type_description
FROM vehicle_types
WHERE vehicle_type IS NOT NULL
  AND LOWER(TRIM(vehicle_type)) != 'unknown'
