{{ config(materialized='table') }}

WITH base AS (
  SELECT
    contributing_factor_vehicle_1 AS factor
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_1 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_1)) != 'unspecified'
  
  UNION DISTINCT

  SELECT
    contributing_factor_vehicle_2
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_2 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_2)) != 'unspecified'

  UNION DISTINCT

  SELECT
    contributing_factor_vehicle_3
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_3 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_3)) != 'unspecified'

  UNION DISTINCT

  SELECT
    contributing_factor_vehicle_4
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_4 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_4)) != 'unspecified'

  UNION DISTINCT

  SELECT
    contributing_factor_vehicle_5
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_5 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_5)) != 'unspecified'
)

SELECT
  ROW_NUMBER() OVER (ORDER BY INITCAP(TRIM(factor))) AS factor_id,  -- simple number
  INITCAP(TRIM(factor)) AS cf_description
FROM base
