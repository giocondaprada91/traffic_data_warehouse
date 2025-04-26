{{ config(materialized='view') }}

WITH base AS (
  SELECT
    collision_id,
    INITCAP(TRIM(contributing_factor_vehicle_1)) AS factor,
    1 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_1 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_1)) != 'unspecified'

  UNION ALL

  SELECT
    collision_id,
    INITCAP(TRIM(contributing_factor_vehicle_2)) AS factor,
    2 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_2 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_2)) != 'unspecified'

  UNION ALL

  SELECT
    collision_id,
    INITCAP(TRIM(contributing_factor_vehicle_3)) AS factor,
    3 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_3 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_3)) != 'unspecified'

  UNION ALL

  SELECT
    collision_id,
    INITCAP(TRIM(contributing_factor_vehicle_4)) AS factor,
    4 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_4 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_4)) != 'unspecified'

  UNION ALL

  SELECT
    collision_id,
    INITCAP(TRIM(contributing_factor_vehicle_5)) AS factor,
    5 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE contributing_factor_vehicle_5 IS NOT NULL
    AND LOWER(TRIM(contributing_factor_vehicle_5)) != 'unspecified'
)

SELECT
  GENERATE_UUID() AS bridge_factor_id,
  collision_id,
  factor,
  vehicle_position
FROM base
