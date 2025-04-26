{{ config(materialized='view') }}

WITH base AS (
  SELECT
    collision_id,
    INITCAP(TRIM(vehicle_type_code1)) AS vehicle_type,
    1 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE vehicle_type_code1 IS NOT NULL
    AND LOWER(TRIM(vehicle_type_code1)) != 'unknown'

  UNION ALL

  SELECT
    collision_id,
    INITCAP(TRIM(vehicle_type_code2)) AS vehicle_type,
    2 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE vehicle_type_code2 IS NOT NULL
    AND LOWER(TRIM(vehicle_type_code2)) != 'unknown'

  UNION ALL

  SELECT
    collision_id,
    INITCAP(TRIM(vehicle_type_code_3)) AS vehicle_type,
    3 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE vehicle_type_code_3 IS NOT NULL
    AND LOWER(TRIM(vehicle_type_code_3)) != 'unknown'

  UNION ALL

  SELECT
    collision_id,
    INITCAP(TRIM(vehicle_type_code_4)) AS vehicle_type,
    4 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE vehicle_type_code_4 IS NOT NULL
    AND LOWER(TRIM(vehicle_type_code_4)) != 'unknown'

  UNION ALL

  SELECT
    collision_id,
    INITCAP(TRIM(vehicle_type_code_5)) AS vehicle_type,
    5 AS vehicle_position
  FROM {{ ref('stg_motor_vehicle_collisions') }}
  WHERE vehicle_type_code_5 IS NOT NULL
    AND LOWER(TRIM(vehicle_type_code_5)) != 'unknown'
)

SELECT
  GENERATE_UUID() AS bridge_vehicle_id,
  collision_id,
  vehicle_type,
  vehicle_position
FROM base
