{{ config(materialized='table') }}

WITH prepared AS (
  SELECT
    *,
    -- Build crash timestamp safely (combining crash_date + crash_time + :00 for missing seconds)
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CONCAT(CAST(crash_date AS STRING), ' ', crash_time, ':00')) AS crash_timestamp
  FROM {{ ref('stg_motor_vehicle_collisions') }}
),

-- Join to dim_date
join_date AS (
  SELECT
    p.*,
    d.date_id
  FROM prepared p
  LEFT JOIN {{ ref('dim_date_collisions') }} d
    ON CAST(FORMAT_DATE('%Y%m%d', DATE(p.crash_date)) AS INT64) = d.date_id
),

-- Join to dim_time
join_time AS (
  SELECT
    jd.*,
    t.time_id
  FROM join_date jd
  LEFT JOIN {{ ref('dim_time_collisions') }} t
    ON SAFE_CAST(FORMAT_TIMESTAMP('%H%M', jd.crash_timestamp) AS INT64) = t.time_id
),

-- Join to dim_location using OR logic in priority order
join_location AS (
  SELECT
    jt.*,
    l.location_id
  FROM join_time jt
  LEFT JOIN {{ ref('dim_location_collisions') }} l
    ON
      SAFE_CAST(jt.latitude AS FLOAT64) = l.latitude OR
      SAFE_CAST(jt.longitude AS FLOAT64) = l.longitude OR
      INITCAP(jt.on_street_name) = l.on_street_name OR
      INITCAP(jt.cross_street_name) = l.cross_street_name OR
      INITCAP(jt.off_street_name) = l.off_street_name OR
      LPAD(CAST(jt.zip_code AS STRING), 5, '0') = l.zip_code OR
      INITCAP(jt.borough) = l.borough
)

-- Final output
SELECT
  SAFE_CAST(jl.collision_id AS STRING) AS collision_id,

  -- Dimension keys
  jl.date_id,
  jl.time_id,
  COALESCE(jl.location_id, 'UNKNOWN') AS location_id,

  -- Measures
  COALESCE(jl.injured_persons, 0) AS injured_persons,
  COALESCE(jl.killed_persons, 0) AS killed_persons,
  COALESCE(jl.number_of_pedestrians_injured, 0) AS injured_pedestrians,
  COALESCE(jl.number_of_pedestrians_killed, 0) AS killed_pedestrians,
  COALESCE(jl.injured_cyclists, 0) AS injured_cyclists,
  COALESCE(jl.killed_cyclists, 0) AS killed_cyclists,
  COALESCE(jl.injured_motorists, 0) AS injured_motorists,
  COALESCE(jl.killed_motorists, 0) AS killed_motorists

FROM join_location jl
WHERE jl.collision_id IS NOT NULL