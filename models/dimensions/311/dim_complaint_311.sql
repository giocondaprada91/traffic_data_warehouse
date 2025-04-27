{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
    INITCAP(complaint_type) AS complaint_type,
    INITCAP(descriptor) AS descriptor,
    INITCAP(status) AS status
  FROM {{ ref('stg_traffic_signal_311') }}
  WHERE complaint_type IS NOT NULL
    OR descriptor IS NOT NULL
    OR status IS NOT NULL
),

dummy_complaint_type AS (
  SELECT
    CAST('Unknown Complaint' AS STRING) AS complaint_type,
    CAST('Unknown Descriptor' AS STRING) AS descriptor,
    CAST('Unknown Status' AS STRING) AS status
)

SELECT
  GENERATE_UUID() AS complaint_type_id,
  complaint_type,
  descriptor,
  status
FROM base

UNION ALL

SELECT
  GENERATE_UUID() AS complaint_type_id,
  complaint_type,
  descriptor,
  status
FROM dummy_complaint_type
