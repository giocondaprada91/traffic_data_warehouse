{{ config(materialized='table') }}

WITH base AS (
  SELECT * FROM `traffic-warehouse.raw_data.311_service_requests`
),

null_and_common AS (
  SELECT
    COUNT(*) AS total_rows,

    -- NULL counts
    COUNTIF(CAST(unique_key AS STRING) IS NULL OR CAST(unique_key AS STRING) = '') AS null_unique_key,
    COUNTIF(created_date IS NULL) AS null_created_date,
    COUNTIF(closed_date IS NULL) AS null_closed_date,
    COUNTIF(agency IS NULL OR agency = '') AS null_agency,
    COUNTIF(complaint_type IS NULL OR complaint_type = '') AS null_complaint_type,
    COUNTIF(descriptor IS NULL OR descriptor = '') AS null_descriptor,
    COUNTIF(CAST(incident_zip AS STRING) IS NULL OR CAST(incident_zip AS STRING) = '') AS null_incident_zip,
    COUNTIF(borough IS NULL OR borough = '') AS null_borough,

-- Most Frequent Values (mode)
    APPROX_TOP_COUNT(agency, 1)[OFFSET(0)].value AS most_common_agency,
    APPROX_TOP_COUNT(complaint_type, 1)[OFFSET(0)].value AS most_common_complaint,
    APPROX_TOP_COUNT(descriptor, 1)[OFFSET(0)].value AS most_common_descriptor,
    APPROX_TOP_COUNT(borough, 1)[OFFSET(0)].value AS most_common_borough

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

