{{ config(materialized='view') }}

WITH source_data AS (
  SELECT
    unique_key,

    -- Format timestamps to remove fractional seconds
    FORMAT_TIMESTAMP('%F %H:%M:%S', created_date) AS created_ts,
    FORMAT_TIMESTAMP('%F %H:%M:%S', closed_date) AS closed_ts,
    DATETIME_DIFF(closed_date, created_date, DAY) AS days_to_close,

    -- Cleaned descriptor
    LOWER(TRIM(descriptor)) AS descriptor_cleaned,

    -- Standardized ZIP
    SAFE_CAST(incident_zip AS STRING) AS incident_zip,


  FROM
    `traffic-warehouse.raw_data.311_service_requests`
  WHERE
    unique_key IS NOT NULL
    AND borough IS NOT NULL
    AND incident_zip IS NOT NULL
)

SELECT *
FROM source_data