{{ config(materialized='view') }}

SELECT
  -- Identifiers
  SAFE_CAST(unique_key AS STRING) AS unique_id,

  -- Date fields (Only Date part, no time)
  DATE(created_date) AS created_date,
  DATE(closed_date) AS closed_date,

  -- Time fields (extracted separately)
  FORMAT_TIMESTAMP('%H:%M:%S', created_date) AS created_time,
  FORMAT_TIMESTAMP('%H:%M:%S', closed_date) AS closed_time,

  -- Agency
  UPPER(agency) AS agency,  -- short name
  INITCAP(agency_name) AS agency_name,  -- full name

  -- Complaint info
  INITCAP(complaint_type) AS complaint_type,
  INITCAP(descriptor) AS descriptor,
  INITCAP(status) AS status,

  -- Resolution
  resolution_description,
  TIMESTAMP(resolution_action_updated_date) AS resolution_action_updated_date,

  -- Location
  INITCAP(city) AS city,
  INITCAP(borough) AS borough,
  LPAD(CAST(incident_zip AS STRING), 5, '0') AS incident_zip,  -- make sure zip codes are 5 digits
  INITCAP(intersection_street_1) AS intersection_street_1,
  INITCAP(intersection_street_2) AS intersection_street_2,
  INITCAP(address_type) AS address_type,
  INITCAP(incident_address) AS incident_address,
  INITCAP(street_name) AS street_name,
  INITCAP(cross_street_1) AS cross_street_1,
  INITCAP(cross_street_2) AS cross_street_2,
  CAST(bbl AS STRING) AS bbl,  -- BBL sometimes are numbers, cast to STRING

  -- Coordinates
  latitude,
  longitude,
  location,  -- raw location object from JSON
  
  -- Park info
  INITCAP(park_facility_name) AS park_facility_name,
  INITCAP(park_borough) AS park_borough,
  
  -- X/Y State Plane Coordinates (keeping them as-is)
  x_coordinate_state_plane,
  y_coordinate_state_plane,

  -- Other
  INITCAP(open_data_channel_type) AS open_data_channel_type,

  -- Community
  INITCAP(community_board) AS community_board

FROM `traffic-warehouse.raw_data.311_service_requests`
WHERE unique_key IS NOT NULL

