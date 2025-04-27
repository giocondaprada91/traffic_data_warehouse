{{ config(materialized="table") }}

WITH fact_table AS(
    SELECT
        GENERATE_UUID() AS unique_id,
        created_d.date_id AS created_date_id,
        closed_d.date_id AS closed_date_id,
        created_t.time_id AS created_time_id,
        closed_t.time_id AS closed_time_id,
        a.agency_id AS agency_id,
        c.complaint_type_id AS complaint_id,
        l.location_id AS location_id,
        ft.facility_type AS facility_type_id,
        CASE 
            WHEN closed_d.date_value IS NOT NULL 
            THEN DATE_DIFF(DATE(closed_d.date_value), DATE(created_d.date_value), DAY)
            ELSE NULL
        END AS status_duration_days
    FROM
        {{ ref('stg_traffic_signal_311') }} s
    LEFT JOIN {{ ref('dim_date_311') }} created_d 
        ON DATE(s.created_date) = created_d.date_value
    LEFT JOIN {{ ref('dim_date_311') }} closed_d 
        ON DATE(s.closed_date) = closed_d.date_value
    LEFT JOIN {{ ref('dim_time_311') }} created_t
        ON TIME(CAST(s.created_time AS TIME)) = created_t.time_value
    LEFT JOIN {{ ref('dim_time_311') }} closed_t
        ON TIME(CAST(s.closed_time AS TIME)) = closed_t.time_value
    LEFT JOIN {{ ref('dim_agency_311') }} a 
        ON s.agency = a.agency
    LEFT JOIN {{ ref('dim_complaint_311') }} c 
        ON s.complaint_type = c.complaint_type
        AND s.descriptor = c.descriptor
        AND s.status = c.status
    LEFT JOIN {{ ref('dim_location_311') }} l
        ON s.location = l.location
    LEFT JOIN {{ ref('dim_facility_type_311') }} ft 
        ON s.facility_type = ft.facility_type
)

SELECT *
FROM fact_table