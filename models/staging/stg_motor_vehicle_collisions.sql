{{ config(materialized="table") }}

select
    -- Fact table identifiers
    collision_id,

    -- Date & Time
    date(crash_date) as crash_date,
    crash_time,

    -- Location fields
    initcap(borough) as borough,
    cast(zip_code as string) as zip_code,
    COALESCE(on_street_name, 'Not Reported') AS on_street_name,
    COALESCE(cross_street_name, 'Not Reported') AS cross_street_name,
    COALESCE(off_street_name, 'Not Reported') AS off_street_name,
    COALESCE(CAST(latitude AS STRING), 'Not Reported') AS latitude,
    COALESCE(CAST(longitude AS STRING), 'Not Reported') AS longitude,
    -- People involved
    number_of_persons_injured as injured_persons,
    number_of_persons_killed as killed_persons,
    number_of_pedestrians_injured,
    number_of_pedestrians_killed,
    number_of_cyclist_injured as injured_cyclists,
    number_of_cyclist_killed as killed_cyclists,
    number_of_motorist_injured as injured_motorists,
    number_of_motorist_killed as killed_motorists,

    -- Contributing factors (for bridge table)
    initcap(
        coalesce(contributing_factor_vehicle_1, 'Unspecified')
    ) as contributing_factor_vehicle_1,
    initcap(
        coalesce(contributing_factor_vehicle_2, 'Unspecified')
    ) as contributing_factor_vehicle_2,
    initcap(
        coalesce(contributing_factor_vehicle_3, 'Unspecified')
    ) as contributing_factor_vehicle_3,
    initcap(
        coalesce(contributing_factor_vehicle_4, 'Unspecified')
    ) as contributing_factor_vehicle_4,
    initcap(
        coalesce(contributing_factor_vehicle_5, 'Unspecified')
    ) as contributing_factor_vehicle_5,

    -- Vehicle types (for bridge table)
    vehicle_type_code1,
    vehicle_type_code2,
    vehicle_type_code_3,
    vehicle_type_code_4,
    vehicle_type_code_5

from `traffic-warehouse.raw_data.motor_vehicle_collisions`
where crash_date is not null and borough is not null
