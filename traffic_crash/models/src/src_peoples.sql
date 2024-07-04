with raw_peoples as (
select * from {{ source('traffic_crash', 'peoples') }}
)

select person_id, person_type, crash_record_id, vehicle_id, crash_date, sex, safety_equipment, airbag_deployed,
       ejection, injury_classification, driver_action, driver_vision, physical_condition, bac_result,
       drivers_license_state, city, state, zipcode, age, drivers_license_class, seat_no, pedpedal_action,
       pedpedal_visibility, pedpedal_location, ems_agency, hospital, ems_run_no from raw_peoples