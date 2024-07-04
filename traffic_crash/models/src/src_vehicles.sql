with raw_vehicles as (
select * from {{ source('traffic_crash', 'vehicles') }}
)

select crash_unit_id, crash_record_id, crash_date, unit_no, unit_type, num_passengers, vehicle_id, make, model,
       lic_plate_state, vehicle_year, vehicle_defect, vehicle_type, vehicle_use,
       travel_direction, maneuver, occupant_cnt, area_05_i, area_06_i, first_contact_point,
       area_01_i, area_02_i, area_11_i, area_12_i, area_08_i, area_09_i, area_10_i, area_99_i, towed_i, towed_by,
       towed_to, area_04_i, area_07_i, area_00_i, area_03_i, cmrc_veh_i, cmv_id, carrier_name, carrier_state,
       carrier_city, hazmat_present_i, hazmat_report_i, mcs_report_i, hazmat_vio_cause_crash_i, mcs_vio_cause_crash_i,
       vehicle_config, cargo_body_type, load_type, hazmat_out_of_service_i, mcs_out_of_service_i, usdot_no,
       commercial_src, ccmc_no, ilcc_no, gvwr, idot_permit_no, trailer1_width, total_vehicle_length, axle_cnt
       from raw_vehicles