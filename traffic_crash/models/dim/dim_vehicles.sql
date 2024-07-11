with vehicles as (
select * from {{ ref('src_vehicles') }}
)

select crash_unit_id,crash_record_id,crash_date,unit_no,unit_type,vehicle_id,
make,model,lic_plate_state,vehicle_year,vehicle_defect,vehicle_type,vehicle_use,
travel_direction,maneuver,occupant_cnt, num_passengers, towed_i, towed_by, towed_to, hour(crash_date) as crash_hour
from vehicles
