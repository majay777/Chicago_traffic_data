with raw_crashes as (
select * from {{ source('traffic_crash', 'crashes') }}
)

select crash_record_id, crash_date, posted_speed_limit,
traffic_control_device, device_condition, weather_condition,
lighting_condition, first_crash_type, trafficway_type, alignment,
roadway_surface_cond, road_defect, report_type, crash_type, damage,
date_police_notified, prim_contributory_cause, sec_contributory_cause,
street_no, street_direction, street_name, beat_of_occurrence, num_units,
most_severe_injury, injuries_total, injuries_fatal, injuries_incapacitating,
injuries_non_incapacitating, injuries_reported_not_evident, injuries_no_indication,
injuries_unknown, crash_hour, crash_day_of_week, crash_month, latitude, longitude,
location__type, _acomputed_region_rpca_8um6,  hit_and_run_i, intersection_related_i,
photos_taken_i, statements_taken_i, crash_date_est_i, private_property_i, work_zone_i,
work_zone_type, workers_present_i, dooring_i from raw_crashes