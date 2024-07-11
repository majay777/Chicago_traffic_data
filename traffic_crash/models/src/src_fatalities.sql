


with raw_fatalities as (
select * from {{ source('traffic_crash', 'fatalities') }}
 QUALIFY row_number() over (partition by person_id  order by person_id,crash_date) = 1
)

select person_id, crash_date, crash_location, victim, crash_circumstances, longitude, latitude,
 geocoded_column__type from raw_fatalities



