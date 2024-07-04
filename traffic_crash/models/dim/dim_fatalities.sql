with fatalities as (
select * from {{ ref("src_fatalities") }} where person_id is not null
)

select distinct(person_id), crash_date, crash_location, victim, crash_circumstances,
               longitude, latitude from fatalities
