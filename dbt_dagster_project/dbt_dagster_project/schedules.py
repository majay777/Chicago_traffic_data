"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import traffic_crash_dbt_assets, crashes_data_load, vehicles_data_load, fatalities_data_load, peoples_data_load

schedules = [
    build_schedule_from_dbt_selection(
        [traffic_crash_dbt_assets],
        job_name="materialize_dbt_models",
        cron_schedule="0 0 * * *",
        dbt_select="fqn:*",
    ),
]