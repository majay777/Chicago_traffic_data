from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagster import EnvVar, PipesSubprocessClient
from dagster_duckdb import DuckDBResource
from .assets import traffic_crash_dbt_assets, app_run, crashes_data_load, vehicles_data_load, fatalities_data_load, \
    peoples_data_load, subprocess_asset
from .project import traffic_crash_project
from .schedules import schedules
database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
)

defs = Definitions(
    assets=[traffic_crash_dbt_assets, app_run, crashes_data_load, vehicles_data_load, fatalities_data_load,
            peoples_data_load, subprocess_asset],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=traffic_crash_project),
        "database": database_resource,
        "pipes_subprocess_client": PipesSubprocessClient(),
    },
)
