from dagster import AssetExecutionContext, asset, MaterializeResult, file_relative_path, PipesSubprocessClient, \
    CodeReferencesMetadataValue, LocalFileCodeReference, AssetKey, AssetCheckResult
from dagster._core.pipes.client import PipesClientCompletedInvocation
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from .project import traffic_crash_project
from typing import Mapping, Any, Optional, Sequence
from dagster_duckdb import DuckDBResource
import dlt
from dlt.sources.helpers import requests
import os
import shutil


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props['resource_type']
        name = dbt_resource_props['name']
        if resource_type == "source":
            return AssetKey(f"{name}_data_load")
        else:
            return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return dbt_resource_props['fqn'][1]


@dbt_assets(manifest=traffic_crash_project.manifest_path, dagster_dbt_translator=CustomizedDagsterDbtTranslator())
def traffic_crash_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(group_name="Ingested", compute_kind="python")
def crashes_data_load(database: DuckDBResource) -> None:
    url = 'https://data.cityofchicago.org/resource/85ca-t3if.json'

    duck_database = os.getenv("DUCKDB_DATABASE")

    @dlt.resource(merge_key="crash_record_id", write_disposition="merge")
    def stream_download_jsonl(url1):
        response = requests.get(url1)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        yield response.json()

    pipeline = dlt.pipeline(
        pipeline_name="traffic_crash_db",
        destination=dlt.destinations.duckdb(duck_database),
        dataset_name="raw"
    )
    # The response contains a list of issues
    load_info = pipeline.run(stream_download_jsonl(url), table_name="crashes")


@asset(group_name="Ingested", compute_kind="python", deps=[crashes_data_load])
def peoples_data_load(database: DuckDBResource) -> None:
    url = 'https://data.cityofchicago.org/resource/u6pd-qa9d.json'
    duckdb_database = os.getenv("DUCKDB_DATABASE")

    @dlt.resource(merge_key="person_id", write_disposition="merge")
    def stream_download_jsonl(url1):
        response = requests.get(url1)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        yield response.json()

    pipeline = dlt.pipeline(
        pipeline_name="traffic_crash_db",
        destination=dlt.destinations.duckdb(duckdb_database),
        dataset_name="raw"
    )
    # The response contains a list of issues
    load_info = pipeline.run(stream_download_jsonl(url), table_name="peoples")


@asset(group_name="Ingested", compute_kind="python", deps=[peoples_data_load])
def vehicles_data_load(database: DuckDBResource) -> None:
    url = 'https://data.cityofchicago.org/resource/68nd-jvt3.json'
    duckdb_database = os.getenv("DUCKDB_DATABASE")

    @dlt.resource(merge_key="crash_unit_id", write_disposition="merge")
    def stream_download_jsonl(url1):
        response = requests.get(url1)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        yield response.json()

    pipeline = dlt.pipeline(
        pipeline_name="traffic_crash_db",
        destination=dlt.destinations.duckdb(duckdb_database),
        dataset_name="raw"
    )
    # The response contains a list of issues
    load_info = pipeline.run(stream_download_jsonl(url), table_name="vehicles")


@asset(group_name="Ingested", compute_kind="python", deps=[vehicles_data_load])
def fatalities_data_load(database: DuckDBResource) -> None:
    url = 'https://data.cityofchicago.org/resource/gzaz-isa6.json'
    duckdb_database = os.getenv("DUCKDB_DATABASE")

    @dlt.resource()
    def stream_download_jsonl(url1):
        response = requests.get(url1)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        yield response.json()

    pipeline = dlt.pipeline(
        pipeline_name="traffic_crash_db",
        destination=dlt.destinations.duckdb(duckdb_database),
        dataset_name="raw"
    )
    # The response contains a list of issues
    load_info = pipeline.run(stream_download_jsonl(url), table_name="fatalities")


@asset(metadata={
    "dagster/code_references": CodeReferencesMetadataValue(
        code_references=[
            LocalFileCodeReference(
                file_path=os.path.join(os.path.dirname(__file__), "pipeline.py"),
                # Label and line number are optional
                label="Pipeline Py File.",

            )
        ]
    )
}, group_name="Streamlit_App", compute_kind="python")
def app_run(database: DuckDBResource) -> None:
    pass


@asset
def subprocess_asset(
        context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> PipesClientCompletedInvocation:
    shell_script_path = "../../scratch.sh"
    # cmd = [shutil.which("streamlit run"), file_relative_path(__file__, "streamlit_app.py")]
    return pipes_subprocess_client.run(
        command=["bash", shell_script_path], context=context
    )
