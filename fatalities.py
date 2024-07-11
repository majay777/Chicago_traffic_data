
import dlt
from dlt.sources.helpers import requests
from streamlit_dynamic_filters import DynamicFilters

url = 'https://data.cityofchicago.org/resource/gzaz-isa6.json'


@dlt.resource()
def stream_download_jsonl(url1):
    response = requests.get(url1)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    yield response.json()


pipeline = dlt.pipeline(
    pipeline_name="traffic_crash_db",
    destination=dlt.destinations.duckdb("traffic_crash_db.duckdb"),
    dataset_name="raw"
)
# The response contains a list of issues
load_info = pipeline.run(stream_download_jsonl(url), table_name="fatalities")
