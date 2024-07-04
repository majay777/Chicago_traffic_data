
import dlt
from dlt.sources.helpers import requests

url = 'https://data.cityofchicago.org/resource/85ca-t3if.json'


@dlt.resource(merge_key="crash_record_id", write_disposition="merge")
def stream_download_jsonl(Url):
    response = requests.get(Url)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    yield response.json()


pipeline = dlt.pipeline(
    pipeline_name="traffic_crash_db",
    destination=dlt.destinations.duckdb("traffic_crash_db.duckdb"),
    dataset_name="raw"
)
# The response contains a list of issues
load_info = pipeline.run(stream_download_jsonl(url), table_name="crashes")
