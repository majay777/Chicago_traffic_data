import json
import dlt
import requests

url = 'https://data.cityofchicago.org/resource/u6pd-qa9d.json'


@dlt.resource(merge_key="person_id", write_disposition="merge")
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
load_info = pipeline.run(stream_download_jsonl(url), table_name="peoples")
