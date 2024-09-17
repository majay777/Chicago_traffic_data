from dagster import pipeline, solid
import subprocess


@solid
def run_streamlit_app(_):
    subprocess.run(["streamlit", "run", "streamlit_app.py"])


@pipeline
def streamlit_pipeline():
    run_streamlit_app()
