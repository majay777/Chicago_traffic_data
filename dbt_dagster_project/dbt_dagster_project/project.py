from pathlib import Path

from dagster_dbt import DbtProject

traffic_crash_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "traffic_crash").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
traffic_crash_project.prepare_if_dev()
