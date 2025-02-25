from dagster import Definitions
from dagster_duckdb import DuckDBResource

from mrl_pipeline import models
from mrl_pipeline.utils import duckdb_path

model_names = [
    "infra_result_files",
    "stg_results",
]

# Create a dictionary of the imported objects.
imported_models = [getattr(models, name) for name in model_names]

defs = Definitions(
    assets=[model.build() for model in imported_models],
    resources={"duckdb_resource": DuckDBResource(database=str(duckdb_path))},
)


if __name__ == "__main__":
    print(defs)
