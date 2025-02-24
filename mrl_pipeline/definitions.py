from dagster import Definitions

from mrl_pipeline import models

model_names = [
    "infra_result_files",
    "stg_results",
]

# Create a dictionary of the imported objects.
imported_models = [getattr(models, name) for name in model_names]

defs = Definitions(
    assets=[model.build() for model in imported_models],
)
