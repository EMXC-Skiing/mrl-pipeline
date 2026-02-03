"""Prefect flow definitions for the MRL pipeline."""

from typing import Dict, Optional

from prefect import flow
from prefect.futures import PrefectFuture

from mrl_pipeline.orchestration import ModelSelector, load_models

imported_models = load_models()
model_tasks = {model.name: model.build() for model in imported_models}


@flow(name="mrl_pipeline")
def run_pipeline(select: Optional[str] = None) -> Dict[str, PrefectFuture]:
    """Trigger each model task in dependency order."""

    selector = ModelSelector(select)
    ordered_names = selector.topological_order()

    futures: Dict[str, PrefectFuture] = {}

    for name in ordered_names:
        task = model_tasks[name]
        deps = set(selector.graph.predecessors(name))
        wait_for = [futures[dep] for dep in deps if dep in futures]
        futures[name] = task.submit(wait_for=wait_for)

    return futures


if __name__ == "__main__":
    run_pipeline()
