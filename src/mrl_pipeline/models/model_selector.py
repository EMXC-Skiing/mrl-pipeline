"""Helpers for selecting and ordering pipeline models."""

from __future__ import annotations

from typing import List, Optional, Sequence, Set

import networkx as nx

from mrl_pipeline import models as model_module
from mrl_pipeline.models.pipeline_model import PipelineModel

# Default ordering mirrors the historical asset list so behavior remains
# consistent for callers that rely on import-time ordering.
DEFAULT_MODEL_NAMES: List[str] = [
    "infra_result_files",
    "stg_results",
]


def load_models(names: Optional[Sequence[str]] = None) -> List[PipelineModel]:
    """Load pipeline models by name from the models package."""

    name_list = list(names) if names is not None else DEFAULT_MODEL_NAMES
    return [getattr(model_module, name) for name in name_list]


class ModelSelector:
    """Compute model selections and execution order based on dependencies."""

    def __init__(
        self,
        select: Optional[str] = None,
        models: Optional[Sequence[PipelineModel]] = None,
    ) -> None:
        loaded_models = list(models) if models is not None else load_models()
        self.models = loaded_models
        self.select = select

        # cache by name for fast lookup and consistent deterministic order
        self.model_by_name = {model.name: model for model in self.models}
        self.import_order_index = {
            model.name: index for index, model in enumerate(self.models)
        }

        self.graph = nx.DiGraph()
        # add every model as a node even if it has no edges, so isolated models run
        self.graph.add_nodes_from(self.model_by_name)
        for model in self.models:
            for dependency in model.deps:
                if dependency not in self.model_by_name:
                    raise ValueError(
                        f"Unknown dependency '{dependency}' for model '{model.name}'.",
                    )
                self.graph.add_edge(dependency, model.name)

        self.selected_names = self._resolve_selection(select)

    def _ancestors(self, name: str) -> Set[str]:
        return nx.ancestors(self.graph, name)

    def _descendants(self, name: str) -> Set[str]:
        return nx.descendants(self.graph, name)

    def _resolve_selector(self, selector: str) -> Set[str]:
        if not selector:
            return set()

        include_upstream = selector.startswith("+")
        include_downstream = selector.endswith("+")
        core = selector.strip("+")

        if not core:
            raise ValueError("Selector must include a model name.")
        if core not in self.model_by_name:
            raise ValueError(f"Unknown model '{core}' in selection.")

        selected: Set[str] = {core}
        if include_upstream:
            selected.update(self._ancestors(core))
        if include_downstream:
            selected.update(self._descendants(core))

        return selected

    def _resolve_selection(self, select: Optional[str]) -> Set[str]:
        if not select:
            return set(self.model_by_name)

        union_groups = [token.strip() for token in select.split() if token.strip()]
        if not union_groups:
            return set(self.model_by_name)

        resolved_sets: List[Set[str]] = []
        for group in union_groups:
            intersections = [part.strip() for part in group.split(",") if part.strip()]
            if not intersections:
                continue

            sets = [self._resolve_selector(selector) for selector in intersections]
            group_set = set.intersection(*sets) if sets else set()
            if not group_set:
                continue
            resolved_sets.append(group_set)

        if not resolved_sets:
            raise ValueError("Selection did not match any models.")

        return set().union(*resolved_sets)

    def topological_order(self) -> List[str]:
        subset = set(self.selected_names)
        subgraph = self.graph.subgraph(subset).copy()

        if not nx.is_directed_acyclic_graph(subgraph):
            raise ValueError(f"Cycle detected or missing dependencies for: {subset}")

        # prefer original import order when multiple options exist
        return list(
            nx.algorithms.dag.lexicographical_topological_sort(
                subgraph,
                key=lambda name: self.import_order_index[name],
            ),
        )
