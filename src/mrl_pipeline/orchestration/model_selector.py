"""Helpers for selecting and ordering pipeline models."""

from __future__ import annotations

import importlib
import pkgutil
from typing import Dict, List, Optional, Sequence, Set

import networkx as nx

from mrl_pipeline import models as models_pkg
from mrl_pipeline.models.pipeline_model import PipelineModel


def _discover_models() -> Dict[str, PipelineModel]:
    """Import modules under ``mrl_pipeline.models`` and collect PipelineModel
    objects."""

    discovered: Dict[str, PipelineModel] = {}

    for info in pkgutil.iter_modules(models_pkg.__path__):
        module_name = info.name
        if info.ispkg or module_name.startswith("_") or module_name == "pipeline_model":
            continue

        module = importlib.import_module(f"{models_pkg.__name__}.{module_name}")

        for attr in vars(module).values():
            if isinstance(attr, PipelineModel):
                discovered[attr.name] = attr

    return dict(sorted(discovered.items(), key=lambda item: item[0]))


ALL_MODELS_BY_NAME = _discover_models()


def load_models(names: Optional[Sequence[str]] = None) -> List[PipelineModel]:
    """Load pipeline models, optionally restricting to a list of names."""

    if names is None:
        return list(ALL_MODELS_BY_NAME.values())

    missing = [name for name in names if name not in ALL_MODELS_BY_NAME]
    if missing:
        raise ValueError(f"Unknown model(s): {', '.join(missing)}")

    return [ALL_MODELS_BY_NAME[name] for name in names]


class ModelSelector:
    """Compute model selections and execution order based on dependencies."""

    def __init__(
        self,
        select: Optional[str] = None,
        models: Optional[Sequence[PipelineModel]] = None,
    ) -> None:
        self.models = list(models) if models is not None else load_models()
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


__all__ = ["ModelSelector", "load_models", "ALL_MODELS_BY_NAME"]
