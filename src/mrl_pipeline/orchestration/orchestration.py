from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Any,
    Optional,
    Sequence,
)

from mrl_pipeline.orchestration import Transformation
from mrl_pipeline.warehouse import WarehouseService, TableRef


class Orchestrator(ABC):
    """
    Builds/holds the logical DAG and executes runs.
    """

    @abstractmethod
    def register(self, t: Transformation) -> None:
        """Add a transformation node to the DAG."""
        raise NotImplementedError

    @abstractmethod
    def plan(self, targets: Sequence[str]) -> Sequence[Transformation]:
        """
        Return an ordered plan (topologically sorted) to build the requested targets.
        Targets are logical table names.
        """
        raise NotImplementedError

    @abstractmethod
    def run(
        self,
        *,
        warehouse: WarehouseService,
        targets: Sequence[str],
        run_id: Optional[str] = None,
        schema: Optional[str] = None,
        logger: Optional[Any] = None,
    ):
        raise NotImplementedError


class RunContext(ABC):
    """
    Run-level state and naming policy. This is where:
      - table lifecycle is tracked
      - temp names are allocated
      - promotion plan is built
      - promotion is triggered/committed
    """

    @property
    @abstractmethod
    def run_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def resolve_input(self, logical_name: str) -> TableRef:
        """
        Return the TableRef to use for a given logical input *right now*:
          - if the logical table was produced earlier in this run, return the temp (or staged) ref
          - otherwise return the current canonical ref from the warehouse environment
        """
        raise NotImplementedError

    @abstractmethod
    def allocate_output_temp(self, logical_name: str) -> TableRef:
        """Allocate a temp/staged TableRef for this run to write into."""
        raise NotImplementedError

    @abstractmethod
    def record_produced_temp(self, logical_name: str, temp_ref: TableRef) -> None:
        """After a node runs, mark its temp output as the latest for downstream nodes."""
        raise NotImplementedError

    @abstractmethod
    def promote_temp_tables(self):
        raise NotImplementedError
