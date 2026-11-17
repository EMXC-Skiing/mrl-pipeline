from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Any,
    Mapping,
    Optional,
    Sequence,
)

from pandas import DataFrame

from mrl_pipeline.warehouse import WarehouseService, TableRef


class Transformation(ABC):
    """
    Inputs/outputs are declared; orchestrator supplies resolved TableRefs at runtime.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def output_logical_name(self) -> str:
        """The logical name of the table produced by this transformation."""
        raise NotImplementedError

    @property
    @abstractmethod
    def input_logical_names(self) -> Sequence[str]:
        """Logical names of upstream tables required by this transformation."""
        raise NotImplementedError

    @abstractmethod
    def run(
        self,
        *,
        warehouse: WarehouseService,
        inputs: Mapping[str, TableRef],  # logical -> resolved physical
        output: TableRef,  # resolved *temp* output target for this run
        logger: Optional[Any] = None,
    ):
        """
        Execute the transformation into `output` (typically a temp table for the run).
        """
        raise NotImplementedError


class SqlTransformation(Transformation):
    """
    Common subclass pattern: SQL template + CTAS.
    """

    @property
    @abstractmethod
    def sql_template(self):
        raise NotImplementedError

    def run(
        self,
        *,
        warehouse: WarehouseService,
        inputs: Mapping[str, TableRef],
        output: TableRef,
        logger: Optional[Any] = None,
    ):
        rendered = warehouse.render_query(self.sql_template, identifiers=inputs)
        warehouse.create_table_as_select(output, rendered.sql)
        return


class PythonTransformation(Transformation):
    """
    Common subclass pattern: fetch inputs as DataFrames, run Python, write result.
    """

    @abstractmethod
    def compute(
        self,
        *,
        inputs: Mapping[str, DataFrame],
        logger: Optional[Any] = None,
    ) -> DataFrame:
        raise NotImplementedError

    def run(
        self,
        *,
        warehouse: WarehouseService,
        inputs: Mapping[str, TableRef],
        output: TableRef,
        logger: Optional[Any] = None,
    ):
        dfs = {k: warehouse.read_table(v) for k, v in inputs.items()}
        out_df = self.compute(inputs=dfs, logger=logger)
        warehouse.write_table(output, out_df, mode="replace")
        return
