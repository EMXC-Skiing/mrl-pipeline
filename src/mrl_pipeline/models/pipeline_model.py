"""pipeline_model.py.

This module defines the base class from which MRL models are instantiated.

"""

from typing import Callable, Optional

import dagster as dg
import duckdb
from dagster_duckdb import DuckDBResource


class PipelineModel:
    """Base class representing a model within the MRL pipeline.

    This class encapsulates both metadata and execution logic for a model,
    enabling it to be integrated as a dagster asset.

    Attributes:
        name (str): Unique identifier for the model.
        description (str): Brief summary of the model's purpose.
        column_descriptions (dict[str, str] | None): Optional mapping of column names to
            their descriptions.
        deps (list[str]): List of asset names that this model depends on.
        runner (callable): Function or callable that performs the model's computations.

    """

    def __init__(
        self,
        name: str,
        description: str,
        deps: list[str],
        runner: Callable,
        column_descriptions: Optional[dict[str, str]] = None,
    ) -> None:
        """Initialize a PipelineModel instance with its metadata and execution
            parameters.

        Args:
            name (str): Unique identifier for the model.
            description (str): Brief summary of the model's purpose.
            column_descriptions (dict[str, str] | None): Optional mapping of column
                names to their descriptions.
            deps (list[str]): List of asset names that this model depends on.
            runner (callable): Function or callable that performs the model's
                computations.

        """
        self.name = name
        self.description = description
        self.deps = deps
        self.runner = runner
        self.column_descriptions = column_descriptions or {}
        self.asset_metadata = {
            "dagster/column_schema": dg.TableSchema(
                columns=[
                    dg.TableColumn(name=k, description=v)
                    for k, v in self.column_descriptions.items()
                ],
            ),
        }

    def run(self) -> duckdb.DuckDBPyConnection:
        """Execute the model's logic with any provided arguments and keyword arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Any: The result of the model's computation.

        """
        return self.runner(duckdb)

    def build(self) -> Callable:
        """Create a Dagster asset that wraps the model's execution logic.

        This method uses the Dagster @asset decorator to convert the model's runner
        function into an asset.

        Returns:
            Callable: A Dagster asset that executes the model's logic.

        """

        @dg.asset(
            name=self.name,
            deps=self.deps,
            description=self.description,
            metadata=self.asset_metadata,
        )
        def run_model(
            context: dg.AssetExecutionContext,
            duckdb_resource: DuckDBResource,
        ) -> dg.MaterializeResult:
            # Run the model with any provided arguments and keyword arguments.
            conn = self.runner(duckdb)

            row_count = conn.execute(f"""
            SELECT count(*) FROM {self.name}
            """).fetchone()[0]  # noqa: S608

            preview_df = conn.execute(f"""
            SELECT * FROM {self.name} LIMIT 10
            """).df()  # noqa: S608

            return dg.MaterializeResult(
                metadata={
                    "row_count": dg.MetadataValue.int(row_count),
                    "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                },
            )

        return run_model
