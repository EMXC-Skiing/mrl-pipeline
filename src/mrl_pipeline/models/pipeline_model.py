"""pipeline_model.py.

This module defines the base class from which MRL models are instantiated.

"""

from typing import Any, Callable, Optional

from duckdb import DuckDBPyConnection
from prefect import get_run_logger, task

from mrl_pipeline.io.database_connectors import (
    DatabaseConnector,
    LocalDuckDBConnector,
)


class PipelineModel:
    """Base class representing a model within the MRL pipeline.

    This class encapsulates both metadata and execution logic for a model,
    enabling it to be integrated as a Prefect task.

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
        connector: Optional[DatabaseConnector] = None,
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
        self.connector = connector or LocalDuckDBConnector()

    def run(self, *runner_args: Any, **runner_kwargs: Any) -> dict[str, Any]:
        """Execute the model and return serializable metadata about the run."""

        kwargs = dict(runner_kwargs)
        kwargs.setdefault("connector", self.connector)

        conn = self.runner(*runner_args, **kwargs)

        result: dict[str, Any] = {
            "status": "success",
            "table": self.name,
        }

        if isinstance(conn, DuckDBPyConnection):
            try:
                row_count = conn.execute(
                    f"SELECT count(*) FROM {self.name}"
                ).fetchone()[0]  # noqa: S608
                result["row_count"] = row_count
            except Exception as exc:  # noqa: BLE001
                result["status"] = "inspection_failed"
                result["error"] = str(exc)
            finally:
                conn.close()
        else:
            result["status"] = "no_connection"

        return result

    def build(self) -> Callable[..., dict[str, Any]]:
        """Create a Prefect task that encapsulates the model's execution logic."""

        @task(name=self.name, description=self.description)
        def run_model(*runner_args: Any, **runner_kwargs: Any) -> dict[str, Any]:
            """Execute the model and log simple telemetry."""

            result = self.run(*runner_args, **runner_kwargs)

            logger = get_run_logger()
            status = result.get("status")
            row_count = result.get("row_count")
            logger.info("%s status=%s row_count=%s", self.name, status, row_count)

            if error := result.get("error"):
                logger.warning("%s inspection error: %s", self.name, error)

            return result

        return run_model
