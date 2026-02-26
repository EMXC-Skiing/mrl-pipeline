# mrl_pipeline/warehouse_operation.py
from __future__ import annotations

from typing import Any, Callable, Optional

from duckdb import DuckDBPyConnection
from prefect import get_run_logger, task

from mrl_pipeline.settings import settings  # adjust import
from mrl_pipeline.io.duckdb_service import make_table_ref
from mrl_pipeline.io.database_connectors import DatabaseConnector, LocalDuckDBConnector


class WarehouseOperation:
    def __init__(
        self,
        name: str,
        description: str,
        deps: list[str],
        runner: Callable,
        column_descriptions: Optional[dict[str, str]] = None,
        connector: Optional[DatabaseConnector] = None,
        schema_name: str | None = None,
        table_suffix: str | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.deps = deps
        self.runner = runner
        self.column_descriptions = column_descriptions or {}
        self.connector = connector or LocalDuckDBConnector()
        self.schema_name = (
            schema_name if schema_name is not None else settings.schema_name
        )
        self.table_suffix = (
            table_suffix if table_suffix is not None else settings.db_table_suffix
        )

    def _target_table_sql(self) -> str:
        tref = make_table_ref(
            base_name=self.name,
            schema_name=self.schema_name,
            table_suffix=self.table_suffix or "",
        )
        return tref.sql()

    def run(self, *runner_args: Any, **runner_kwargs: Any) -> dict[str, Any]:
        kwargs = dict(runner_kwargs)
        kwargs.setdefault("connector", self.connector)
        # Optional: also pass naming bits to runners so they can materialize consistently
        kwargs.setdefault("schema_name", self.schema_name)
        kwargs.setdefault("table_suffix", self.table_suffix)

        conn = self.runner(*runner_args, **kwargs)

        result: dict[str, Any] = {"status": "success", "table": self.name}

        if isinstance(conn, DuckDBPyConnection):
            try:
                table_sql = self._target_table_sql()
                row_count = conn.execute(
                    f"SELECT count(*) FROM {table_sql}"
                ).fetchone()[0]
                result["row_count"] = row_count
            except Exception as exc:
                result["status"] = "inspection_failed"
                result["error"] = str(exc)
            finally:
                conn.close()
        else:
            result["status"] = "no_connection"

        return result

    def build(self) -> Callable[..., dict[str, Any]]:
        @task(name=self.name, description=self.description)
        def run_model(*runner_args: Any, **runner_kwargs: Any) -> dict[str, Any]:
            result = self.run(*runner_args, **runner_kwargs)

            logger = get_run_logger()
            logger.info(
                "%s status=%s row_count=%s",
                self.name,
                result.get("status"),
                result.get("row_count"),
            )
            if error := result.get("error"):
                logger.warning("%s inspection error: %s", self.name, error)
            return result

        return run_model
