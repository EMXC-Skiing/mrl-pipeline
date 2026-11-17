from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence, Set

from pandas import DataFrame

from mrl_pipeline.io.table_version_ref import TableVersionRef
from mrl_pipeline.settings import settings


@dataclass(frozen=True)
class TableSummary:
    """
    Keep this intentionally small and stable across backends.
    You can extend later (null counts, distincts, min/max, etc).
    """

    row_count: Optional[int] = None
    column_count: Optional[int] = None
    columns: Optional[Sequence[str]] = None
    dtypes: Optional[Mapping[str, str]] = None  # backend-normalized dtype strings
    notes: Optional[str] = None  # e.g., "row_count estimated" / "metadata unavailable"


class WarehouseTable(ABC):
    """Abstract lifecycle handle for one logical table in a specific backend.

    A ``WarehouseTable`` instance binds a logical table node (``base_name``) to
    run-scoped version names and tracks state transitions across write/promotion
    phases.
    """

    VALID_STATES: Set[str] = {"empty", "temp", "promoted"}

    def __init__(self, table_name: str, run, env: Optional[str] = None, **kwargs):
        """Initialize a table handle for the current run/environment context.

        Args:
            table_name: User-provided table identifier used by concrete classes.
            run: Run context object (must expose ``timestamp`` at minimum).
            env: Optional environment override; defaults to configured environment.
            **kwargs: Backend-specific initialization options.
        """
        self.table_name = table_name
        self.run = run
        self.env = env if env is not None else settings.environment_name
        self._run_state = "empty"
        self._address = ""
        self.versions = TableVersionRef(
            table_name=self.base_name,
            env=self.env,
            timestamp=run.timestamp,
        )

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"backend={self.backend!r} address={self.address!r}>"
        )

    @property
    def run_state(self) -> str:
        """Current lifecycle state (for example: ``empty``, ``temp``, ``promoted``)."""
        return self._run_state

    @property
    @abstractmethod
    def base_name(self) -> str:
        """Logical name for the table node in your DAG (stable identifier)."""
        raise NotImplementedError

    @property
    @abstractmethod
    def backend(self) -> str:
        """Backend name, e.g. 'duckdb', 'motherduck', 'gsheets'."""
        raise NotImplementedError

    @property
    def address(self) -> str:
        """Current backend address/name for this table instance."""
        return self._address

    def register(self, data=None, *args, **kwargs) -> str:
        """Run backend register/write logic and transition ``empty`` -> ``temp``.

        Returns:
            The staged backend address.
        """
        staged_address = self._register_impl(data=data, *args, **kwargs)
        self._transition(
            to_state="temp",
            expected_from={"empty"},
            address=staged_address,
        )
        return self.address

    def promote(self, *args, **kwargs) -> str:
        """Run backend promotion logic and transition ``temp`` -> ``promoted``.

        Returns:
            The promoted backend address.
        """
        promoted_address = self._promote_impl(*args, **kwargs)
        self._transition(
            to_state="promoted",
            expected_from={"temp"},
            address=promoted_address,
        )
        return self.address

    @abstractmethod
    def _register_impl(self, data=None, *args, **kwargs) -> str:
        """Perform backend-specific staging/write and return staged address."""

    @abstractmethod
    def _promote_impl(self, *args, **kwargs) -> str:
        """Perform backend-specific promotion and return promoted address."""

    def _transition(
        self,
        *,
        to_state: str,
        expected_from: Set[str],
        address: Optional[str] = None,
    ) -> None:
        """Validate and apply a lifecycle state transition."""
        if to_state not in self.VALID_STATES:
            valid = ", ".join(sorted(self.VALID_STATES))
            raise ValueError(
                f"Invalid target state '{to_state}'. Valid states: {valid}."
            )
        if self._run_state not in expected_from:
            expected = ", ".join(sorted(expected_from))
            raise RuntimeError(
                f"Invalid state transition from '{self._run_state}' to '{to_state}'. "
                f"Expected current state in: {expected}.",
            )
        self._run_state = to_state
        if address is not None:
            self._address = address

    @abstractmethod
    def as_dataframe(self) -> DataFrame:
        """
        Materialize the resource into a pandas DataFrame.
        """
        raise NotImplementedError

    @abstractmethod
    def summarize(self) -> TableSummary:
        """
        Return lightweight stats/metadata. Implementations may return partial info.
        """
        raise NotImplementedError
