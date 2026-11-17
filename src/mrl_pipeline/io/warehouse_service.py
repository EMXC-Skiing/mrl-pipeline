"""Abstract interfaces and shared helpers for warehouse backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

from pandas import DataFrame

from mrl_pipeline.io.table_version_ref import TableVersionRef
from mrl_pipeline.io.utils import _default_timestamp
from mrl_pipeline.settings import settings

AuthCredentials = Optional[Union[str, Path, Mapping[str, Any]]]


class BaseIOService(ABC):
    """Shared context for IO services."""

    def __init__(
        self,
        *,
        env: Optional[str] = None,
        auth_credentials: AuthCredentials = None,
        **kwargs: Any,
    ) -> None:
        self._env = env or settings.environment_name
        self._auth_credentials = auth_credentials
        self._extra_options: Dict[str, Any] = dict(kwargs)

    @property
    def env(self) -> str:
        return self._env

    @property
    def auth_credentials(self) -> AuthCredentials:
        return self._auth_credentials

    @property
    def extra_options(self) -> Mapping[str, Any]:
        return MappingProxyType(self._extra_options)


class PrepTableService(BaseIOService):
    """Interface for services that expose prep-table reads."""

    @abstractmethod
    def get_prep_table(
        self,
        sheet_name: str,
        *,
        worksheet_name: Optional[str] = None,
        return_url: bool = False,
    ) -> Union[DataFrame, Tuple[DataFrame, str]]:
        """Load a single prep table by index or name."""

    @abstractmethod
    def iter_prep_tables(
        self,
        sheet_name: str,
    ) -> Iterable[Tuple[str, DataFrame]]:
        """Yield every table within the key``sheet_name`` as ``(title, DataFrame)``."""


class WarehouseTableService(BaseIOService):
    """Declare a minimal contract for services that expose warehouse-like tables."""

    @abstractmethod
    def put_warehouse_table(
        self,
        table_name: str,
        data: Union[DataFrame, Iterable[DataFrame]],
        *,
        lifecycle_management: bool = True,
    ) -> None:
        """Write one or more DataFrames to the warehouse under ``table_name``."""

    @abstractmethod
    def get_warehouse_table(
        self,
        table_name: str,
        *,
        return_url: bool = False,
    ) -> Union[DataFrame, Tuple[DataFrame, str]]:
        """Read the most recent warehouse snapshot for ``table_name``."""

    @abstractmethod
    def promote_temp_tables(self, *, archive_previous: bool = True) -> None:
        """Finalize staged tables, optionally archiving previous primaries."""


class WarehouseService(WarehouseTableService, PrepTableService):
    """Combined interface for warehouse and prep-table service APIs."""


__all__ = [
    "AuthCredentials",
    "BaseIOService",
    "PrepTableService",
    "WarehouseTableService",
    "WarehouseService",
    "TableVersionRef",
    "_default_timestamp",
]
