from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class PaginatedResponse(BaseModel, frozen=True):
    """Paginated query result."""

    data: list[dict]
    total: int
    page: int
    page_size: int
    has_next: bool


class QueryRequest(BaseModel, frozen=True):
    """Request body for the POST /query endpoint."""

    sql: str
    params: dict[str, str | int | float | bool] = Field(default_factory=dict)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=100, ge=1, le=10000)


class DatasetInfo(BaseModel, frozen=True):
    """Metadata about a single dataset registered in VirtualDB."""

    db_name: str
    repo_id: str
    config_name: str
    is_comparative: bool


class DatasetCatalogEntry(BaseModel, frozen=True):
    """Dataset available for Active Set configuration."""

    id: str
    name: str
    repo_id: str
    config_name: str
    db_name: str
    sample_id_field: str
    estimated_rows: int | None = None
    num_columns: int | None = None
    column_names: list[str] = Field(default_factory=list)
    selectable: bool = True
    is_active: bool = False
    unsupported_reason: str | None = None


class ColumnInfo(BaseModel, frozen=True):
    """Column name and type from a table description."""

    column_name: str
    column_type: str


class FilterOption(BaseModel, frozen=True):
    """Distinct values available for a filterable field."""

    field: str
    kind: Literal["categorical", "numeric"] = "categorical"
    values: list[str] = Field(default_factory=list)
    min_value: float | None = None
    max_value: float | None = None


class NumericRangeFilter(BaseModel, frozen=True):
    """Inclusive numeric range filter for a metadata field."""

    min_value: float | None = None
    max_value: float | None = None


class IntersectionRequest(BaseModel, frozen=True):
    """Request body for the POST /active-set/intersection endpoint."""

    datasets: list[str]
    filters: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    numeric_filters: dict[str, dict[str, NumericRangeFilter]] = Field(
        default_factory=dict
    )


class ActiveSetConfigSyncRequest(BaseModel, frozen=True):
    """Request body for syncing selected datasets into the VirtualDB YAML config."""

    dataset_ids: list[str] = Field(default_factory=list)


class ActiveSetConfigSyncResponse(BaseModel, frozen=True):
    """Response for active-set config sync and VirtualDB reload."""

    config_path: str
    active_dataset_ids: list[str]
    active_dataset_count: int


class IntersectionCell(BaseModel, frozen=True):
    """A single cell in the pairwise intersection matrix."""

    row: str
    col: str
    count: int


class HealthResponse(BaseModel, frozen=True):
    """Health check response."""

    status: str
    tables_registered: int
