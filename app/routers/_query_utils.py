"""Shared query utilities for SQL validation and filter building."""

from __future__ import annotations

import math
import re

from tfbpapi import VirtualDB

from app.dataset_catalog import DATASET_CATALOG_BY_DB_NAME
from app.schemas import IntersectionRequest

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NUMERIC_TYPE_TOKENS = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
    "REAL",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "NUMERIC",
)
_REGULATOR_IDENTIFIER_CANDIDATES = (
    "regulator",
    "tf",
    "regulator_symbol",
    "regulator_locus_tag",
    "gene_symbol",
)
_SAMPLE_IDENTIFIER_CANDIDATES = ("sample_id", "sra_accession", "id")


def _validate_identifier(name: str) -> str:
    """Validate that a string is a safe SQL identifier."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def _qi(name: str) -> str:
    """Quote a validated identifier for use in SQL (handles reserved keywords)."""
    return f'"{name}"'


def _resolve_regulator_identifier(fields: list[str], table: str) -> str:
    """Pick the best available regulator identifier field."""
    for candidate in _REGULATOR_IDENTIFIER_CANDIDATES:
        if candidate in fields:
            return candidate

    available = ", ".join(fields[:12]) if fields else "<none>"
    raise ValueError(
        f"No regulator identifier field found in '{table}'. "
        f"Available fields: {available}"
    )


def _resolve_sample_identifier(fields: list[str], table: str) -> str:
    """Pick the available sample identifier field for join/filter operations."""
    for fallback in _SAMPLE_IDENTIFIER_CANDIDATES:
        if fallback in fields:
            return fallback

    available = ", ".join(fields[:12]) if fields else "<none>"
    raise ValueError(
        f"No sample identifier field found in '{table}'. "
        f"Available fields: {available}"
    )


def _resolve_join_sample_identifier(
    fields: list[str],
    table: str,
    preferred: str | None,
) -> str:
    """Resolve sample identifier for joins, preferring the base-table identifier."""
    if preferred and preferred in fields:
        return preferred
    return _resolve_sample_identifier(fields, table)


def _is_numeric_column_type(column_type: str | None) -> bool:
    """Return whether a DuckDB column type should be treated as numeric."""
    if not column_type:
        return False
    upper = str(column_type).upper()
    return any(token in upper for token in _NUMERIC_TYPE_TOKENS)


def _column_type_map(vdb: VirtualDB, table: str) -> dict[str, str]:
    """Return map of column name -> DuckDB type for a table."""
    describe_df = vdb.describe(table)
    if describe_df.empty:
        return {}

    if "column_name" not in describe_df or "column_type" not in describe_df:
        return {}

    return {
        str(row["column_name"]): str(row["column_type"])
        for _, row in describe_df.iterrows()
    }


def _build_where_sql(body: IntersectionRequest, dataset_name: str) -> str:
    """Build SQL WHERE clause for categorical + numeric filters."""
    where_clauses: list[str] = []

    categorical_filters = body.filters.get(dataset_name, {})
    for field, values in categorical_filters.items():
        field = _validate_identifier(field)
        escaped = [str(v).replace("'", "''") for v in values if str(v) != ""]
        if not escaped:
            continue
        value_list = ", ".join(f"'{v}'" for v in escaped)
        where_clauses.append(f"{field} IN ({value_list})")

    numeric_filters = body.numeric_filters.get(dataset_name, {})
    for field, numeric_filter in numeric_filters.items():
        field = _validate_identifier(field)
        min_value = numeric_filter.min_value
        max_value = numeric_filter.max_value

        if min_value is not None:
            where_clauses.append(f"{field} >= {float(min_value)}")
        if max_value is not None:
            where_clauses.append(f"{field} <= {float(max_value)}")
        if (
            min_value is not None
            and max_value is not None
            and float(min_value) > float(max_value)
        ):
            raise ValueError(
                f"Invalid numeric filter for '{dataset_name}.{field}': "
                f"min_value {min_value} is greater than max_value {max_value}"
            )

    return f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""


def _candidate_regulator_tables(dataset_name: str) -> list[str]:
    """Build ordered list of metadata tables to search for regulator identifiers."""
    candidates: list[str] = [f"{dataset_name}_meta"]
    item = DATASET_CATALOG_BY_DB_NAME.get(dataset_name)
    if not item:
        return candidates

    for supplemental in item.supplemental_configs:
        candidates.extend(
            [
                f"{supplemental.db_name}_meta",
                supplemental.db_name,
            ]
        )
    return candidates


def _normalize_numeric_stat(value: object) -> float | None:
    """Convert MIN/MAX result value to float, treating NaN as None."""
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(numeric) else numeric
