from __future__ import annotations

import math
import re
import threading

from fastapi import APIRouter, Depends, Query
from tfbpapi import VirtualDB

from app.dataset_catalog import DATASET_CATALOG_BY_DB_NAME
from app.dependencies import get_vdb, get_vdb_lock
from app.schemas import (
    FilterOption,
    IntersectionCell,
    IntersectionRequest,
    PaginatedResponse,
    QueryRequest,
)

router = APIRouter(tags=["query"])

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
_SAMPLE_IDENTIFIER_CANDIDATES = ("sample_id", "sra_accession")


def _validate_identifier(name: str) -> str:
    """Validate that a string is a safe SQL identifier."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


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


@router.post("/query", response_model=PaginatedResponse)
def execute_query(
    body: QueryRequest,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> PaginatedResponse:
    """Execute parameterized SQL with pagination."""
    offset = (body.page - 1) * body.page_size

    count_sql = f"SELECT COUNT(*) AS total FROM ({body.sql}) AS _q"
    paginated_sql = (
        f"SELECT * FROM ({body.sql}) AS _q "
        f"LIMIT {body.page_size} OFFSET {offset}"
    )

    with lock:
        total_df = vdb.query(count_sql, **body.params)
        total = int(total_df["total"].iloc[0])
        data_df = vdb.query(paginated_sql, **body.params)

    records = data_df.to_dict(orient="records")
    return PaginatedResponse(
        data=records,
        total=total,
        page=body.page,
        page_size=body.page_size,
        has_next=(offset + body.page_size) < total,
    )


@router.get("/tables/{table}/sample", response_model=list[dict])
def sample_rows(
    table: str,
    n: int = Query(default=10, ge=1, le=1000),
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[dict]:
    """Get the first N rows from a table/view."""
    table = _validate_identifier(table)
    with lock:
        df = vdb.query(f"SELECT * FROM {table} LIMIT {n}")
    return df.to_dict(orient="records")


@router.get("/tables/{table}/distinct/{field}", response_model=list)
def distinct_values(
    table: str,
    field: str,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list:
    """Get distinct values for a field in a table/view."""
    table = _validate_identifier(table)
    field = _validate_identifier(field)
    with lock:
        df = vdb.query(
            f"SELECT DISTINCT {field} FROM {table} "
            f"WHERE {field} IS NOT NULL ORDER BY {field}"
        )
    return df[field].tolist()


@router.get("/tables/{table}/count", response_model=int)
def row_count(
    table: str,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> int:
    """Get row count for a table/view."""
    table = _validate_identifier(table)
    with lock:
        df = vdb.query(f"SELECT COUNT(*) AS cnt FROM {table}")
    return int(df["cnt"].iloc[0])


@router.get(
    "/active-set/filter-options/{table}", response_model=list[FilterOption]
)
def filter_options(
    table: str,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[FilterOption]:
    """Get filter metadata for all columns in a _meta view (for filter UI)."""
    table = _validate_identifier(table)
    with lock:
        fields = vdb.get_fields(table)
        type_map = _column_type_map(vdb, table)
        result: list[FilterOption] = []
        for field in fields:
            if field == "sample_id":
                continue
            if _is_numeric_column_type(type_map.get(field)):
                range_df = vdb.query(
                    f"SELECT MIN({field}) AS min_value, MAX({field}) AS max_value "
                    f"FROM {table} WHERE {field} IS NOT NULL"
                )
                min_value = _normalize_numeric_stat(range_df["min_value"].iloc[0])
                max_value = _normalize_numeric_stat(range_df["max_value"].iloc[0])
                if min_value is None and max_value is None:
                    continue
                result.append(
                    FilterOption(
                        field=field,
                        kind="numeric",
                        min_value=min_value,
                        max_value=max_value,
                    )
                )
                continue

            df = vdb.query(
                f"SELECT DISTINCT {field} FROM {table} "
                f"WHERE {field} IS NOT NULL ORDER BY {field}"
            )
            values = [str(v) for v in df[field].tolist()]
            if values:
                result.append(
                    FilterOption(field=field, kind="categorical", values=values)
                )
    return result


@router.post("/active-set/intersection", response_model=list[IntersectionCell])
def compute_intersection(
    body: IntersectionRequest,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[IntersectionCell]:
    """Compute pairwise regulator overlap between selected datasets."""
    dataset_names = [_validate_identifier(d) for d in body.datasets]

    # Build regulator sets for each dataset
    regulator_sets: dict[str, set[str]] = {}
    with lock:
        for ds_name in dataset_names:
            base_meta_table = _validate_identifier(f"{ds_name}_meta")
            base_fields = vdb.get_fields(base_meta_table)
            where_sql = _build_where_sql(body, ds_name)

            resolved = False
            checked_tables: list[str] = []
            base_sample_field: str | None = None

            for table in _candidate_regulator_tables(ds_name):
                table = _validate_identifier(table)
                checked_tables.append(table)
                try:
                    fields = vdb.get_fields(table)
                except Exception:
                    continue

                try:
                    regulator_field = _resolve_regulator_identifier(fields, table)
                except ValueError:
                    continue

                if table == base_meta_table:
                    sql = (
                        f"SELECT DISTINCT {regulator_field} AS regulator "
                        f"FROM {table}{where_sql}"
                    )
                else:
                    if base_sample_field is None:
                        base_sample_field = _resolve_sample_identifier(
                            base_fields, base_meta_table
                        )
                    source_sample_field = _resolve_join_sample_identifier(
                        fields=fields,
                        table=table,
                        preferred=base_sample_field,
                    )
                    filtered_samples_sql = (
                        f"SELECT DISTINCT {base_sample_field} AS __sample_id "
                        f"FROM {base_meta_table}{where_sql}"
                    )
                    sql = (
                        f"SELECT DISTINCT src.{regulator_field} AS regulator "
                        f"FROM {table} AS src "
                        f"JOIN ({filtered_samples_sql}) AS f "
                        f"ON CAST(src.{source_sample_field} AS VARCHAR) = "
                        f"CAST(f.__sample_id AS VARCHAR)"
                    )

                df = vdb.query(sql)
                regulator_sets[ds_name] = set(
                    df["regulator"].dropna().astype(str).tolist()
                )
                resolved = True
                break

            if not resolved:
                checked = ", ".join(checked_tables) if checked_tables else "<none>"
                raise ValueError(
                    f"No regulator metadata found for dataset '{ds_name}'. "
                    f"Checked tables: {checked}"
                )

    # Compute pairwise intersections
    cells: list[IntersectionCell] = []
    for i, ds_a in enumerate(dataset_names):
        for j, ds_b in enumerate(dataset_names):
            if j < i:
                continue
            if i == j:
                count = len(regulator_sets.get(ds_a, set()))
            else:
                set_a = regulator_sets.get(ds_a, set())
                set_b = regulator_sets.get(ds_b, set())
                count = len(set_a & set_b)
            cells.append(IntersectionCell(row=ds_a, col=ds_b, count=count))

    return cells
