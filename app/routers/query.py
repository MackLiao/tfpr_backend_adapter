from __future__ import annotations

import threading

from fastapi import APIRouter, Depends, Query
from tfbpapi import VirtualDB

from app.dependencies import get_vdb, get_vdb_lock
from app.routers._query_utils import (
    _build_where_sql,
    _candidate_regulator_tables,
    _column_type_map,
    _is_numeric_column_type,
    _normalize_numeric_stat,
    _resolve_join_sample_identifier,
    _resolve_regulator_identifier,
    _resolve_sample_identifier,
    _validate_identifier,
)
from app.schemas import (
    FilterOption,
    IntersectionCell,
    IntersectionRequest,
    PaginatedResponse,
    QueryRequest,
)

router = APIRouter(tags=["query"])


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
        available_tables = set(vdb.tables())
        query_table = table

        if query_table not in available_tables and table.endswith("_meta"):
            dataset_name = table[: -len("_meta")]
            for candidate in _candidate_regulator_tables(dataset_name):
                candidate = _validate_identifier(candidate)
                if candidate in available_tables:
                    query_table = candidate
                    break
            else:
                if dataset_name in getattr(vdb, "_db_name_map", {}):
                    # Dataset is configured but not currently registered (e.g. unavailable
                    # source data); return no options instead of failing the selection UI.
                    return []

        fields = vdb.get_fields(query_table)
        type_map = _column_type_map(vdb, query_table)
        result: list[FilterOption] = []
        for field in fields:
            if field in {"sample_id", "sra_accession", "id"}:
                continue
            if _is_numeric_column_type(type_map.get(field)):
                range_df = vdb.query(
                    f"SELECT MIN({field}) AS min_value, MAX({field}) AS max_value "
                    f"FROM {query_table} WHERE {field} IS NOT NULL"
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
                f"SELECT DISTINCT {field} FROM {query_table} "
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
        available_tables = set(vdb.tables())
        configured_datasets = set(getattr(vdb, "_db_name_map", {}).keys())

        for ds_name in dataset_names:
            base_meta_table = _validate_identifier(f"{ds_name}_meta")
            base_meta_available = base_meta_table in available_tables
            base_fields = vdb.get_fields(base_meta_table) if base_meta_available else []
            where_sql = _build_where_sql(body, ds_name) if base_meta_available else ""

            resolved = False
            checked_tables: list[str] = []
            base_sample_field: str | None = None

            for table in _candidate_regulator_tables(ds_name):
                table = _validate_identifier(table)
                if table not in available_tables:
                    continue
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
                    if not where_sql:
                        # No filters on the base metadata table: use supplemental
                        # regulator metadata directly, avoiding unreliable sample-id joins.
                        sql = (
                            f"SELECT DISTINCT src.{regulator_field} AS regulator "
                            f"FROM {table} AS src"
                        )
                    elif base_meta_available:
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
                    else:
                        # Dataset is configured but no *_meta table was registered;
                        # fall back to direct regulator extraction from source table.
                        sql = (
                            f"SELECT DISTINCT src.{regulator_field} AS regulator "
                            f"FROM {table} AS src"
                        )

                df = vdb.query(sql)
                regulator_sets[ds_name] = set(
                    df["regulator"].dropna().astype(str).tolist()
                )
                resolved = True
                break

            if not resolved:
                if ds_name in configured_datasets and not checked_tables:
                    raise ValueError(
                        f"Dataset '{ds_name}' is configured but no SQL views were "
                        "registered for it. This usually means source parquet files "
                        "were not downloaded (for private/gated repos, ensure a valid "
                        "Hugging Face token is configured)."
                    )

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
