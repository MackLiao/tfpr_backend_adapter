"""Analysis endpoints for binding, perturbation, correlation, and source summary."""

from __future__ import annotations

import threading

from fastapi import APIRouter, Depends, HTTPException
from tfbpapi import VirtualDB

from app.dependencies import get_vdb, get_vdb_lock
from app.routers._query_utils import (
    _build_where_sql,
    _column_type_map,
    _is_numeric_column_type,
    _normalize_numeric_stat,
    _resolve_regulator_identifier,
    _resolve_sample_identifier,
    _validate_identifier,
)
from app.schemas import (
    AnalysisDataResponse,
    AnalysisRequest,
    CorrelationMatrixResponse,
    CorrelationRequest,
    FilterOption,
    FilterOptionsRequest,
    FilterOptionsResponse,
    SourceSummaryEntry,
)

router = APIRouter(tags=["analysis"])


@router.get("/analysis/source-summary/{db_name}", response_model=SourceSummaryEntry)
def source_summary(
    db_name: str,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> SourceSummaryEntry:
    """Get summary statistics for a single dataset."""
    db_name = _validate_identifier(db_name)
    meta_table = f"{db_name}_meta"

    with lock:
        # Verify tables exist
        tables = vdb.tables()
        if db_name not in tables:
            raise HTTPException(status_code=400, detail=f"Dataset '{db_name}' not found")
        if meta_table not in tables:
            raise HTTPException(
                status_code=400, detail=f"Metadata table '{meta_table}' not found"
            )

        # Get basic counts
        total_rows_df = vdb.query(f"SELECT COUNT(*) AS cnt FROM {db_name}")
        total_rows = int(total_rows_df["cnt"].iloc[0])

        # Get column count
        fields = vdb.get_fields(db_name)
        column_count = len(fields)

        # Get regulator count
        meta_fields = vdb.get_fields(meta_table)
        try:
            regulator_field = _resolve_regulator_identifier(meta_fields, meta_table)
            regulator_count_df = vdb.query(
                f"SELECT COUNT(DISTINCT {regulator_field}) AS cnt FROM {meta_table}"
            )
            regulator_count = int(regulator_count_df["cnt"].iloc[0])
        except ValueError:
            regulator_count = 0

        # Get target count (if target_locus_tag or similar exists)
        target_count = 0
        target_candidates = ["target_locus_tag", "target", "gene_locus_tag"]
        for candidate in target_candidates:
            if candidate in fields:
                target_count_df = vdb.query(
                    f"SELECT COUNT(DISTINCT {candidate}) AS cnt FROM {db_name}"
                )
                target_count = int(target_count_df["cnt"].iloc[0])
                break

        # Get sample count
        try:
            sample_field = _resolve_sample_identifier(meta_fields, meta_table)
            sample_count_df = vdb.query(
                f"SELECT COUNT(DISTINCT {sample_field}) AS cnt FROM {meta_table}"
            )
            sample_count = int(sample_count_df["cnt"].iloc[0])
        except ValueError:
            sample_count = 0

        # Get metadata field info (for filter UI)
        type_map = _column_type_map(vdb, meta_table)
        metadata_fields: list[FilterOption] = []
        for field in meta_fields:
            if field in ("sample_id", "sra_accession"):
                continue
            if _is_numeric_column_type(type_map.get(field)):
                range_df = vdb.query(
                    f"SELECT MIN({field}) AS min_value, MAX({field}) AS max_value "
                    f"FROM {meta_table} WHERE {field} IS NOT NULL"
                )
                min_value = _normalize_numeric_stat(range_df["min_value"].iloc[0])
                max_value = _normalize_numeric_stat(range_df["max_value"].iloc[0])
                if min_value is None and max_value is None:
                    continue
                metadata_fields.append(
                    FilterOption(
                        field=field,
                        kind="numeric",
                        min_value=min_value,
                        max_value=max_value,
                    )
                )
            else:
                distinct_df = vdb.query(
                    f"SELECT COUNT(DISTINCT {field}) AS cnt "
                    f"FROM {meta_table} WHERE {field} IS NOT NULL"
                )
                distinct_count = int(distinct_df["cnt"].iloc[0])
                if distinct_count > 0 and distinct_count <= 100:
                    values_df = vdb.query(
                        f"SELECT DISTINCT {field} FROM {meta_table} "
                        f"WHERE {field} IS NOT NULL ORDER BY {field}"
                    )
                    values = [str(v) for v in values_df[field].tolist()]
                    metadata_fields.append(
                        FilterOption(field=field, kind="categorical", values=values)
                    )

        # Get repo_id and config_name from VirtualDB's internal map
        db_name_map = getattr(vdb, "_db_name_map", {})
        config_info = db_name_map.get(db_name)
        if isinstance(config_info, tuple) and len(config_info) >= 2:
            # VirtualDB stores as (repo_id, config_name) tuple
            repo_id = config_info[0]
            config_name = config_info[1]
        elif isinstance(config_info, dict):
            repo_id = config_info.get("repo_id", "unknown")
            config_name = config_info.get("config_name", "unknown")
        else:
            repo_id = "unknown"
            config_name = "unknown"

        # Infer dataset type from db_name
        db_name_lower = db_name.lower()
        if any(
            keyword in db_name_lower
            for keyword in [
                "binding",
                "chip",
                "calling_cards",
                "occupancy",
                "chec",
                "chip-exo",
            ]
        ):
            dataset_type = "Binding"
        elif any(
            keyword in db_name_lower
            for keyword in [
                "perturb",
                "expression",
                "rna",
                "knockout",
                "deletion",
                "overexpression",
                "comparative",
                "degron",
            ]
        ):
            dataset_type = "Perturbation"
        else:
            dataset_type = "Expression"

    return SourceSummaryEntry(
        db_name=db_name,
        repo_id=repo_id,
        config_name=config_name,
        dataset_type=dataset_type,
        total_rows=total_rows,
        regulator_count=regulator_count,
        target_count=target_count,
        sample_count=sample_count,
        column_count=column_count,
        metadata_fields=metadata_fields,
    )


@router.post("/analysis/binding", response_model=list[AnalysisDataResponse])
def binding_analysis(
    body: AnalysisRequest,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[AnalysisDataResponse]:
    """Query binding data from active binding datasets."""
    dataset_names = [_validate_identifier(d) for d in body.datasets]
    if not dataset_names:
        return []

    results: list[AnalysisDataResponse] = []

    with lock:
        for ds_name in dataset_names:
            # Verify table exists
            if ds_name not in vdb.tables():
                raise HTTPException(
                    status_code=400, detail=f"Dataset '{ds_name}' not found"
                )

            # Build WHERE clause from filters
            where_sql = _build_where_sql(body, ds_name)

            # Count total (for pagination)
            count_sql = f"SELECT COUNT(*) AS total FROM {ds_name}{where_sql}"
            total_df = vdb.query(count_sql)
            total = int(total_df["total"].iloc[0])

            if total == 0:
                # No results for this dataset
                results.append(
                    AnalysisDataResponse(
                        db_name=ds_name,
                        data=[],
                        total=0,
                        page=body.page,
                        page_size=body.page_size,
                        has_next=False,
                        columns=[],
                    )
                )
                continue

            # Get columns
            columns = vdb.get_fields(ds_name)

            # Query paginated data
            offset = (body.page - 1) * body.page_size
            data_sql = (
                f"SELECT * FROM {ds_name}{where_sql} "
                f"LIMIT {body.page_size} OFFSET {offset}"
            )
            data_df = vdb.query(data_sql)

            records = data_df.to_dict(orient="records")
            has_next = (offset + body.page_size) < total

            results.append(
                AnalysisDataResponse(
                    db_name=ds_name,
                    data=records,
                    total=total,
                    page=body.page,
                    page_size=body.page_size,
                    has_next=has_next,
                    columns=columns,
                )
            )

    return results


@router.post("/analysis/perturbation", response_model=list[AnalysisDataResponse])
def perturbation_analysis(
    body: AnalysisRequest,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[AnalysisDataResponse]:
    """Query perturbation data from active perturbation datasets."""
    # Same implementation as binding_analysis - the semantic distinction is in the caller
    dataset_names = [_validate_identifier(d) for d in body.datasets]
    if not dataset_names:
        return []

    results: list[AnalysisDataResponse] = []

    with lock:
        for ds_name in dataset_names:
            # Verify table exists
            if ds_name not in vdb.tables():
                raise HTTPException(
                    status_code=400, detail=f"Dataset '{ds_name}' not found"
                )

            # Build WHERE clause from filters
            where_sql = _build_where_sql(body, ds_name)

            # Count total (for pagination)
            count_sql = f"SELECT COUNT(*) AS total FROM {ds_name}{where_sql}"
            total_df = vdb.query(count_sql)
            total = int(total_df["total"].iloc[0])

            if total == 0:
                # No results for this dataset
                results.append(
                    AnalysisDataResponse(
                        db_name=ds_name,
                        data=[],
                        total=0,
                        page=body.page,
                        page_size=body.page_size,
                        has_next=False,
                        columns=[],
                    )
                )
                continue

            # Get columns
            columns = vdb.get_fields(ds_name)

            # Query paginated data
            offset = (body.page - 1) * body.page_size
            data_sql = (
                f"SELECT * FROM {ds_name}{where_sql} "
                f"LIMIT {body.page_size} OFFSET {offset}"
            )
            data_df = vdb.query(data_sql)

            records = data_df.to_dict(orient="records")
            has_next = (offset + body.page_size) < total

            results.append(
                AnalysisDataResponse(
                    db_name=ds_name,
                    data=records,
                    total=total,
                    page=body.page,
                    page_size=body.page_size,
                    has_next=has_next,
                    columns=columns,
                )
            )

    return results


@router.post("/analysis/correlation", response_model=CorrelationMatrixResponse)
def correlation_matrix(
    body: CorrelationRequest,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> CorrelationMatrixResponse:
    """Compute pairwise correlation matrix for a dataset."""
    db_name = _validate_identifier(body.db_name)
    value_column = _validate_identifier(body.value_column)

    with lock:
        # Verify table exists
        if db_name not in vdb.tables():
            raise HTTPException(status_code=400, detail=f"Dataset '{db_name}' not found")

        fields = vdb.get_fields(db_name)
        if value_column not in fields:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{value_column}' not found in '{db_name}'",
            )

        # Determine grouping column
        if body.group_by == "regulator":
            try:
                meta_table = f"{db_name}_meta"
                meta_fields = vdb.get_fields(meta_table)
                group_column = _resolve_regulator_identifier(meta_fields, meta_table)
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot resolve regulator identifier: {str(e)}",
                )
        elif body.group_by == "sample":
            group_column = "sample_id"
            if group_column not in fields:
                raise HTTPException(
                    status_code=400,
                    detail=f"Column '{group_column}' not found in '{db_name}'",
                )
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid group_by value: '{body.group_by}'. Must be 'regulator' or 'sample'.",
            )

        # Get top N items by frequency
        top_items_sql = (
            f"SELECT {group_column}, COUNT(*) AS cnt "
            f"FROM {db_name} "
            f"WHERE {group_column} IS NOT NULL AND {value_column} IS NOT NULL "
            f"GROUP BY {group_column} "
            f"ORDER BY cnt DESC "
            f"LIMIT {body.max_items}"
        )
        top_items_df = vdb.query(top_items_sql)

        if top_items_df.empty:
            return CorrelationMatrixResponse(
                db_name=db_name,
                labels=[],
                cells=[],
                method=body.method,
            )

        labels = [str(item) for item in top_items_df[group_column].tolist()]

        # Compute pairwise correlations
        # For each pair of items, compute correlation of their value_column vectors
        from app.schemas import CorrelationCell

        cells: list[CorrelationCell] = []

        for i, label_a in enumerate(labels):
            for j, label_b in enumerate(labels):
                if j < i:
                    continue

                if i == j:
                    # Self-correlation is always 1.0
                    cells.append(
                        CorrelationCell(row=label_a, col=label_b, value=1.0)
                    )
                else:
                    # Compute correlation between the two items
                    # Get vectors for both items
                    vectors_sql = f"""
                        SELECT
                            {group_column} AS item,
                            {value_column} AS value
                        FROM {db_name}
                        WHERE {group_column} IN ('{label_a}', '{label_b}')
                          AND {value_column} IS NOT NULL
                        ORDER BY {group_column}
                    """
                    vectors_df = vdb.query(vectors_sql)

                    if vectors_df.empty or len(vectors_df) < 2:
                        cells.append(
                            CorrelationCell(row=label_a, col=label_b, value=0.0)
                        )
                        continue

                    # Use DuckDB's CORR aggregate function
                    # Pivot the data to compute correlation
                    corr_sql = f"""
                        WITH pairs AS (
                            SELECT
                                a.{value_column} AS val_a,
                                b.{value_column} AS val_b
                            FROM (
                                SELECT {value_column}
                                FROM {db_name}
                                WHERE {group_column} = '{label_a}'
                                  AND {value_column} IS NOT NULL
                            ) a
                            CROSS JOIN (
                                SELECT {value_column}
                                FROM {db_name}
                                WHERE {group_column} = '{label_b}'
                                  AND {value_column} IS NOT NULL
                            ) b
                        )
                        SELECT CORR(val_a, val_b) AS correlation
                        FROM pairs
                    """
                    corr_df = vdb.query(corr_sql)

                    if corr_df.empty or corr_df["correlation"].isna().iloc[0]:
                        correlation = 0.0
                    else:
                        correlation = float(corr_df["correlation"].iloc[0])

                    cells.append(
                        CorrelationCell(row=label_a, col=label_b, value=correlation)
                    )

    return CorrelationMatrixResponse(
        db_name=db_name,
        labels=labels,
        cells=cells,
        method=body.method,
    )


@router.post("/analysis/filter-options", response_model=FilterOptionsResponse)
def filter_options(
    body: FilterOptionsRequest,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> FilterOptionsResponse:
    """Get unique values for a column across multiple datasets."""
    dataset_names = [_validate_identifier(d) for d in body.datasets]
    column = _validate_identifier(body.column)

    if not dataset_names:
        return FilterOptionsResponse(column=column, values=[])

    unique_values: set[str] = set()

    with lock:
        for ds_name in dataset_names:
            # Verify table exists
            if ds_name not in vdb.tables():
                continue

            # Check if column exists in this dataset
            fields = vdb.get_fields(ds_name)
            if column not in fields:
                continue

            # Query distinct values for this column
            query_sql = (
                f"SELECT DISTINCT {column} "
                f"FROM {ds_name} "
                f"WHERE {column} IS NOT NULL "
                f"ORDER BY {column}"
            )
            result_df = vdb.query(query_sql)

            if not result_df.empty:
                # Add values to set (automatically deduplicates)
                unique_values.update(str(val) for val in result_df[column].tolist())

    # Return sorted list
    return FilterOptionsResponse(
        column=column,
        values=sorted(list(unique_values)),
    )
