from __future__ import annotations

import pytest
from httpx import AsyncClient
import pandas as pd

from app.routers._query_utils import _resolve_sample_identifier


def test_resolve_sample_identifier_accepts_id() -> None:
    assert _resolve_sample_identifier(["id", "regulator_symbol"], "calling_cards_meta") == "id"


@pytest.mark.asyncio
async def test_execute_query(client: AsyncClient) -> None:
    resp = await client.post(
        "/api/v1/query",
        json={
            "sql": "SELECT * FROM harbison_meta",
            "params": {},
            "page": 1,
            "page_size": 10,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    assert "total" in data
    assert "page" in data
    assert "page_size" in data
    assert "has_next" in data
    assert data["page"] == 1


@pytest.mark.asyncio
async def test_sample_rows(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/tables/harbison/sample?n=5")
    assert resp.status_code == 200
    rows = resp.json()
    assert isinstance(rows, list)
    assert len(rows) > 0


@pytest.mark.asyncio
async def test_distinct_values(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/tables/harbison_meta/distinct/carbon_source")
    assert resp.status_code == 200
    values = resp.json()
    assert isinstance(values, list)


@pytest.mark.asyncio
async def test_row_count(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/tables/harbison/count")
    assert resp.status_code == 200
    count = resp.json()
    assert count == 42


@pytest.mark.asyncio
async def test_filter_options(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/active-set/filter-options/harbison_meta")
    assert resp.status_code == 200
    options = resp.json()
    assert isinstance(options, list)
    assert len(options) > 0

    for opt in options:
        assert "field" in opt
        assert "kind" in opt
        if opt["kind"] == "numeric":
            assert "min_value" in opt
            assert "max_value" in opt
        else:
            assert "values" in opt


@pytest.mark.asyncio
async def test_filter_options_configured_dataset_without_registered_tables_returns_empty(
    client: AsyncClient, test_app
) -> None:
    test_app.state.vdb._db_name_map["calling_cards"] = (
        "BrentLab/callingcards",
        "annotated_features",
    )
    resp = await client.get("/api/v1/active-set/filter-options/calling_cards_meta")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_intersection(client: AsyncClient) -> None:
    resp = await client.post(
        "/api/v1/active-set/intersection",
        json={"datasets": ["harbison", "kemmeren"], "filters": {}},
    )
    assert resp.status_code == 200
    cells = resp.json()
    assert isinstance(cells, list)
    # Upper triangle: (harbison,harbison), (harbison,kemmeren), (kemmeren,kemmeren) = 3 cells
    assert len(cells) == 3
    for cell in cells:
        assert "row" in cell
        assert "col" in cell
        assert "count" in cell


@pytest.mark.asyncio
async def test_intersection_supports_numeric_filters(
    client: AsyncClient, test_app
) -> None:
    resp = await client.post(
        "/api/v1/active-set/intersection",
        json={
            "datasets": ["harbison", "kemmeren"],
            "filters": {},
            "numeric_filters": {
                "harbison": {
                    "effect": {
                        "min_value": 0.25,
                        "max_value": 1.5,
                    }
                }
            },
        },
    )
    assert resp.status_code == 200
    cells = resp.json()
    assert len(cells) == 3

    issued_sql = " ".join(call.args[0] for call in test_app.state.vdb.query.call_args_list)
    assert "effect >=" in issued_sql
    assert "effect <=" in issued_sql


@pytest.mark.asyncio
async def test_intersection_handles_configured_dataset_without_registered_tables(
    client: AsyncClient, test_app
) -> None:
    test_app.state.vdb._db_name_map["calling_cards"] = (
        "BrentLab/callingcards",
        "annotated_features",
    )

    resp = await client.post(
        "/api/v1/active-set/intersection",
        json={"datasets": ["calling_cards"], "filters": {}},
    )
    assert resp.status_code == 400
    detail = resp.json()["detail"].lower()
    assert "configured" in detail
    assert "no sql views were registered" in detail


@pytest.mark.asyncio
async def test_intersection_uses_supplemental_regulators_without_join_when_unfiltered(
    client: AsyncClient, test_app
) -> None:
    vdb = test_app.state.vdb
    vdb._db_name_map["calling_cards"] = (
        "BrentLab/callingcards",
        "annotated_features",
    )

    table_list = list(vdb.tables.return_value)
    table_list.extend(["calling_cards", "calling_cards_meta", "calling_cards_regmeta"])
    vdb.tables.return_value = table_list

    original_get_fields = vdb.get_fields.side_effect
    original_query = vdb.query.side_effect

    def get_fields_side_effect(table: str | None = None):
        if table == "calling_cards_meta":
            return ["id", "target_locus_tag"]
        if table == "calling_cards_regmeta":
            return ["id", "regulator_locus_tag", "regulator_symbol"]
        if table == "calling_cards":
            return ["id", "target_locus_tag"]
        return original_get_fields(table)

    def query_side_effect(sql: str, **params: object):
        sql_lower = sql.lower()
        if (
            " as regulator" in sql_lower
            and "from calling_cards_regmeta as src" in sql_lower
        ):
            return pd.DataFrame({"regulator": ["TF1", "TF2", "TF3"]})
        return original_query(sql, **params)

    vdb.get_fields.side_effect = get_fields_side_effect
    vdb.query.side_effect = query_side_effect

    resp = await client.post(
        "/api/v1/active-set/intersection",
        json={"datasets": ["calling_cards"], "filters": {}},
    )
    assert resp.status_code == 200
    cells = resp.json()
    assert cells == [{"row": "calling_cards", "col": "calling_cards", "count": 3}]

    issued_sql = " ".join(call.args[0] for call in vdb.query.call_args_list)
    assert "from calling_cards_regmeta as src join (" not in issued_sql.lower()


@pytest.mark.asyncio
async def test_invalid_table_name_rejected(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/tables/drop%20table/count")
    assert resp.status_code == 400
