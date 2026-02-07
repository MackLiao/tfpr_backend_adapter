from __future__ import annotations

import pytest
from httpx import AsyncClient


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
async def test_invalid_table_name_rejected(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/tables/drop%20table/count")
    assert resp.status_code == 400
