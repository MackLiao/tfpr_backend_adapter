from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["tables_registered"] == 4


@pytest.mark.asyncio
async def test_list_tables(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/tables")
    assert resp.status_code == 200
    tables = resp.json()
    assert "harbison" in tables
    assert "harbison_meta" in tables
    assert "kemmeren" in tables


@pytest.mark.asyncio
async def test_list_datasets(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/datasets")
    assert resp.status_code == 200
    datasets = resp.json()
    assert len(datasets) == 2
    names = {d["db_name"] for d in datasets}
    assert names == {"harbison", "kemmeren"}
    for d in datasets:
        assert "repo_id" in d
        assert "config_name" in d
        assert "is_comparative" in d


@pytest.mark.asyncio
async def test_common_fields(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/common-fields")
    assert resp.status_code == 200
    fields = resp.json()
    assert "sample_id" in fields
    assert "regulator_symbol" in fields
