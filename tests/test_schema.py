from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_describe_table(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/schema/harbison")
    assert resp.status_code == 200
    columns = resp.json()
    assert len(columns) >= 3
    names = {c["column_name"] for c in columns}
    assert "sample_id" in names
    assert "regulator_symbol" in names
    for c in columns:
        assert "column_type" in c


@pytest.mark.asyncio
async def test_get_fields(client: AsyncClient) -> None:
    resp = await client.get("/api/v1/fields/harbison_meta")
    assert resp.status_code == 200
    fields = resp.json()
    assert "sample_id" in fields
    assert "regulator_symbol" in fields
