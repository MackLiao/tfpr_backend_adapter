"""Tests for analysis endpoints."""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_source_summary(client: AsyncClient) -> None:
    """Test GET /analysis/source-summary/{db_name}."""
    resp = await client.get("/api/v1/analysis/source-summary/harbison")
    assert resp.status_code == 200
    data = resp.json()
    assert data["db_name"] == "harbison"
    assert data["repo_id"] == "BrentLab/harbison_2004"
    assert data["config_name"] == "harbison_2004"
    assert "dataset_type" in data
    assert data["total_rows"] == 42
    assert data["regulator_count"] == 42
    assert data["target_count"] >= 0
    assert data["sample_count"] == 42
    assert data["column_count"] > 0
    assert isinstance(data["metadata_fields"], list)


@pytest.mark.asyncio
async def test_source_summary_invalid_table(client: AsyncClient) -> None:
    """Test source summary with non-existent dataset."""
    resp = await client.get("/api/v1/analysis/source-summary/nonexistent")
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_source_summary_no_meta_table(client: AsyncClient) -> None:
    """Test source summary when _meta table is missing."""
    # This would require updating the mock to simulate missing meta table
    # For now, we rely on the mock always providing both table and _meta
    pass


@pytest.mark.asyncio
async def test_binding_analysis(client: AsyncClient) -> None:
    """Test POST /analysis/binding."""
    resp = await client.post(
        "/api/v1/analysis/binding",
        json={
            "datasets": ["harbison"],
            "page": 1,
            "page_size": 10,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    result = data[0]
    assert result["db_name"] == "harbison"
    assert isinstance(result["data"], list)
    assert result["total"] == 42
    assert result["page"] == 1
    assert result["page_size"] == 10
    assert isinstance(result["has_next"], bool)
    assert isinstance(result["columns"], list)


@pytest.mark.asyncio
async def test_binding_analysis_multiple_datasets(client: AsyncClient) -> None:
    """Test binding analysis with multiple datasets."""
    resp = await client.post(
        "/api/v1/analysis/binding",
        json={
            "datasets": ["harbison", "kemmeren"],
            "page": 1,
            "page_size": 100,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    db_names = {result["db_name"] for result in data}
    assert db_names == {"harbison", "kemmeren"}


@pytest.mark.asyncio
async def test_binding_analysis_with_filters(client: AsyncClient) -> None:
    """Test binding analysis with categorical and numeric filters."""
    resp = await client.post(
        "/api/v1/analysis/binding",
        json={
            "datasets": ["harbison"],
            "filters": {
                "harbison": {
                    "carbon_source": ["glucose", "galactose"],
                }
            },
            "numeric_filters": {
                "harbison": {
                    "effect": {"min_value": 0.1, "max_value": 5.0},
                }
            },
            "page": 1,
            "page_size": 50,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1


@pytest.mark.asyncio
async def test_binding_analysis_empty_datasets(client: AsyncClient) -> None:
    """Test binding analysis with empty datasets list."""
    resp = await client.post(
        "/api/v1/analysis/binding",
        json={
            "datasets": [],
            "page": 1,
            "page_size": 10,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data == []


@pytest.mark.asyncio
async def test_binding_analysis_invalid_dataset(client: AsyncClient) -> None:
    """Test binding analysis with non-existent dataset."""
    resp = await client.post(
        "/api/v1/analysis/binding",
        json={
            "datasets": ["nonexistent"],
            "page": 1,
            "page_size": 10,
        },
    )
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_perturbation_analysis(client: AsyncClient) -> None:
    """Test POST /analysis/perturbation."""
    resp = await client.post(
        "/api/v1/analysis/perturbation",
        json={
            "datasets": ["kemmeren"],
            "page": 1,
            "page_size": 20,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == 1
    result = data[0]
    assert result["db_name"] == "kemmeren"
    assert isinstance(result["data"], list)
    assert result["total"] == 42
    assert result["page"] == 1
    assert result["page_size"] == 20


@pytest.mark.asyncio
async def test_perturbation_analysis_with_filters(client: AsyncClient) -> None:
    """Test perturbation analysis with filters."""
    resp = await client.post(
        "/api/v1/analysis/perturbation",
        json={
            "datasets": ["kemmeren"],
            "filters": {
                "kemmeren": {
                    "carbon_source": ["glucose"],
                }
            },
            "page": 1,
            "page_size": 100,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1


@pytest.mark.asyncio
async def test_correlation_matrix(client: AsyncClient) -> None:
    """Test POST /analysis/correlation."""
    resp = await client.post(
        "/api/v1/analysis/correlation",
        json={
            "db_name": "harbison",
            "method": "pearson",
            "value_column": "effect",
            "group_by": "regulator",
            "max_items": 10,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["db_name"] == "harbison"
    assert data["method"] == "pearson"
    assert isinstance(data["labels"], list)
    assert isinstance(data["cells"], list)
    # Each cell should have row, col, value
    for cell in data["cells"]:
        assert "row" in cell
        assert "col" in cell
        assert "value" in cell
        assert isinstance(cell["value"], (int, float))


@pytest.mark.asyncio
async def test_correlation_matrix_sample_grouping(client: AsyncClient) -> None:
    """Test correlation matrix with sample-level grouping."""
    resp = await client.post(
        "/api/v1/analysis/correlation",
        json={
            "db_name": "harbison",
            "method": "pearson",
            "value_column": "effect",
            "group_by": "sample",
            "max_items": 5,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["db_name"] == "harbison"
    assert isinstance(data["labels"], list)
    assert isinstance(data["cells"], list)


@pytest.mark.asyncio
async def test_correlation_matrix_invalid_dataset(client: AsyncClient) -> None:
    """Test correlation with non-existent dataset."""
    resp = await client.post(
        "/api/v1/analysis/correlation",
        json={
            "db_name": "nonexistent",
            "method": "pearson",
            "value_column": "effect",
            "group_by": "regulator",
        },
    )
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_correlation_matrix_invalid_column(client: AsyncClient) -> None:
    """Test correlation with non-existent value column."""
    resp = await client.post(
        "/api/v1/analysis/correlation",
        json={
            "db_name": "harbison",
            "method": "pearson",
            "value_column": "nonexistent_column",
            "group_by": "regulator",
        },
    )
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_correlation_matrix_invalid_group_by(client: AsyncClient) -> None:
    """Test correlation with invalid group_by parameter."""
    resp = await client.post(
        "/api/v1/analysis/correlation",
        json={
            "db_name": "harbison",
            "method": "pearson",
            "value_column": "effect",
            "group_by": "invalid",
        },
    )
    assert resp.status_code == 400
    assert "group_by" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_filter_options_single_dataset(client: AsyncClient) -> None:
    """Test POST /analysis/filter-options with single dataset."""
    resp = await client.post(
        "/api/v1/analysis/filter-options",
        json={
            "datasets": ["harbison"],
            "column": "regulator_symbol",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["column"] == "regulator_symbol"
    assert isinstance(data["values"], list)
    assert len(data["values"]) > 0
    # Values should be sorted
    assert data["values"] == sorted(data["values"])


@pytest.mark.asyncio
async def test_filter_options_multiple_datasets(client: AsyncClient) -> None:
    """Test filter options across multiple datasets (deduplicates values)."""
    resp = await client.post(
        "/api/v1/analysis/filter-options",
        json={
            "datasets": ["harbison", "kemmeren"],
            "column": "regulator_symbol",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["column"] == "regulator_symbol"
    assert isinstance(data["values"], list)
    # Should have unique values from both datasets
    assert len(data["values"]) > 0
    assert data["values"] == sorted(data["values"])


@pytest.mark.asyncio
async def test_filter_options_empty_datasets(client: AsyncClient) -> None:
    """Test filter options with empty datasets list."""
    resp = await client.post(
        "/api/v1/analysis/filter-options",
        json={
            "datasets": [],
            "column": "regulator_symbol",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["column"] == "regulator_symbol"
    assert data["values"] == []


@pytest.mark.asyncio
async def test_filter_options_nonexistent_dataset(client: AsyncClient) -> None:
    """Test filter options with nonexistent dataset (should skip gracefully)."""
    resp = await client.post(
        "/api/v1/analysis/filter-options",
        json={
            "datasets": ["nonexistent"],
            "column": "regulator_symbol",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["column"] == "regulator_symbol"
    assert data["values"] == []


@pytest.mark.asyncio
async def test_filter_options_nonexistent_column(client: AsyncClient) -> None:
    """Test filter options with column that doesn't exist in dataset."""
    resp = await client.post(
        "/api/v1/analysis/filter-options",
        json={
            "datasets": ["harbison"],
            "column": "nonexistent_column",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["column"] == "nonexistent_column"
    assert data["values"] == []
