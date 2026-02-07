from __future__ import annotations

import os
import threading
from typing import AsyncIterator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient

# Set required env vars before anything imports app modules.
# With env_prefix="TFBP_", field "config_path" reads TFBP_CONFIG_PATH.
os.environ.setdefault("TFBP_CONFIG_PATH", "/dev/null")


@pytest.fixture
def mock_vdb() -> MagicMock:
    """Create a mock VirtualDB that returns realistic test data."""
    vdb = MagicMock()

    # _db_name_map: db_name -> (repo_id, config_name)
    vdb._db_name_map = {
        "harbison": ("BrentLab/harbison_2004", "harbison_2004"),
        "kemmeren": ("BrentLab/kemmeren_2014", "kemmeren_2014"),
    }

    # _is_comparative
    vdb._is_comparative.return_value = False

    # tables()
    vdb.tables.return_value = [
        "harbison",
        "harbison_meta",
        "kemmeren",
        "kemmeren_meta",
    ]

    # get_common_fields()
    vdb.get_common_fields.return_value = [
        "sample_id",
        "regulator_symbol",
        "carbon_source",
        "effect",
    ]

    # describe()
    def mock_describe(table: str | None = None) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "column_name": [
                    "sample_id",
                    "regulator_symbol",
                    "carbon_source",
                    "effect",
                ],
                "column_type": ["INTEGER", "VARCHAR", "VARCHAR", "DOUBLE"],
            }
        )

    vdb.describe.side_effect = mock_describe

    # get_fields()
    def mock_get_fields(table: str | None = None) -> list[str]:
        if table and "meta" in table:
            return ["sample_id", "regulator_symbol", "carbon_source", "effect"]
        return ["sample_id", "regulator_symbol", "target_locus_tag", "effect", "pvalue"]

    vdb.get_fields.side_effect = mock_get_fields

    # query() — return different data depending on SQL
    def mock_query(sql: str, **params: object) -> pd.DataFrame:
        import re

        sql_lower = sql.lower()
        if "count(*)" in sql_lower:
            return pd.DataFrame({"total": [42]}) if "total" in sql_lower else pd.DataFrame({"cnt": [42]})

        if "min(" in sql_lower and "max(" in sql_lower:
            return pd.DataFrame({"min_value": [0.1], "max_value": [9.9]})

        # Handle "SELECT DISTINCT field AS alias" — return alias as column name
        alias_match = re.search(r"distinct\s+(\w+)\s+as\s+(\w+)", sql_lower)
        if alias_match:
            alias = alias_match.group(2)
            return pd.DataFrame({alias: ["TF1", "TF2", "TF3"]})

        # Handle "SELECT DISTINCT field" — return field as column name
        distinct_match = re.search(r"distinct\s+(\w+)", sql_lower)
        if distinct_match:
            col = distinct_match.group(1)
            return pd.DataFrame({col: ["glucose", "galactose", "raffinose"]})

        # Default: return sample rows
        return pd.DataFrame(
            {
                "sample_id": [1, 2, 3],
                "regulator_symbol": ["TF1", "TF2", "TF3"],
                "effect": [0.5, -0.3, 1.2],
            }
        )

    vdb.query.side_effect = mock_query

    return vdb


@pytest.fixture
def test_app(mock_vdb: MagicMock):
    """Create FastAPI test app with mocked VirtualDB."""
    from app.config import get_settings

    # Clear cached settings so each test gets a fresh instance.
    get_settings.cache_clear()

    from app.main import create_app

    app = create_app()
    app.state.vdb = mock_vdb
    app.state.vdb_lock = threading.Lock()
    return app


@pytest.fixture
async def client(test_app) -> AsyncIterator[AsyncClient]:
    """Async HTTP test client."""
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
