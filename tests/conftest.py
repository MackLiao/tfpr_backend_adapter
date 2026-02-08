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

        # Top items queries (for correlation matrix) — CHECK THIS FIRST before COUNT(*)
        # Pattern: SELECT column_name, COUNT(*) ... GROUP BY column_name
        if "group by" in sql_lower:
            # Extract the group-by column from the SQL
            # Pattern: "SELECT column_name, COUNT(*)" or "select column_name, count(*)"
            select_match = re.search(r"select\s+(\w+)\s*,\s*count", sql_lower)
            if select_match:
                col_name_lower = select_match.group(1)
                # Find the original case column name
                if "regulator_symbol" in sql_lower or "regulator" in sql_lower:
                    return pd.DataFrame({
                        "regulator_symbol": ["TF1", "TF2", "TF3"],
                        "cnt": [100, 95, 90]
                    })
                elif "sample_id" in sql_lower:
                    return pd.DataFrame({
                        "sample_id": [1, 2, 3],
                        "cnt": [100, 95, 90]
                    })
            # Default fallback for GROUP BY
            return pd.DataFrame({
                "regulator_symbol": ["TF1", "TF2", "TF3"],
                "cnt": [100, 95, 90]
            })

        # CORR queries (for correlation matrix)
        if "corr(" in sql_lower:
            return pd.DataFrame({"correlation": [0.85]})

        # MIN/MAX queries
        if "min(" in sql_lower and "max(" in sql_lower:
            return pd.DataFrame({"min_value": [0.1], "max_value": [9.9]})

        # COUNT DISTINCT queries
        if "count(distinct" in sql_lower:
            return pd.DataFrame({"cnt": [42]})

        # COUNT queries (must come after GROUP BY check)
        if "count(*)" in sql_lower:
            return pd.DataFrame({"total": [42]}) if "total" in sql_lower else pd.DataFrame({"cnt": [42]})

        # Handle "SELECT DISTINCT field AS alias" — return alias as column name
        alias_match = re.search(r"distinct\s+(\w+)\s+as\s+(\w+)", sql_lower)
        if alias_match:
            alias = alias_match.group(2)
            return pd.DataFrame({alias: ["TF1", "TF2", "TF3"]})

        # Handle "SELECT DISTINCT field" — return field as column name
        distinct_match = re.search(r"distinct\s+(\w+)", sql_lower)
        if distinct_match:
            col = distinct_match.group(1)
            # Return appropriate values based on column name
            if "regulator" in col:
                return pd.DataFrame({col: ["ACE2", "GAL4", "SWI4"]})
            else:
                return pd.DataFrame({col: ["glucose", "galactose", "raffinose"]})

        # Default: return sample rows
        return pd.DataFrame(
            {
                "sample_id": [1, 2, 3],
                "regulator_symbol": ["TF1", "TF2", "TF3"],
                "effect": [0.5, -0.3, 1.2],
                "target_locus_tag": ["YAL001C", "YAL002W", "YAL003W"],
                "pvalue": [0.001, 0.005, 0.01],
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
