"""
PyTest Configuration and Fixtures
"""
import os
import pytest
from typing import Dict, Any


@pytest.fixture
def mock_config() -> Dict[str, Any]:
    """Provide mock configuration for testing"""
    return {
        "environment": "test",
        "s3": {
            "bucket_name": "test-bucket",
            "warehouse_path": "test-warehouse",
            "endpoint": "http://localhost:9000",
            "region": "us-east-1",
            "access_key_id": "test",
            "secret_access_key": "test",
            "use_ssl": False,
            "path_style_access": True,
        },
        "catalog": {
            "type": "rest",
            "name": "rest",
            "uri": "http://localhost:8181",
        },
        "duckdb": {
            "memory_limit": "1GB",
            "threads": 2,
        },
    }


@pytest.fixture
def sample_records():
    """Sample records for testing"""
    return [
        {"id": 1, "name": "Alice", "age": 30, "status": "active"},
        {"id": 2, "name": "Bob", "age": 25, "status": "active"},
        {"id": 3, "name": "Charlie", "age": 35, "status": "inactive"},
    ]


@pytest.fixture(autouse=True)
def set_test_env():
    """Set test environment variables"""
    os.environ["IBEX_ENV"] = "test"
    os.environ["IBEX_TENANT_ID"] = "test-tenant"
    yield
    # Cleanup
    os.environ.pop("IBEX_ENV", None)
    os.environ.pop("IBEX_TENANT_ID", None)

