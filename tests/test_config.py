"""
Unit tests for configuration management
"""
import json
import os
import tempfile

import pytest

from ibexdb.config import Config


class TestConfig:
    """Test Config class"""

    def test_config_loads_from_file(self, tmp_path):
        """Test creating config from config.json file"""
        config_data = {
            "test": {
                "s3": {
                    "bucket_name": "test-bucket",
                    "warehouse_path": "test-warehouse",
                    "endpoint": "http://localhost:9000",
                    "region": "us-east-1",
                    "use_ssl": False,
                    "path_style_access": True,
                },
                "catalog": {"type": "rest", "name": "rest", "uri": "http://localhost:8181"},
                "duckdb": {"memory_limit": "1GB", "threads": 2},
                "performance": {"max_retries": 3, "query_timeout_ms": 30000},
            }
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config_data))

        os.environ["IBEX_CONFIG_PATH"] = str(config_file)
        try:
            config = Config(environment="test")
            assert config.environment == "test"
            assert config.s3["bucket_name"] == "test-bucket"
            assert config.catalog["type"] == "rest"
        finally:
            os.environ.pop("IBEX_CONFIG_PATH", None)

    def test_config_s3_properties(self, tmp_path):
        """Test S3 configuration properties"""
        config_data = {
            "test": {
                "s3": {
                    "bucket_name": "test-bucket",
                    "warehouse_path": "test-warehouse",
                    "endpoint": "http://localhost:9000",
                    "region": "us-east-1",
                    "use_ssl": False,
                    "path_style_access": True,
                },
                "catalog": {"type": "rest"},
                "duckdb": {"memory_limit": "1GB", "threads": 2},
            }
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config_data))

        os.environ["IBEX_CONFIG_PATH"] = str(config_file)
        try:
            config = Config(environment="test")
            assert config.s3["endpoint"] == "http://localhost:9000"
            assert config.s3["use_ssl"] is False
            assert config.s3["path_style_access"] is True
        finally:
            os.environ.pop("IBEX_CONFIG_PATH", None)

    def test_config_duckdb_properties(self, tmp_path):
        """Test DuckDB configuration properties"""
        config_data = {
            "test": {
                "s3": {"bucket_name": "test-bucket"},
                "catalog": {"type": "rest"},
                "duckdb": {"memory_limit": "1GB", "threads": 2},
            }
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config_data))

        os.environ["IBEX_CONFIG_PATH"] = str(config_file)
        try:
            config = Config(environment="test")
            assert config.duckdb["memory_limit"] == "1GB"
            assert config.duckdb["threads"] == 2
        finally:
            os.environ.pop("IBEX_CONFIG_PATH", None)

    def test_config_get_nested_key(self, tmp_path):
        """Test get() with nested keys"""
        config_data = {
            "test": {
                "s3": {"bucket_name": "my-bucket", "region": "us-west-2"},
                "catalog": {"type": "rest"},
                "duckdb": {},
            }
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config_data))

        os.environ["IBEX_CONFIG_PATH"] = str(config_file)
        try:
            config = Config(environment="test")
            assert config.get("s3", "bucket_name") == "my-bucket"
            assert config.get("s3", "region") == "us-west-2"
        finally:
            os.environ.pop("IBEX_CONFIG_PATH", None)

    def test_config_missing_environment_raises(self, tmp_path):
        """Test that missing environment raises ValueError"""
        config_data = {"production": {"s3": {}}}
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config_data))

        os.environ["IBEX_CONFIG_PATH"] = str(config_file)
        try:
            with pytest.raises(ValueError, match="not found in config.json"):
                Config(environment="test")
        finally:
            os.environ.pop("IBEX_CONFIG_PATH", None)

    def test_config_env_var_substitution(self, tmp_path):
        """Test ${VAR_NAME} substitution"""
        config_data = {
            "test": {
                "s3": {"bucket_name": "${TEST_BUCKET_NAME}"},
                "catalog": {"type": "rest"},
            }
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config_data))

        os.environ["IBEX_CONFIG_PATH"] = str(config_file)
        os.environ["TEST_BUCKET_NAME"] = "substituted-bucket"
        try:
            config = Config(environment="test")
            assert config.s3["bucket_name"] == "substituted-bucket"
        finally:
            os.environ.pop("IBEX_CONFIG_PATH", None)
            os.environ.pop("TEST_BUCKET_NAME", None)
