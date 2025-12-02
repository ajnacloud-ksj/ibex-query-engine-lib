"""
Unit tests for configuration management
"""
import pytest
from ibexdb.config import Config


class TestConfig:
    """Test Config class"""

    def test_config_from_dict(self, mock_config):
        """Test creating config from dictionary"""
        config = Config.from_dict(mock_config)
        assert config.environment == "test"
        assert config.s3["bucket_name"] == "test-bucket"
        assert config.catalog["type"] == "rest"

    def test_config_s3_properties(self, mock_config):
        """Test S3 configuration properties"""
        config = Config.from_dict(mock_config)
        assert config.s3["endpoint"] == "http://localhost:9000"
        assert config.s3["use_ssl"] is False
        assert config.s3["path_style_access"] is True

    def test_config_duckdb_properties(self, mock_config):
        """Test DuckDB configuration properties"""
        config = Config.from_dict(mock_config)
        assert config.duckdb["memory_limit"] == "1GB"
        assert config.duckdb["threads"] == 2

