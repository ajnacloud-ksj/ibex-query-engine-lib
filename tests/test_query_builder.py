"""
Unit tests for TypeSafeQueryBuilder
"""
import pytest

from ibexdb.models import Filter, QueryRequest, SortField, SortOrder
from ibexdb.query_builder import TypeSafeQueryBuilder


class TestQueryBuilder:
    """Test TypeSafeQueryBuilder"""

    def test_simple_select(self):
        """Test simple SELECT query"""
        builder = TypeSafeQueryBuilder()
        request = QueryRequest(
            tenant_id="test",
            table="users",
            projection=["id", "name", "email"],
        )
        sql, params = builder.build_query(request)
        assert "SELECT id, name, email" in sql
        assert "FROM users" in sql
        assert params == []

    def test_select_with_where(self):
        """Test SELECT with WHERE clause"""
        builder = TypeSafeQueryBuilder()
        request = QueryRequest(
            tenant_id="test",
            table="users",
            filters=[Filter(field="age", operator="gte", value=18)],
        )
        sql, params = builder.build_query(request)
        assert "SELECT *" in sql
        assert "WHERE" in sql
        assert "age >=" in sql
        assert 18 in params

    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY"""
        builder = TypeSafeQueryBuilder()
        request = QueryRequest(
            tenant_id="test",
            table="users",
            sort=[SortField(field="created_at", order=SortOrder.DESC)],
        )
        sql, params = builder.build_query(request)
        assert "ORDER BY created_at DESC" in sql

    def test_select_with_limit(self):
        """Test SELECT with LIMIT"""
        builder = TypeSafeQueryBuilder()
        request = QueryRequest(
            tenant_id="test",
            table="users",
            limit=10,
        )
        sql, params = builder.build_query(request)
        assert "LIMIT 10" in sql

    def test_select_with_limit_and_offset(self):
        """Test SELECT with LIMIT and OFFSET"""
        builder = TypeSafeQueryBuilder()
        request = QueryRequest(
            tenant_id="test",
            table="users",
            limit=10,
            offset=20,
        )
        sql, params = builder.build_query(request)
        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    def test_multiple_where_conditions(self):
        """Test multiple WHERE conditions (ANDed)"""
        builder = TypeSafeQueryBuilder()
        request = QueryRequest(
            tenant_id="test",
            table="users",
            filters=[
                Filter(field="age", operator="gte", value=18),
                Filter(field="status", operator="eq", value="active"),
            ],
        )
        sql, params = builder.build_query(request)
        assert "age >=" in sql
        assert "status =" in sql
        assert "AND" in sql
        assert 18 in params
        assert "active" in params

    def test_select_with_distinct(self):
        """Test SELECT DISTINCT"""
        builder = TypeSafeQueryBuilder()
        request = QueryRequest(
            tenant_id="test",
            table="users",
            projection=["status"],
            distinct=True,
        )
        sql, params = builder.build_query(request)
        assert "SELECT DISTINCT status" in sql

    def test_dialect_parameter_placeholder(self):
        """Test different SQL dialects use correct placeholders"""
        duckdb_builder = TypeSafeQueryBuilder(dialect="duckdb")
        postgres_builder = TypeSafeQueryBuilder(dialect="postgres")
        mysql_builder = TypeSafeQueryBuilder(dialect="mysql")

        request = QueryRequest(
            tenant_id="test",
            table="users",
            filters=[Filter(field="id", operator="eq", value=1)],
        )

        duckdb_sql, _ = duckdb_builder.build_query(request)
        postgres_sql, _ = postgres_builder.build_query(request)
        mysql_sql, _ = mysql_builder.build_query(request)

        assert "?" in duckdb_sql
        assert "%s" in postgres_sql
        assert "%s" in mysql_sql
