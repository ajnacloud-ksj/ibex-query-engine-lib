"""
Unit tests for TypeSafeQueryBuilder
"""
import pytest
from ibexdb.query_builder import TypeSafeQueryBuilder
from ibexdb.models import Filter, SortField, SortOrder


class TestQueryBuilder:
    """Test TypeSafeQueryBuilder"""

    def test_simple_select(self):
        """Test simple SELECT query"""
        query = (
            TypeSafeQueryBuilder()
            .select(["id", "name", "email"])
            .from_table("users")
            .build()
        )
        assert "SELECT id, name, email" in query
        assert "FROM users" in query

    def test_select_with_where(self):
        """Test SELECT with WHERE clause"""
        query = (
            TypeSafeQueryBuilder()
            .select(["*"])
            .from_table("users")
            .where([Filter(field="age", operator="gte", value=18)])
            .build()
        )
        assert "SELECT *" in query
        assert "WHERE" in query
        assert "age >=" in query

    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY"""
        query = (
            TypeSafeQueryBuilder()
            .select(["*"])
            .from_table("users")
            .order_by([SortField(field="created_at", order=SortOrder.DESC)])
            .build()
        )
        assert "ORDER BY created_at DESC" in query

    def test_select_with_limit(self):
        """Test SELECT with LIMIT"""
        query = (
            TypeSafeQueryBuilder()
            .select(["*"])
            .from_table("users")
            .limit(10)
            .build()
        )
        assert "LIMIT 10" in query

    def test_select_with_limit_and_offset(self):
        """Test SELECT with LIMIT and OFFSET"""
        query = (
            TypeSafeQueryBuilder()
            .select(["*"])
            .from_table("users")
            .limit(10)
            .offset(20)
            .build()
        )
        assert "LIMIT 10" in query
        assert "OFFSET 20" in query

    def test_multiple_where_conditions(self):
        """Test multiple WHERE conditions (ANDed)"""
        query = (
            TypeSafeQueryBuilder()
            .select(["*"])
            .from_table("users")
            .where([
                Filter(field="age", operator="gte", value=18),
                Filter(field="status", operator="eq", value="active"),
            ])
            .build()
        )
        assert "age >=" in query
        assert "status =" in query
        assert "AND" in query

