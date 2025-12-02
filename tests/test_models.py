"""
Unit tests for Pydantic models
"""
import pytest
from pydantic import ValidationError
from ibexdb.models import (
    QueryRequest,
    WriteRequest,
    UpdateRequest,
    DeleteRequest,
    CreateTableRequest,
    Filter,
    SortField,
    SortOrder,
    WriteMode,
    SchemaDefinition,
    FieldDefinition,
    FieldType,
)


class TestFilter:
    """Test Filter model validation"""

    def test_valid_filter(self):
        """Test creating valid filter"""
        f = Filter(field="age", operator="gte", value=18)
        assert f.field == "age"
        assert f.operator == "gte"
        assert f.value == 18

    def test_invalid_operator(self):
        """Test filter with invalid operator"""
        with pytest.raises(ValidationError):
            Filter(field="age", operator="invalid", value=18)

    def test_all_operators(self):
        """Test all valid operators"""
        valid_ops = ["eq", "ne", "gt", "gte", "lt", "lte", "in", "like"]
        for op in valid_ops:
            f = Filter(field="test", operator=op, value="test")
            assert f.operator == op


class TestSortField:
    """Test SortField model"""

    def test_default_sort_order(self):
        """Test default sort order is ASC"""
        sort = SortField(field="created_at")
        assert sort.order == SortOrder.ASC

    def test_desc_sort_order(self):
        """Test DESC sort order"""
        sort = SortField(field="created_at", order=SortOrder.DESC)
        assert sort.order == SortOrder.DESC


class TestQueryRequest:
    """Test QueryRequest model validation"""

    def test_minimal_query_request(self):
        """Test minimal valid query request"""
        req = QueryRequest(
            tenant_id="test",
            table="users"
        )
        assert req.tenant_id == "test"
        assert req.table == "users"
        assert req.namespace == "default"

    def test_query_with_filters(self):
        """Test query with filters"""
        req = QueryRequest(
            tenant_id="test",
            table="users",
            filters=[
                Filter(field="age", operator="gte", value=18),
                Filter(field="status", operator="eq", value="active"),
            ],
        )
        assert len(req.filters) == 2
        assert req.filters[0].field == "age"

    def test_query_with_projection(self):
        """Test query with specific projections"""
        req = QueryRequest(
            tenant_id="test",
            table="users",
            projection=["id", "name", "email"],
        )
        assert len(req.projection) == 3
        assert "name" in req.projection

    def test_query_with_pagination(self):
        """Test query with pagination"""
        req = QueryRequest(
            tenant_id="test",
            table="users",
            limit=10,
            offset=20,
        )
        assert req.limit == 10
        assert req.offset == 20

    def test_having_without_group_by_fails(self):
        """Test that having clause requires group_by"""
        with pytest.raises(ValidationError):
            QueryRequest(
                tenant_id="test",
                table="users",
                having=[Filter(field="count", operator="gt", value=5)],
            )


class TestWriteRequest:
    """Test WriteRequest model validation"""

    def test_minimal_write_request(self, sample_records):
        """Test minimal valid write request"""
        req = WriteRequest(
            tenant_id="test",
            table="users",
            records=sample_records,
        )
        assert req.tenant_id == "test"
        assert req.table == "users"
        assert len(req.records) == 3
        assert req.mode == WriteMode.APPEND

    def test_write_mode_overwrite(self, sample_records):
        """Test write with overwrite mode"""
        req = WriteRequest(
            tenant_id="test",
            table="users",
            records=sample_records,
            mode=WriteMode.OVERWRITE,
        )
        assert req.mode == WriteMode.OVERWRITE


class TestUpdateRequest:
    """Test UpdateRequest model validation"""

    def test_update_request(self):
        """Test update request"""
        req = UpdateRequest(
            tenant_id="test",
            table="users",
            updates={"status": "inactive"},
            filters=[Filter(field="age", operator="gt", value=65)],
        )
        assert req.updates == {"status": "inactive"}
        assert len(req.filters) == 1


class TestDeleteRequest:
    """Test DeleteRequest model validation"""

    def test_delete_request(self):
        """Test delete request"""
        req = DeleteRequest(
            tenant_id="test",
            table="users",
            filters=[Filter(field="id", operator="eq", value=1)],
        )
        assert len(req.filters) == 1


class TestCreateTableRequest:
    """Test CreateTableRequest model validation"""

    def test_create_table_request(self):
        """Test create table request"""
        schema = SchemaDefinition(
            fields={
                "id": FieldDefinition(type=FieldType.INTEGER, required=True),
                "name": FieldDefinition(type=FieldType.STRING, required=True),
                "age": FieldDefinition(type=FieldType.INTEGER),
            },
            primary_key=["id"],
        )
        
        req = CreateTableRequest(
            tenant_id="test",
            table="users",
            schema=schema,
        )
        assert req.table == "users"
        assert "id" in req.table_schema.fields
        assert req.if_not_exists is True

