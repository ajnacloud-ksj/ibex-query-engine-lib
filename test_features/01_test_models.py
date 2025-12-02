#!/usr/bin/env python3
"""Feature Test 1: Pydantic Models & Validation"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("\n" + "="*70)
print("FEATURE TEST 1: Pydantic Models & Validation")
print("="*70)

# Test 1.1: Filter Model
print("\n[1.1] Testing Filter model...")
from ibexdb.models import Filter
from pydantic import ValidationError

try:
    # Valid filters
    f1 = Filter(field="age", operator="gte", value=18)
    f2 = Filter(field="status", operator="eq", value="active")
    print(f"  ✓ Valid filters: {f1.operator}, {f2.operator}")
    
    # Invalid operator
    try:
        Filter(field="x", operator="invalid", value=1)
        print("  ✗ FAILED: Should reject invalid operator")
        sys.exit(1)
    except ValidationError:
        print("  ✓ Correctly rejects invalid operator")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    sys.exit(1)

# Test 1.2: QueryRequest
print("\n[1.2] Testing QueryRequest model...")
from ibexdb.models import QueryRequest, SortField, SortOrder

try:
    q = QueryRequest(
        tenant_id="demo",
        table="users",
        projection=["id", "name", "email"],
        filters=[
            Filter(field="age", operator="gte", value=18),
            Filter(field="status", operator="eq", value="active")
        ],
        sort=[SortField(field="created_at", order=SortOrder.DESC)],
        limit=100
    )
    assert q.table == "users"
    assert len(q.filters) == 2
    assert q.limit == 100
    print(f"  ✓ QueryRequest: table={q.table}, filters={len(q.filters)}, limit={q.limit}")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 1.3: WriteRequest
print("\n[1.3] Testing WriteRequest model...")
from ibexdb.models import WriteRequest, WriteMode

try:
    w = WriteRequest(
        tenant_id="demo",
        table="users",
        records=[
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25}
        ],
        mode=WriteMode.APPEND
    )
    assert len(w.records) == 2
    print(f"  ✓ WriteRequest: {len(w.records)} records, mode={w.mode.value}")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    sys.exit(1)

# Test 1.4: Schema Definition
print("\n[1.4] Testing SchemaDefinition model...")
from ibexdb.models import SchemaDefinition, FieldDefinition, FieldType, CreateTableRequest

try:
    schema = SchemaDefinition(
        fields={
            "id": FieldDefinition(type=FieldType.INTEGER, required=True),
            "name": FieldDefinition(type=FieldType.STRING, required=True),
            "age": FieldDefinition(type=FieldType.INTEGER),
        },
        primary_key=["id"]
    )
    
    create = CreateTableRequest(
        tenant_id="demo",
        table="test_table",
        schema=schema
    )
    
    assert "id" in create.table_schema.fields
    print(f"  ✓ SchemaDefinition: {len(schema.fields)} fields, PK={schema.primary_key}")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 1.5: Response Models
print("\n[1.5] Testing Response models...")
from ibexdb.models import QueryResponse, QueryResponseData, ResponseMetadata, QueryMetadata

try:
    resp = QueryResponse(
        success=True,
        metadata=ResponseMetadata(request_id="test-123", execution_time_ms=45.2),
        data=QueryResponseData(
            records=[{"id": 1, "name": "Alice"}],
            query_metadata=QueryMetadata(row_count=1, execution_time_ms=45.2)
        )
    )
    assert resp.success is True
    assert len(resp.data.records) == 1
    print(f"  ✓ QueryResponse: success={resp.success}, records={len(resp.data.records)}")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

print("\n" + "="*70)
print("✓ ALL MODEL TESTS PASSED")
print("="*70 + "\n")

