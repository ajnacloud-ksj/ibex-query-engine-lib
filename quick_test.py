#!/usr/bin/env python3
"""Quick validation test for IbexDB library"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

print("=" * 60)
print("IbexDB Library - Quick Validation Test")
print("=" * 60)

# Test 1: Import models
print("\n[1/5] Testing model imports...")
try:
    from ibexdb.models import (
        QueryRequest, WriteRequest, Filter, SortField, 
        SchemaDefinition, FieldDefinition, FieldType
    )
    print("✓ All models imported successfully")
except Exception as e:
    print(f"✗ Model import failed: {e}")
    sys.exit(1)

# Test 2: Create Filter
print("\n[2/5] Testing Filter validation...")
try:
    f1 = Filter(field="age", operator="gte", value=18)
    assert f1.field == "age"
    assert f1.operator == "gte"
    print("✓ Filter created and validated")
except Exception as e:
    print(f"✗ Filter test failed: {e}")
    sys.exit(1)

# Test 3: Invalid Filter operator
print("\n[3/5] Testing Filter validation (should fail)...")
try:
    from pydantic import ValidationError
    try:
        bad_filter = Filter(field="age", operator="invalid", value=18)
        print("✗ Invalid operator should have raised ValidationError")
        sys.exit(1)
    except ValidationError:
        print("✓ Validation correctly rejected invalid operator")
except Exception as e:
    print(f"✗ Validation test failed: {e}")
    sys.exit(1)

# Test 4: Create QueryRequest
print("\n[4/5] Testing QueryRequest...")
try:
    query = QueryRequest(
        tenant_id="test",
        table="users",
        filters=[
            Filter(field="age", operator="gte", value=18),
            Filter(field="status", operator="eq", value="active")
        ],
        projection=["id", "name", "email"],
        limit=10
    )
    assert query.table == "users"
    assert query.limit == 10
    assert len(query.filters) == 2
    print("✓ QueryRequest created successfully")
except Exception as e:
    print(f"✗ QueryRequest test failed: {e}")
    sys.exit(1)

# Test 5: Create Table Schema
print("\n[5/5] Testing SchemaDefinition...")
try:
    schema = SchemaDefinition(
        fields={
            "id": FieldDefinition(type=FieldType.INTEGER, required=True),
            "name": FieldDefinition(type=FieldType.STRING, required=True),
            "age": FieldDefinition(type=FieldType.INTEGER),
        },
        primary_key=["id"]
    )
    assert "id" in schema.fields
    assert schema.primary_key == ["id"]
    print("✓ SchemaDefinition created successfully")
except Exception as e:
    print(f"✗ SchemaDefinition test failed: {e}")
    sys.exit(1)

print("\n" + "=" * 60)
print("✓ ALL TESTS PASSED!")
print("=" * 60)
print("\nLibrary is ready for integration with ibex-db-lambda")

