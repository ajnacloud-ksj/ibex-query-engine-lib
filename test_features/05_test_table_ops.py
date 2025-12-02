#!/usr/bin/env python3
"""Feature Test 5: Table Operations (CREATE, LIST, DESCRIBE)"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("\n" + "="*70)
print("FEATURE TEST 5: Table Operations")
print("="*70)

os.environ['ENVIRONMENT'] = 'development'

from ibexdb.operations import FullIcebergOperations
from ibexdb.models import (
    CreateTableRequest, SchemaDefinition, FieldDefinition, FieldType,
    ListTablesRequest, DescribeTableRequest
)

# Initialize
print("\n[Setup] Initializing operations...")
try:
    ops = FullIcebergOperations()
    print("  ✓ Operations ready")
except Exception as e:
    print(f"  ✗ Setup failed: {e}")
    sys.exit(1)

# Test 5.1: Create Table
print("\n[5.1] Testing CREATE TABLE...")
try:
    schema = SchemaDefinition(
        fields={
            "id": FieldDefinition(type=FieldType.INTEGER, required=True),
            "name": FieldDefinition(type=FieldType.STRING, required=True),
            "age": FieldDefinition(type=FieldType.INTEGER),
            "email": FieldDefinition(type=FieldType.STRING),
            "created_at": FieldDefinition(type=FieldType.TIMESTAMP),
        },
        primary_key=["id"]
    )
    
    create_req = CreateTableRequest(
        tenant_id="demo",
        namespace="default",
        table="test_users",
        schema=schema,
        if_not_exists=True
    )
    
    response = ops.create_table(create_req)
    
    if response.success:
        if response.data.table_created:
            print(f"  ✓ Table created: test_users")
        else:
            print(f"  ✓ Table already exists: test_users")
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 5.2: List Tables
print("\n[5.2] Testing LIST TABLES...")
try:
    list_req = ListTablesRequest(
        tenant_id="demo",
        namespace="default"
    )
    
    response = ops.list_tables(list_req)
    
    if response.success:
        tables = response.data.tables
        print(f"  ✓ Found {len(tables)} table(s): {tables}")
        assert "test_users" in tables, "test_users not found in table list"
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 5.3: Describe Table
print("\n[5.3] Testing DESCRIBE TABLE...")
try:
    describe_req = DescribeTableRequest(
        tenant_id="demo",
        namespace="default",
        table="test_users"
    )
    
    response = ops.describe_table(describe_req)
    
    if response.success:
        table_info = response.data.table
        print(f"  ✓ Table: {table_info.table_name}")
        print(f"  ✓ Namespace: {table_info.namespace}")
        if table_info.table_schema:
            print(f"  ✓ Schema fields: {list(table_info.table_schema.keys())[:3]}...")
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

print("\n" + "="*70)
print("✓ ALL TABLE OPERATION TESTS PASSED")
print("="*70 + "\n")

