#!/usr/bin/env python3
"""Feature Test 6: Write and Query Operations"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("\n" + "="*70)
print("FEATURE TEST 6: Write and Query Operations")
print("="*70)

os.environ['ENVIRONMENT'] = 'development'

from ibexdb.operations import FullIcebergOperations
from ibexdb.models import WriteRequest, WriteMode, QueryRequest, Filter

# Initialize
print("\n[Setup] Initializing operations...")
try:
    ops = FullIcebergOperations()
    print("  ✓ Operations ready")
except Exception as e:
    print(f"  ✗ Setup failed: {e}")
    sys.exit(1)

# Test 6.1: Write Data
print("\n[6.1] Testing WRITE operation...")
try:
    records = [
        {"id": 1, "name": "Alice Johnson", "age": 30, "email": "alice@example.com"},
        {"id": 2, "name": "Bob Smith", "age": 25, "email": "bob@example.com"},
        {"id": 3, "name": "Charlie Brown", "age": 35, "email": "charlie@example.com"},
        {"id": 4, "name": "Diana Prince", "age": 28, "email": "diana@example.com"},
    ]
    
    write_req = WriteRequest(
        tenant_id="demo",
        namespace="default",
        table="test_users",
        records=records,
        mode=WriteMode.APPEND
    )
    
    response = ops.write(write_req)
    
    if response.success:
        print(f"  ✓ Wrote {response.data.records_written} records")
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 6.2: Query All Data
print("\n[6.2] Testing QUERY (all records)...")
try:
    query_req = QueryRequest(
        tenant_id="demo",
        namespace="default",
        table="test_users",
        projection=["*"]
    )
    
    response = ops.query(query_req)
    
    if response.success:
        records = response.data.records
        print(f"  ✓ Retrieved {len(records)} records")
        if records:
            print(f"  ✓ Sample record: {records[0]}")
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 6.3: Query with Filter
print("\n[6.3] Testing QUERY with filters...")
try:
    query_req = QueryRequest(
        tenant_id="demo",
        namespace="default",
        table="test_users",
        projection=["id", "name", "age"],
        filters=[
            Filter(field="age", operator="gte", value=30)
        ]
    )
    
    response = ops.query(query_req)
    
    if response.success:
        records = response.data.records
        print(f"  ✓ Retrieved {len(records)} records (age >= 30)")
        for rec in records:
            print(f"     - {rec['name']}: {rec['age']} years old")
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 6.4: Query with Limit
print("\n[6.4] Testing QUERY with limit...")
try:
    query_req = QueryRequest(
        tenant_id="demo",
        namespace="default",
        table="test_users",
        projection=["name"],
        limit=2
    )
    
    response = ops.query(query_req)
    
    if response.success:
        records = response.data.records
        print(f"  ✓ Retrieved {len(records)} records (limit=2)")
        assert len(records) <= 2, "Returned more records than limit"
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

print("\n" + "="*70)
print("✓ ALL WRITE/QUERY TESTS PASSED")
print("="*70 + "\n")

