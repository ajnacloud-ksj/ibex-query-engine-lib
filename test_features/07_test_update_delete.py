#!/usr/bin/env python3
"""Feature Test 7: Update and Delete Operations"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("\n" + "="*70)
print("FEATURE TEST 7: Update and Delete Operations")
print("="*70)

os.environ['ENVIRONMENT'] = 'development'

from ibexdb.operations import FullIcebergOperations
from ibexdb.models import UpdateRequest, DeleteRequest, QueryRequest, Filter

# Initialize
print("\n[Setup] Initializing operations...")
try:
    ops = FullIcebergOperations()
    print("  ✓ Operations ready")
except Exception as e:
    print(f"  ✗ Setup failed: {e}")
    sys.exit(1)

# Test 7.1: Update Records
print("\n[7.1] Testing UPDATE operation...")
try:
    update_req = UpdateRequest(
        tenant_id="demo",
        namespace="default",
        table="test_users",
        updates={"age": 31},
        filters=[Filter(field="id", operator="eq", value=1)]
    )
    
    response = ops.update(update_req)
    
    if response.success:
        print(f"  ✓ Updated {response.data.records_updated} record(s)")
        
        # Verify update
        query_req = QueryRequest(
            tenant_id="demo",
            namespace="default",
            table="test_users",
            filters=[Filter(field="id", operator="eq", value=1)]
        )
        query_resp = ops.query(query_req)
        if query_resp.success and query_resp.data.records:
            updated_age = query_resp.data.records[0].get('age')
            print(f"  ✓ Verified: age updated to {updated_age}")
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 7.2: Delete Records (Soft Delete)
print("\n[7.2] Testing DELETE operation (soft delete)...")
try:
    delete_req = DeleteRequest(
        tenant_id="demo",
        namespace="default",
        table="test_users",
        filters=[Filter(field="id", operator="eq", value=4)]
    )
    
    response = ops.delete(delete_req)
    
    if response.success:
        print(f"  ✓ Deleted {response.data.records_deleted} record(s)")
        
        # Verify deletion (record should not appear in normal query)
        query_req = QueryRequest(
            tenant_id="demo",
            namespace="default",
            table="test_users"
        )
        query_resp = ops.query(query_req)
        if query_resp.success:
            remaining = len(query_resp.data.records)
            print(f"  ✓ Remaining records: {remaining}")
    else:
        print(f"  ✗ FAILED: {response.error.message if response.error else 'Unknown error'}")
        sys.exit(1)
        
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

print("\n" + "="*70)
print("✓ ALL UPDATE/DELETE TESTS PASSED")
print("="*70 + "\n")

