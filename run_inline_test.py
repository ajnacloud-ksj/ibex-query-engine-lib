#!/usr/bin/env python3
"""Inline test runner - runs all tests in one Python process"""
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_models():
    """Test 1: Pydantic Models"""
    print("\n" + "="*70)
    print("TEST 1: Pydantic Models & Validation")
    print("="*70)
    
    from ibexdb.models import Filter, QueryRequest, WriteRequest, SortField, SortOrder, WriteMode
    from ibexdb.models import SchemaDefinition, FieldDefinition, FieldType, CreateTableRequest
    from ibexdb.models import QueryResponse, QueryResponseData, ResponseMetadata, QueryMetadata
    from pydantic import ValidationError
    
    # Test Filter
    print("\n[1.1] Filter validation...")
    f1 = Filter(field="age", operator="gte", value=18)
    print(f"  ✓ Valid filter: {f1.operator}")
    
    try:
        Filter(field="x", operator="invalid", value=1)
        print("  ✗ FAILED: Should reject invalid operator")
        return False
    except ValidationError:
        print("  ✓ Invalid operator rejected")
    
    # Test QueryRequest
    print("\n[1.2] QueryRequest...")
    q = QueryRequest(
        tenant_id="demo",
        table="users",
        filters=[Filter(field="age", operator="gte", value=18)],
        limit=100
    )
    print(f"  ✓ QueryRequest: table={q.table}, filters={len(q.filters)}")
    
    # Test WriteRequest
    print("\n[1.3] WriteRequest...")
    w = WriteRequest(
        tenant_id="demo",
        table="users",
        records=[{"id": 1, "name": "Alice"}],
        mode=WriteMode.APPEND
    )
    print(f"  ✓ WriteRequest: {len(w.records)} records")
    
    # Test Schema
    print("\n[1.4] SchemaDefinition...")
    schema = SchemaDefinition(
        fields={
            "id": FieldDefinition(type=FieldType.INTEGER, required=True),
            "name": FieldDefinition(type=FieldType.STRING, required=True),
        },
        primary_key=["id"]
    )
    print(f"  ✓ Schema: {len(schema.fields)} fields")
    
    # Test Response
    print("\n[1.5] Response models...")
    resp = QueryResponse(
        success=True,
        metadata=ResponseMetadata(request_id="test", execution_time_ms=10.0),
        data=QueryResponseData(
            records=[{"id": 1}],
            query_metadata=QueryMetadata(row_count=1, execution_time_ms=10.0)
        )
    )
    print(f"  ✓ QueryResponse: success={resp.success}")
    
    return True

def test_config():
    """Test 2: Configuration"""
    print("\n" + "="*70)
    print("TEST 2: Configuration Management")
    print("="*70)
    
    os.environ['ENVIRONMENT'] = 'development'
    
    from ibexdb.config import Config
    from ibexdb.logger import setup_logging, get_logger
    
    print("\n[2.1] Config loading...")
    config = Config(environment='development')
    print(f"  ✓ Config: environment={config.environment}")
    print(f"  ✓ S3: bucket={config.s3['bucket_name']}")
    print(f"  ✓ Catalog: type={config.catalog['type']}")
    
    print("\n[2.2] Logging...")
    setup_logging(log_level='INFO')
    logger = get_logger(__name__)
    logger.info("Test message")
    print(f"  ✓ Logging configured")
    
    return True

def test_connectivity():
    """Test 3: Infrastructure connectivity"""
    print("\n" + "="*70)
    print("TEST 3: Infrastructure Connectivity")
    print("="*70)
    
    os.environ['ENVIRONMENT'] = 'development'
    from ibexdb.config import Config
    config = Config(environment='development')
    
    # MinIO
    print("\n[3.1] MinIO S3...")
    try:
        import boto3
        s3 = boto3.client(
            's3',
            endpoint_url=config.s3['endpoint'],
            aws_access_key_id=config.s3['access_key_id'],
            aws_secret_access_key=config.s3['secret_access_key'],
            region_name=config.s3['region']
        )
        buckets = s3.list_buckets()
        print(f"  ✓ MinIO: {len(buckets.get('Buckets', []))} buckets")
    except Exception as e:
        print(f"  ✗ MinIO failed: {e}")
        return False
    
    # REST Catalog
    print("\n[3.2] REST Catalog...")
    try:
        import httpx
        resp = httpx.get(f"{config.catalog['uri']}/v1/config", timeout=5.0)
        print(f"  ✓ REST Catalog: status={resp.status_code}")
    except Exception as e:
        print(f"  ✗ REST Catalog failed: {e}")
        return False
    
    # DuckDB
    print("\n[3.3] DuckDB...")
    try:
        import duckdb
        conn = duckdb.connect(':memory:')
        result = conn.execute("SELECT 42").fetchone()[0]
        print(f"  ✓ DuckDB: test query={result}")
        conn.close()
    except Exception as e:
        print(f"  ✗ DuckDB failed: {e}")
        return False
    
    return True

def test_operations_init():
    """Test 4: Operations initialization"""
    print("\n" + "="*70)
    print("TEST 4: Operations Initialization")
    print("="*70)
    
    os.environ['ENVIRONMENT'] = 'development'
    
    print("\n[4.1] Initializing operations...")
    try:
        from ibexdb.operations import FullIcebergOperations
        ops = FullIcebergOperations()
        print(f"  ✓ Operations initialized")
        print(f"  ✓ Catalog: {ops.config.catalog['type']}")
        print(f"  ✓ DuckDB: {'active' if ops.conn else 'inactive'}")
        return True
    except Exception as e:
        print(f"  ✗ Operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║   IbexDB Library - Inline Test Runner                             ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    
    results = []
    
    # Test 1: Models (no Docker)
    try:
        results.append(("Models", test_models()))
    except Exception as e:
        print(f"\n✗ Models test crashed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Models", False))
    
    # Test 2: Config (no Docker)
    try:
        results.append(("Config", test_config()))
    except Exception as e:
        print(f"\n✗ Config test crashed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Config", False))
    
    # Test 3: Connectivity (needs Docker)
    try:
        results.append(("Connectivity", test_connectivity()))
    except Exception as e:
        print(f"\n✗ Connectivity test crashed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Connectivity", False))
    
    # Test 4: Operations (needs Docker)
    try:
        results.append(("Operations", test_operations_init()))
    except Exception as e:
        print(f"\n✗ Operations test crashed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Operations", False))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status:8} - {name}")
    
    passed = sum(1 for _, p in results if p)
    total = len(results)
    
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())

