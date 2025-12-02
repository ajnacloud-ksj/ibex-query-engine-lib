#!/usr/bin/env python3
"""Feature Test 3: Infrastructure Connectivity"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("\n" + "="*70)
print("FEATURE TEST 3: Infrastructure Connectivity")
print("="*70)

os.environ['ENVIRONMENT'] = 'development'

from ibexdb.config import Config
config = Config(environment='development')

# Test 3.1: MinIO S3 Connectivity
print("\n[3.1] Testing MinIO S3 connectivity...")
try:
    import boto3
    from botocore.exceptions import ClientError
    
    s3_client = boto3.client(
        's3',
        endpoint_url=config.s3['endpoint'],
        aws_access_key_id=config.s3['access_key_id'],
        aws_secret_access_key=config.s3['secret_access_key'],
        region_name=config.s3['region']
    )
    
    # Try to list buckets
    response = s3_client.list_buckets()
    print(f"  ✓ MinIO connected: {len(response.get('Buckets', []))} buckets found")
    
    # Check if our bucket exists
    bucket_name = config.s3['bucket_name']
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"  ✓ Bucket '{bucket_name}' exists")
    except ClientError:
        print(f"  ! Bucket '{bucket_name}' doesn't exist (will be created on first write)")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    print(f"  → Is MinIO running? docker-compose ps")
    sys.exit(1)

# Test 3.2: Iceberg REST Catalog Connectivity
print("\n[3.2] Testing Iceberg REST Catalog connectivity...")
try:
    import httpx
    
    catalog_uri = config.catalog['uri']
    response = httpx.get(f"{catalog_uri}/v1/config", timeout=5.0)
    
    if response.status_code == 200:
        print(f"  ✓ REST Catalog connected: {catalog_uri}")
        config_data = response.json()
        print(f"  ✓ Catalog config received: {list(config_data.keys())[:3]}...")
    else:
        print(f"  ! REST Catalog responded with status {response.status_code}")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    print(f"  → Is Iceberg REST catalog running? docker-compose ps")
    sys.exit(1)

# Test 3.3: DuckDB Basic Functionality
print("\n[3.3] Testing DuckDB...")
try:
    import duckdb
    
    conn = duckdb.connect(':memory:')
    result = conn.execute("SELECT 42 as answer").fetchone()
    assert result[0] == 42
    print(f"  ✓ DuckDB working: test query returned {result[0]}")
    
    # Check DuckDB version
    version = conn.execute("SELECT version()").fetchone()[0]
    print(f"  ✓ DuckDB version: {version.split()[0]}")
    
    conn.close()
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    sys.exit(1)

print("\n" + "="*70)
print("✓ ALL CONNECTIVITY TESTS PASSED")
print("="*70 + "\n")

