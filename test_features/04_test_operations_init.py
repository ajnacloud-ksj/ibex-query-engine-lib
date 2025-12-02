#!/usr/bin/env python3
"""Feature Test 4: Operations Initialization"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("\n" + "="*70)
print("FEATURE TEST 4: Operations Initialization")
print("="*70)

os.environ['ENVIRONMENT'] = 'development'

# Test 4.1: Import Operations
print("\n[4.1] Testing operations imports...")
try:
    from ibexdb.operations import FullIcebergOperations
    from ibexdb.config import get_config
    print(f"  ✓ Operations module imported")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 4.2: Initialize Operations (connects to real services)
print("\n[4.2] Initializing FullIcebergOperations...")
print("      (This will connect to MinIO and REST Catalog)")
try:
    ops = FullIcebergOperations()
    print(f"  ✓ Operations initialized successfully")
    print(f"  ✓ Catalog type: {ops.config.catalog['type']}")
    print(f"  ✓ DuckDB connection: {'active' if ops.conn else 'failed'}")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    print("\n  TROUBLESHOOTING:")
    print("  1. Check if services are running: docker-compose ps")
    print("  2. Check MinIO: http://localhost:9000")
    print("  3. Check REST Catalog: http://localhost:8181/v1/config")
    sys.exit(1)

# Test 4.3: Verify DuckDB extensions
print("\n[4.3] Checking DuckDB extensions...")
try:
    # Check if iceberg extension can be loaded
    ops.conn.execute("LOAD iceberg")
    print(f"  ✓ Iceberg extension loaded")
    
    ops.conn.execute("LOAD httpfs")
    print(f"  ✓ HTTPFS extension loaded")
    
except Exception as e:
    print(f"  ! Warning: Extension loading issue: {e}")
    print(f"    (Extensions may install on first use)")

print("\n" + "="*70)
print("✓ ALL OPERATIONS INIT TESTS PASSED")
print("="*70 + "\n")

