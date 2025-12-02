#!/usr/bin/env python3
"""Feature Test 2: Configuration Management"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("\n" + "="*70)
print("FEATURE TEST 2: Configuration Management")
print("="*70)

# Set environment
os.environ['ENVIRONMENT'] = 'development'

# Test 2.1: Config Loading
print("\n[2.1] Testing config loading...")
from ibexdb.config import Config

try:
    config = Config(environment='development')
    print(f"  ✓ Config loaded: environment={config.environment}")
    
    # Test S3 config
    s3 = config.s3
    print(f"  ✓ S3 config: bucket={s3['bucket_name']}, endpoint={s3['endpoint']}")
    
    # Test Catalog config
    catalog = config.catalog
    print(f"  ✓ Catalog config: type={catalog['type']}, uri={catalog['uri']}")
    
    # Test DuckDB config
    duckdb = config.duckdb
    print(f"  ✓ DuckDB config: memory={duckdb['memory_limit']}, threads={duckdb['threads']}")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 2.2: Logging Config
print("\n[2.2] Testing logging configuration...")
try:
    from ibexdb.logger import setup_logging, get_logger
    
    # Setup with config
    logging_config = config.get('logging', default={'level': 'INFO'})
    setup_logging(log_level=logging_config.get('level', 'INFO'))
    
    logger = get_logger(__name__)
    logger.info("Test log message")
    print(f"  ✓ Logging configured: level={logging_config.get('level')}")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 2.3: Config-driven features
print("\n[2.3] Testing config values...")
try:
    # Check all required config sections exist
    assert config.s3['bucket_name'] == 'test-bucket'
    assert config.s3['endpoint'] == 'http://localhost:9000'
    assert config.catalog['uri'] == 'http://localhost:8181'
    print(f"  ✓ All config sections validated")
    
except Exception as e:
    print(f"  ✗ FAILED: {e}")
    sys.exit(1)

print("\n" + "="*70)
print("✓ ALL CONFIG TESTS PASSED")
print("="*70 + "\n")

