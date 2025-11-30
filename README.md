# IbexDB Python Library

> **Production-ready ACID database on S3 with Apache Iceberg**

IbexDB is a serverless, ACID-compliant database built on AWS S3 and Apache Iceberg. It provides full SQL-like operations with strong consistency, versioning, and time-travel capabilities.

## Features

✨ **ACID Transactions** - Full consistency guarantees on S3 object storage  
🚀 **Serverless** - Deploy as AWS Lambda or use locally  
📊 **Analytics-Ready** - Powered by DuckDB + Apache Iceberg  
🔒 **Type-Safe** - Pydantic models with comprehensive validation  
⏰ **Time Travel** - Query data at any point in time  
🔄 **Schema Evolution** - Add/modify columns without breaking changes  
📦 **Complex Types** - Arrays, Structs, Maps, and nested structures  
🎯 **High Performance** - Automatic compaction and query optimization  

## Installation

```bash
# Basic installation
pip install ibexdb

# With FastAPI support (for local development)
pip install ibexdb[fastapi]

# With AWS Lambda support
pip install ibexdb[lambda]

# All optional dependencies
pip install ibexdb[all]
```

## Quick Start

### 1. Basic Usage

```python
from ibexdb import IbexDB, IbexConfig

# Initialize client
config = IbexConfig(
    environment="development",
    s3_bucket="my-bucket",
    s3_region="us-east-1",
    catalog_uri="http://localhost:8181"
)

db = IbexDB(config)

# Create a table
db.create_table(
    table="users",
    schema={
        "fields": {
            "id": {"type": "integer", "required": True},
            "name": {"type": "string", "required": True},
            "email": {"type": "string"},
            "created_at": {"type": "timestamp"}
        },
        "primary_key": ["id"]
    }
)

# Write data
db.write(
    table="users",
    records=[
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"}
    ]
)

# Query data
results = db.query(
    table="users",
    filters=[
        {"field": "name", "operator": "like", "value": "A%"}
    ],
    limit=10
)

print(results.data.records)
# [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
```

### 2. Advanced Querying

```python
# Complex filters and aggregations
results = db.query(
    table="orders",
    projection=["customer_id", "order_date"],
    filters=[
        {"field": "status", "operator": "eq", "value": "completed"},
        {"field": "total", "operator": "gte", "value": 100}
    ],
    aggregations=[
        {"field": "total", "function": "sum", "alias": "total_revenue"},
        {"field": None, "function": "count", "alias": "order_count"}
    ],
    group_by=["customer_id"],
    sort=[{"field": "total_revenue", "order": "desc"}],
    limit=100
)
```

### 3. Update and Delete

```python
# Update records
db.update(
    table="users",
    updates={"status": "active"},
    filters=[{"field": "last_login", "operator": "gte", "value": "2024-01-01"}]
)

# Soft delete (mark as deleted)
db.delete(
    table="users",
    filters=[{"field": "status", "operator": "eq", "value": "inactive"}],
    mode="soft"
)

# Hard delete (physically remove)
db.hard_delete(
    table="users",
    filters=[{"field": "created_at", "operator": "lt", "value": "2020-01-01"}],
    confirm=True
)
```

### 4. Time Travel

```python
from datetime import datetime, timedelta

# Query data as it was 7 days ago
past_data = db.query(
    table="users",
    as_of=datetime.now() - timedelta(days=7)
)

# Query changes between two timestamps
changes = db.query(
    table="users",
    between_times=(
        datetime.now() - timedelta(days=7),
        datetime.now()
    )
)
```

### 5. Complex Data Types

```python
# Create table with complex types
db.create_table(
    table="products",
    schema={
        "fields": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "tags": {
                "type": "array",
                "items": {"type": "string"}
            },
            "metadata": {
                "type": "map",
                "key_type": "string",
                "value_type": {"type": "string"}
            },
            "address": {
                "type": "struct",
                "fields": {
                    "street": {"type": "string"},
                    "city": {"type": "string"},
                    "zipcode": {"type": "string"}
                }
            }
        }
    }
)

# Write complex data
db.write(
    table="products",
    records=[{
        "id": 1,
        "name": "Widget",
        "tags": ["electronics", "gadget"],
        "metadata": {"color": "blue", "size": "medium"},
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "zipcode": "10001"
        }
    }]
)
```

### 6. Compaction and Maintenance

```python
# Compact small files for better performance
db.compact(
    table="users",
    force=False,  # Only compact if thresholds are met
    target_file_size_mb=128,
    expire_snapshots=True,
    snapshot_retention_hours=168  # 7 days
)
```

## Integration with FastAPI

```python
from fastapi import FastAPI
from ibexdb import IbexDB
from ibexdb.fastapi import create_database_router

app = FastAPI()

# Initialize IbexDB
db = IbexDB.from_env()

# Add database router
app.include_router(
    create_database_router(db),
    prefix="/database",
    tags=["database"]
)
```

## Integration with AWS Lambda

```python
# lambda_handler.py
from ibexdb.lambda_handler import create_lambda_handler
from ibexdb import IbexConfig

# Create Lambda handler
config = IbexConfig.from_env()
lambda_handler = create_lambda_handler(config)

# That's it! Deploy to AWS Lambda
```

## Configuration

### Environment Variables

```bash
# Required
ENVIRONMENT=production
BUCKET_NAME=my-iceberg-bucket
AWS_REGION=us-east-1

# Optional (for production with Glue)
AWS_ACCOUNT_ID=123456789012

# Optional (for development with REST catalog)
CATALOG_URI=http://localhost:8181
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
```

### Configuration File

Create `config/config.json`:

```json
{
  "production": {
    "s3": {
      "bucket_name": "${BUCKET_NAME}",
      "warehouse_path": "iceberg-warehouse",
      "region": "${AWS_REGION}"
    },
    "catalog": {
      "type": "glue",
      "region": "${AWS_REGION}"
    },
    "duckdb": {
      "memory_limit": "3.5GB",
      "threads": 16
    }
  }
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        IbexDB Client                         │
│                     (Python Library)                         │
└─────────────────────────────────────────────────────────────┘
                             │
                ┌────────────┼────────────┐
                │            │            │
                ▼            ▼            ▼
         ┌──────────┐  ┌──────────┐  ┌──────────┐
         │PyIceberg │  │  DuckDB  │  │  Config  │
         │ (Writes) │  │ (Reads)  │  │ Manager  │
         └──────────┘  └──────────┘  └──────────┘
                │            │
                └────────────┼────────────┘
                             │
                    ┌────────▼────────┐
                    │ Apache Iceberg  │
                    │   (Metadata)    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   AWS S3 / MinIO │
                    │   (Data Files)   │
                    └──────────────────┘
```

## Use Cases

### 1. Data Lake Analytics
Query petabytes of data directly from S3 with full ACID guarantees.

### 2. Event Sourcing
Store immutable events with time-travel capabilities.

### 3. Data Warehouse
Build a serverless data warehouse with schema evolution.

### 4. ML Feature Store
Store and version ML features with point-in-time correctness.

### 5. Audit Logs
Maintain tamper-proof audit trails with versioning.

## Performance

- **Writes**: ~1000 records/sec per Lambda invocation
- **Queries**: Sub-second for millions of rows (with proper partitioning)
- **Storage**: ~50% compression with Zstd
- **Cost**: ~$0.023 per GB/month on S3

## API Reference

See [full API documentation](https://docs.ibexdb.io) for detailed information.

### Core Classes

- `IbexDB` - Main client interface
- `IbexConfig` - Configuration management
- `DatabaseOperations` - Low-level operations
- `QueryBuilder` - Type-safe query builder

### Request Models

- `QueryRequest` - Query configuration
- `WriteRequest` - Write/insert operations
- `UpdateRequest` - Update operations
- `DeleteRequest` - Delete operations
- `CreateTableRequest` - Table creation
- `CompactRequest` - Compaction operations

### Response Models

All operations return standardized responses:
- `success`: boolean
- `data`: operation results
- `metadata`: execution info (request_id, execution_time_ms)
- `error`: error details (if failed)

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT License - see [LICENSE](LICENSE) for details.

## Support

- 📧 Email: support@ajnacloud.com
- 💬 Discord: [Join our community](https://discord.gg/ibexdb)
- 🐛 Issues: [GitHub Issues](https://github.com/ajnacloud/ibexdb/issues)
- 📖 Docs: [docs.ibexdb.io](https://docs.ibexdb.io)

