# Configuration Guide for IbexDB

## Overview

The `ibexdb` library now supports **three ways to configure data sources**:

1. ✅ **Config Manager Endpoint** (like ajna-db-backend)
2. ✅ **Configuration File** (JSON/YAML)
3. ✅ **Manual Configuration** (programmatic)

Plus **type-safe QueryRequest/QueryResponse** models for all queries!

---

## 🎯 Quick Comparison

| Method | Best For | Auto-Refresh | Dynamic |
|--------|----------|--------------|---------|
| **Config Manager** | Production, multi-service | ✅ Yes | ✅ Yes |
| **Configuration File** | Dev, single-service | ✅ Yes | ❌ No |
| **Manual** | Testing, scripts | ❌ No | ❌ No |

---

## 1️⃣ Config Manager Endpoint (Recommended!)

### Like ajna-db-backend AnalyticsManager

This fetches data sources dynamically from a Config Manager endpoint, just like `ajna-db-backend` does.

```python
from ibexdb import create_federated_engine

# Create engine from Config Manager
fed = create_federated_engine(
    config_endpoint="http://ibex-data-platform:8080",
    api_key="my-api-key"
)

# Sources are automatically loaded!
print(f"Loaded {len(fed.list_sources())} sources")

# Query across all sources
result = fed.execute_sql("""
    SELECT * FROM source1.table1
    JOIN source2.table2 ON ...
""")
```

### With Auto-Refresh

```python
from ibexdb import DataSourceConfigManager, FederatedQueryEngine

# Create config manager with auto-refresh
config_mgr = DataSourceConfigManager.from_endpoint(
    url="http://ibex-data-platform:8080",
    api_key="my-api-key",
    auto_refresh=True,
    refresh_interval=60  # Refresh every 60 seconds
)

# Start background refresh
config_mgr.start_auto_refresh()

# Create federated engine
fed = FederatedQueryEngine(config_manager=config_mgr)

# Sources automatically refresh every 60 seconds!
```

### Config Manager API Format

Your Config Manager should return:

```json
{
  "success": true,
  "data_sources": [
    {
      "id": "ibexdb-production",
      "name": "IbexDB Production",
      "type": "ibexdb",
      "enabled": true,
      "connection_config": {
        "tenant_id": "my_company",
        "namespace": "production"
      },
      "metadata": {
        "description": "Production data lake",
        "owner": "data-team"
      }
    },
    {
      "id": "postgres-analytics",
      "name": "PostgreSQL Analytics",
      "type": "postgres",
      "enabled": true,
      "connection_config": {
        "host": "postgres-server",
        "port": 5432,
        "database": "analytics",
        "user": "app_user",
        "password": "secret"
      }
    }
  ]
}
```

---

## 2️⃣ Configuration File

### JSON Format

Create `datasources.json`:

```json
{
  "data_sources": [
    {
      "id": "ibexdb-production",
      "name": "IbexDB Production",
      "type": "ibexdb",
      "enabled": true,
      "config": {
        "tenant_id": "my_company",
        "namespace": "production"
      },
      "metadata": {
        "description": "Production data lake on S3"
      }
    },
    {
      "id": "postgres-analytics",
      "name": "PostgreSQL Analytics",
      "type": "postgres",
      "enabled": true,
      "config": {
        "host": "localhost",
        "port": 5432,
        "database": "analytics",
        "user": "app_user",
        "password": "${POSTGRES_PASSWORD}"
      }
    },
    {
      "id": "mysql-orders",
      "name": "MySQL Orders",
      "type": "mysql",
      "enabled": true,
      "config": {
        "host": "localhost",
        "port": 3306,
        "database": "ecommerce",
        "user": "app_user",
        "password": "${MYSQL_PASSWORD}"
      }
    }
  ]
}
```

### YAML Format

Create `datasources.yaml`:

```yaml
data_sources:
  - id: ibexdb-production
    name: IbexDB Production
    type: ibexdb
    enabled: true
    config:
      tenant_id: my_company
      namespace: production
    metadata:
      description: Production data lake on S3
      
  - id: postgres-analytics
    name: PostgreSQL Analytics
    type: postgres
    enabled: true
    config:
      host: localhost
      port: 5432
      database: analytics
      user: app_user
      password: ${POSTGRES_PASSWORD}
```

### Load from File

```python
from ibexdb import create_federated_engine

# From JSON
fed = create_federated_engine(
    config_file="datasources.json"
)

# From YAML
fed = create_federated_engine(
    config_file="datasources.yaml"
)
```

---

## 3️⃣ Manual Configuration

### Programmatic Setup

```python
from ibexdb import create_federated_engine

# Manual configuration
fed = create_federated_engine({
    "ibexdb_prod": {
        "type": "ibexdb",
        "config": {
            "tenant_id": "my_company",
            "namespace": "production"
        }
    },
    "postgres_analytics": {
        "type": "postgres",
        "config": {
            "host": "localhost",
            "port": 5432,
            "database": "analytics",
            "user": "app_user",
            "password": "secret"
        }
    }
})
```

### Add Sources Dynamically

```python
from ibexdb import FederatedQueryEngine

fed = FederatedQueryEngine()

# Add sources one by one
fed.add_source("source1", "ibexdb", {
    "tenant_id": "company",
    "namespace": "prod"
})

fed.add_source("source2", "postgres", {
    "host": "localhost",
    "database": "analytics",
    "user": "user",
    "password": "pass"
})
```

---

## 🔒 QueryRequest/QueryResponse API

### Type-Safe Queries (Like ibex-db-lambda!)

```python
from ibexdb import FederatedQueryEngine
from ibexdb.models import QueryRequest, Filter, AggregateField

# Load sources from Config Manager
fed = create_federated_engine(
    config_endpoint="http://config-manager:8080"
)

# Create type-safe request
request = QueryRequest(
    operation="QUERY",
    tenant_id="my_company",
    namespace="production",
    table="users",
    projection=["id", "name", "email"],
    filters=[
        Filter(field="age", operator="gte", value=18),
        Filter(field="status", operator="eq", "value": "active")
    ],
    aggregations=[
        AggregateField(
            field=None,
            function="count",
            alias="total_users"
        )
    ],
    group_by=["status"],
    limit=100
)

# Execute with type-safe response
response = fed.execute_query_request(request)

if response.success:
    print(f"Records: {len(response.data.records)}")
    print(f"Execution time: {response.metadata.execution_time_ms}ms")
    
    for record in response.data.records:
        print(record)
else:
    print(f"Error: {response.error.message}")
```

### Response Structure

```python
QueryResponse(
    success=True,
    data=QueryResponseData(
        records=[{...}, {...}],
        query_metadata=QueryMetadata(
            row_count=100,
            execution_time_ms=45.2,
            scanned_rows=100,
            cache_hit=False
        )
    ),
    metadata=ResponseMetadata(
        request_id="fed_1234567890",
        execution_time_ms=45.2
    ),
    error=None  # Only present if success=False
)
```

---

## 🎯 Supported Data Source Types

| Type | Configuration | Example |
|------|---------------|---------|
| **ibexdb** | `tenant_id`, `namespace` | Iceberg tables on S3 |
| **postgres** | `host`, `port`, `database`, `user`, `password` | PostgreSQL |
| **mysql** | `host`, `port`, `database`, `user`, `password` | MySQL |
| **s3** | `endpoint`, `access_key`, `secret_key` | S3 Parquet files |

### IbexDB Source

```json
{
  "type": "ibexdb",
  "config": {
    "tenant_id": "my_company",
    "namespace": "production",
    "environment": "production"
  }
}
```

### PostgreSQL Source

```json
{
  "type": "postgres",
  "config": {
    "host": "postgres-server",
    "port": 5432,
    "database": "analytics",
    "user": "app_user",
    "password": "secret"
  }
}
```

### MySQL Source

```json
{
  "type": "mysql",
  "config": {
    "host": "mysql-server",
    "port": 3306,
    "database": "ecommerce",
    "user": "app_user",
    "password": "secret"
  }
}
```

### S3 Source

```json
{
  "type": "s3",
  "config": {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "use_ssl": false
  }
}
```

---

## 🚀 Production Patterns

### Pattern 1: Like ajna-db-backend

```python
import os
from ibexdb import create_federated_engine

# Get config from environment
config_manager_url = os.getenv(
    'CONFIG_MANAGER_URL',
    'http://ibex-data-platform:8080'
)
api_key = os.getenv('CONFIG_MANAGER_API_KEY')

# Create engine from Config Manager
fed = create_federated_engine(
    config_endpoint=config_manager_url,
    api_key=api_key
)

# Query across all configured sources!
result = fed.execute_sql("""
    SELECT * FROM postgres.users u
    JOIN ibexdb.events e ON u.id = e.user_id
""")
```

### Pattern 2: Hybrid Configuration

```python
from ibexdb import create_federated_engine

# Start with Config Manager sources
fed = create_federated_engine(
    config_endpoint="http://config-manager:8080",
    api_key="api-key"
)

# Add additional manual sources
fed.add_source("temp_db", "postgres", {
    "host": "temp-server",
    "database": "temp",
    "user": "user",
    "password": "pass"
})
```

### Pattern 3: Environment-Based Config

```bash
# .env file
ENVIRONMENT=production
CONFIG_MANAGER_URL=http://config-manager:8080
CONFIG_MANAGER_API_KEY=my-secret-key
```

```python
import os
from ibexdb import create_federated_engine

# Load from environment
fed = create_federated_engine(
    config_endpoint=os.getenv('CONFIG_MANAGER_URL'),
    api_key=os.getenv('CONFIG_MANAGER_API_KEY')
)
```

---

## 📊 Complete Example

```python
"""
Complete production example matching ajna-db-backend pattern
"""
import os
from ibexdb import create_federated_engine
from ibexdb.models import QueryRequest, Filter

# 1. Configure from environment
config_url = os.getenv('CONFIG_MANAGER_URL', 'http://config-manager:8080')
api_key = os.getenv('CONFIG_MANAGER_API_KEY')

# 2. Create federated engine
fed = create_federated_engine(
    config_endpoint=config_url,
    api_key=api_key
)

print(f"Loaded {len(fed.list_sources())} data sources")

# 3. Option A: Use QueryRequest (type-safe)
request = QueryRequest(
    operation="QUERY",
    tenant_id="company",
    namespace="production",
    table="users",
    filters=[
        Filter(field="status", operator="eq", value="active")
    ],
    limit=100
)

response = fed.execute_query_request(request)

if response.success:
    print(f"Type-safe query: {len(response.data.records)} records")

# 4. Option B: Use SQL (flexible)
result = fed.execute_sql("""
    SELECT 
        u.name,
        COUNT(e.event_id) as events,
        SUM(o.total) as revenue
    FROM postgres.users u
    LEFT JOIN ibexdb.events e ON u.id = e.user_id
    LEFT JOIN mysql.orders o ON u.id = o.customer_id
    WHERE u.status = 'active'
    GROUP BY u.name
    ORDER BY revenue DESC
    LIMIT 100
""")

print(f"SQL query: {len(result)} records")

# 5. Close connection
fed.close()
```

---

## 🔄 Migration from ajna-db-backend

If you're using `ajna-db-backend`'s `AnalyticsManager`, migrating is easy:

### Before (AnalyticsManager)

```python
from app.services.analytics_manager import AnalyticsManager

manager = AnalyticsManager(
    config_manager_url="http://config-manager:8080",
    api_key="my-key",
    cors_origins=["*"]
)

manager.initialize()

result = manager.execute_query("SELECT * FROM users")
```

### After (IbexDB)

```python
from ibexdb import create_federated_engine

fed = create_federated_engine(
    config_endpoint="http://config-manager:8080",
    api_key="my-key"
)

result = fed.execute_sql("SELECT * FROM users")
```

**Same Config Manager, cleaner API!**

---

## 📚 Summary

### Three Configuration Methods

1. ✅ **Config Manager Endpoint** - Dynamic, production-ready
2. ✅ **Configuration File** - Simple, version-controlled
3. ✅ **Manual** - Flexible, programmatic

### Two Query APIs

1. ✅ **QueryRequest/QueryResponse** - Type-safe, validated
2. ✅ **SQL** - Flexible, powerful

### Key Benefits

- ✅ **Like ajna-db-backend** - Same Config Manager pattern
- ✅ **Type-safe** - Pydantic validation
- ✅ **Auto-refresh** - Dynamic source discovery
- ✅ **Flexible** - Multiple configuration methods
- ✅ **Production-ready** - Battle-tested patterns

---

**Start using it now!**

```bash
pip install -e /path/to/ibexdb
python examples/config_management.py
```

