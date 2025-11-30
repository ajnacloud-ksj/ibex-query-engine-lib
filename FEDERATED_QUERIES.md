# Federated Queries with IbexDB

## Overview

The `ibexdb` library now supports **federated queries** across multiple data sources, combining:

- ✅ **Structured API** (No SQL knowledge needed!)
- ✅ **SQL API** (For advanced users)
- ✅ **Multi-source queries** (Iceberg + PostgreSQL + MySQL + more)
- ✅ **Type-safe** with Pydantic validation
- ✅ **Flexible catalog support** (REST, Glue, Nessie, custom)

This gives you the **best of both worlds**:
- Type-safe structured requests (like `ibex-db-lambda`)
- SQL flexibility (like `ajna-db-backend`)
- Multi-source federation

---

## 🎯 Three Ways to Query

### 1. Structured API (No SQL Required!)

Perfect for:
- Users who don't know SQL
- Type-safe applications
- API-driven queries
- Automated systems

```python
from ibexdb import IbexDB

db = IbexDB.from_env()

# NO SQL! Just pass parameters
results = db.query(
    table="users",
    filters=[
        {"field": "age", "operator": "gte", "value": 18},
        {"field": "status", "operator": "eq", "value": "active"}
    ],
    aggregations=[
        {"field": None, "function": "count", "alias": "total"},
        {"field": "age", "function": "avg", "alias": "avg_age"}
    ],
    group_by=["status"],
    limit=100
)
```

**Benefits:**
- ✅ No SQL knowledge needed
- ✅ Type-safe with validation
- ✅ IDE autocomplete
- ✅ Prevents SQL injection

### 2. SQL API (For Power Users)

Perfect for:
- Complex analytical queries
- Ad-hoc analysis
- Users comfortable with SQL
- Migrating from traditional databases

```python
from ibexdb import IbexDB

db = IbexDB.from_env()

# Write SQL directly!
results = db.execute_sql("""
    SELECT 
        status,
        COUNT(*) as total,
        AVG(age) as avg_age,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) as median_age
    FROM users
    WHERE age >= 18
    GROUP BY status
    HAVING COUNT(*) > 100
    ORDER BY total DESC
""")
```

**Benefits:**
- ✅ Full SQL power (window functions, CTEs, etc.)
- ✅ Familiar syntax
- ✅ Complex analytics
- ✅ Ad-hoc queries

### 3. Federated API (Multi-Source Queries)

Perfect for:
- Joining data across sources
- Unified analytics
- Data warehouse replacement
- Cost-effective BI

```python
from ibexdb import FederatedQueryEngine

# Initialize with multiple sources
fed = FederatedQueryEngine()

fed.add_source("iceberg", "ibexdb", {
    "tenant_id": "company",
    "namespace": "production"
})

fed.add_source("postgres", "postgres", {
    "host": "localhost",
    "database": "crm",
    "user": "app",
    "password": "secret"
})

fed.add_source("mysql", "mysql", {
    "host": "localhost",
    "database": "orders",
    "user": "app",
    "password": "secret"
})

# Query across ALL sources!
result = fed.execute_sql("""
    SELECT 
        p.name,
        p.email,
        COUNT(i.event_id) as events,
        SUM(m.total) as revenue
    FROM postgres.customers p
    LEFT JOIN iceberg.events i ON p.id = i.customer_id
    LEFT JOIN mysql.orders m ON p.id = m.customer_id
    WHERE p.status = 'active'
    GROUP BY p.name, p.email
    ORDER BY revenue DESC
    LIMIT 100
""")
```

**Benefits:**
- ✅ Query multiple sources as one
- ✅ Join Iceberg with PostgreSQL/MySQL
- ✅ No ETL needed
- ✅ Real-time unified view

---

## 📊 Architecture

### Single-Source (IbexDB Client)

```
┌─────────────────────────────────────┐
│        Your Application             │
└─────────────────┬───────────────────┘
                  │
    ┌─────────────┴─────────────┐
    │                           │
    ▼                           ▼
┌──────────────┐    ┌───────────────────┐
│ Structured   │    │   SQL API         │
│ API (No SQL) │    │   (Advanced)      │
└──────────────┘    └───────────────────┘
    │                           │
    └────────────┬──────────────┘
                 ▼
    ┌────────────────────────┐
    │     IbexDB Client      │
    │   (Single Source)      │
    └────────────────────────┘
                 │
                 ▼
    ┌────────────────────────┐
    │  PyIceberg + DuckDB    │
    │  (REST/Glue Catalog)   │
    └────────────────────────┘
                 │
                 ▼
    ┌────────────────────────┐
    │    Apache Iceberg      │
    │       on S3            │
    └────────────────────────┘
```

### Multi-Source (Federated Engine)

```
┌──────────────────────────────────────────────┐
│         Your Application                      │
└──────────────────┬───────────────────────────┘
                   │
    ┌──────────────┴───────────────┐
    │                              │
    ▼                              ▼
┌──────────────┐      ┌─────────────────────┐
│ Structured   │      │   SQL API           │
│ Federated    │      │   (Multi-Source)    │
│ Query        │      │                     │
└──────────────┘      └─────────────────────┘
    │                              │
    └──────────────┬───────────────┘
                   ▼
    ┌───────────────────────────────────┐
    │   FederatedQueryEngine            │
    │   (Query Coordinator)             │
    └───────────────────────────────────┘
          │          │           │
    ┌─────┴────┬─────┴─────┬─────┴────┐
    ▼          ▼           ▼          ▼
┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
│IbexDB  │ │Postgres│ │ MySQL  │ │  S3    │
│(Icebrg)│ │        │ │        │ │Parquet │
└────────┘ └────────┘ └────────┘ └────────┘
```

---

## 🚀 Quick Start Examples

### Example 1: Simple Structured Query

```python
from ibexdb import IbexDB

db = IbexDB.from_env()

# Query with filters and aggregations (NO SQL!)
results = db.query(
    table="orders",
    filters=[
        {"field": "status", "operator": "eq", "value": "completed"},
        {"field": "order_date", "operator": "gte", "value": "2024-01-01"}
    ],
    aggregations=[
        {"field": None, "function": "count", "alias": "order_count"},
        {"field": "total", "function": "sum", "alias": "revenue"},
        {"field": "total", "function": "avg", "alias": "avg_order"}
    ],
    group_by=["customer_id"],
    sort=[{"field": "revenue", "order": "desc"}],
    limit=100
)

for record in results.data.records:
    print(f"Customer {record['customer_id']}: ${record['revenue']:.2f}")
```

### Example 2: SQL Query on Single Source

```python
from ibexdb import IbexDB

db = IbexDB.from_env()

# Use SQL for complex analytics
results = db.execute_sql("""
    WITH monthly_revenue AS (
        SELECT 
            DATE_TRUNC('month', order_date) as month,
            customer_id,
            SUM(total) as revenue
        FROM orders
        WHERE status = 'completed'
        GROUP BY 1, 2
    )
    SELECT 
        month,
        COUNT(DISTINCT customer_id) as customers,
        SUM(revenue) as total_revenue,
        AVG(revenue) as avg_revenue_per_customer
    FROM monthly_revenue
    GROUP BY month
    ORDER BY month DESC
""")

for row in results:
    print(f"{row['month']}: {row['customers']} customers, ${row['total_revenue']:.2f}")
```

### Example 3: Federated Structured Query

```python
from ibexdb import FederatedQueryEngine

fed = FederatedQueryEngine()

# Add sources
fed.add_source("warehouse", "ibexdb", {"tenant_id": "company", "namespace": "prod"})
fed.add_source("crm", "postgres", {"host": "...", "database": "crm", ...})

# Structured federated query
result = fed.query(
    sources={
        "c": {"source": "crm", "table": "customers"},
        "o": {"source": "warehouse", "table": "orders"}
    },
    projection=["c.name", "COUNT(o.id) as orders", "SUM(o.total) as revenue"],
    join={
        "type": "left",
        "left": "c",
        "right": "o",
        "on": {"c.id": "o.customer_id"}
    },
    filters=[
        {"source": "c", "field": "status", "operator": "eq", "value": "active"}
    ],
    group_by=["c.name"],
    limit=50
)
```

### Example 4: Federated SQL Query

```python
from ibexdb import create_federated_engine

# Create engine with all sources
fed = create_federated_engine({
    "warehouse": {
        "type": "ibexdb",
        "config": {"tenant_id": "company", "namespace": "analytics"}
    },
    "crm": {
        "type": "postgres",
        "config": {"host": "...", "database": "crm", ...}
    },
    "billing": {
        "type": "mysql",
        "config": {"host": "...", "database": "billing", ...}
    }
})

# SQL across all sources!
result = fed.execute_sql("""
    SELECT 
        c.name,
        c.email,
        c.segment,
        COUNT(DISTINCT e.event_id) as events_30d,
        COUNT(DISTINCT o.order_id) as orders_30d,
        SUM(i.amount) as revenue_30d
    FROM crm.customers c
    LEFT JOIN warehouse.events e 
        ON c.id = e.customer_id 
        AND e.event_date >= CURRENT_DATE - INTERVAL 30 DAYS
    LEFT JOIN warehouse.orders o 
        ON c.id = o.customer_id 
        AND o.order_date >= CURRENT_DATE - INTERVAL 30 DAYS
    LEFT JOIN billing.invoices i 
        ON c.id = i.customer_id 
        AND i.invoice_date >= CURRENT_DATE - INTERVAL 30 DAYS
        AND i.status = 'paid'
    WHERE c.status = 'active'
    GROUP BY c.name, c.email, c.segment
    HAVING revenue_30d > 1000
    ORDER BY revenue_30d DESC
    LIMIT 100
""")
```

---

## 🔧 Catalog Configuration

### REST Catalog (Development/Multi-Cloud)

```json
{
  "development": {
    "catalog": {
      "type": "rest",
      "uri": "http://localhost:8181"
    },
    "s3": {
      "bucket_name": "my-bucket",
      "endpoint": "http://localhost:9000",
      "region": "us-east-1"
    }
  }
}
```

### AWS Glue Catalog (Production)

```json
{
  "production": {
    "catalog": {
      "type": "glue",
      "region": "us-east-1"
    },
    "s3": {
      "bucket_name": "production-bucket",
      "region": "us-east-1"
    }
  }
}
```

---

## 🎯 Use Cases

### 1. Customer 360 View
Combine CRM (Postgres), events (Iceberg), and billing (MySQL) for complete customer view.

### 2. Real-Time Analytics
Query live Iceberg tables alongside operational databases without ETL.

### 3. Cost-Effective Data Warehouse
Store cold data in S3 (Iceberg), hot data in PostgreSQL, query both seamlessly.

### 4. Multi-Tenant Analytics
Separate tenant data sources, federate for cross-tenant analytics.

### 5. Hybrid Cloud
Combine AWS (Iceberg/Glue) with on-prem PostgreSQL for unified analytics.

---

## 📈 Performance Tips

1. **Push filters down**: Apply filters in WHERE clause, not after JOIN
2. **Use partitioning**: Partition Iceberg tables by date for fast queries
3. **Index operational DBs**: Ensure join keys are indexed in Postgres/MySQL
4. **Limit result sets**: Use LIMIT to prevent large data transfers
5. **Cache frequently used queries**: Store results for dashboards

---

## 🔒 Security

- ✅ SQL injection prevention (structured API)
- ✅ Connection credentials encrypted
- ✅ Role-based access control (integrate with Ajna RBAC)
- ✅ Query timeouts
- ✅ Resource limits

---

## 🚧 Limitations

- IbexDB queries are read-only in federated mode
- Writes must go through single-source IbexDB client
- Some complex SQL features may vary by source type
- Performance depends on network latency between sources

---

## 📚 See Also

- [Quick Start Guide](QUICKSTART.md)
- [Integration with Ajna Backend](INTEGRATION_GUIDE.md)
- [API Reference](README.md)
- [Examples](examples/federated_queries.py)

---

**The best of both worlds: Type-safe structured API + SQL flexibility + Multi-source federation!** 🎉

