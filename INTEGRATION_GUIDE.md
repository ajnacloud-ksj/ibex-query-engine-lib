# Integration Guide: IbexDB + Ajna Backend

This guide shows how to integrate IbexDB library into the Ajna DB Backend to use IbexDB tables as data sources for reports, charts, and dashboards.

## Overview

The integration allows:
- ✅ IbexDB tables as data sources in Ajna Backend
- ✅ Create reports and charts from IbexDB data
- ✅ Use IbexDB in dashboards alongside other data sources
- ✅ Full analytics capabilities on S3 data lakes
- ✅ ACID guarantees for all operations

## Step 1: Install IbexDB in Ajna Backend

### Add to requirements.txt

```bash
# In ajna-db-backend/requirements.txt
ibexdb>=0.1.0
```

### Install dependencies

```bash
cd ajna-db-backend
pip install -r requirements.txt
```

## Step 2: Update Analytics Manager

### Modify `app/services/analytics_manager.py`

Add IbexDB support to the AnalyticsManager:

```python
# Add import at top of file
from ibexdb.integrations import IbexDBDataSource

class AnalyticsManager:
    def __init__(self, config_manager_url: str, api_key: str, cors_origins: List[str]):
        # ... existing code ...
        self.ibexdb_clients = {}  # Add this line
    
    def _setup_connections(self):
        """Setup connections including IbexDB"""
        logger.info(f"📡 Setting up connections for {len(self.data_sources)} data sources")
        
        for ds_id, ds in self.data_sources.items():
            ds_type = ds.get('type')
            
            # ... existing code for postgres, mysql, etc ...
            
            # ============================================
            # ADD THIS BLOCK - IbexDB Support
            # ============================================
            elif ds_type == 'ibexdb':
                try:
                    config = ds.get('config', {})
                    self.ibexdb_clients[ds_id] = IbexDBDataSource(config)
                    logger.info(f"✅ Connected to IbexDB: {ds_id} ({config.get('tenant_id')}/{config.get('namespace')})")
                except Exception as e:
                    logger.error(f"❌ Failed to connect to IbexDB {ds_id}: {e}")
            # ============================================
    
    def execute_query(self, data_source_id: str, query_config: Dict[str, Any]) -> pl.DataFrame:
        """Execute query on specified data source"""
        ds = self.data_sources.get(data_source_id)
        if not ds:
            raise ValueError(f"Data source not found: {data_source_id}")
        
        ds_type = ds.get('type')
        
        # ... existing code ...
        
        # ============================================
        # ADD THIS BLOCK - IbexDB Query Execution
        # ============================================
        elif ds_type == 'ibexdb':
            client = self.ibexdb_clients.get(data_source_id)
            if not client:
                raise RuntimeError(f"IbexDB client not initialized: {data_source_id}")
            
            # Execute query using IbexDB adapter
            results = client.execute_query(query_config)
            
            # Convert to Polars DataFrame
            if results:
                df = pl.DataFrame(results)
            else:
                df = pl.DataFrame()
            
            return df
        # ============================================
        
        else:
            raise ValueError(f"Unsupported data source type: {ds_type}")
```

## Step 3: Configure Data Source in Config Manager

Add IbexDB data source configuration:

```json
{
  "data_sources": [
    {
      "id": "ibexdb-production",
      "name": "IbexDB Production",
      "type": "ibexdb",
      "description": "Production data lake on S3 with Iceberg",
      "enabled": true,
      "config": {
        "tenant_id": "my_company",
        "namespace": "production",
        "environment": "production"
      },
      "metadata": {
        "icon": "database",
        "color": "#00A8E8"
      }
    }
  ]
}
```

## Step 4: Create Reports from IbexDB

### Using Ajna Backend API

```python
import requests

# Create a report that queries IbexDB
report_data = {
    "report_name": "User Analytics",
    "report_description": "User metrics from IbexDB",
    "data_source_id": "ibexdb-production",
    "report_criteria": json.dumps({
        "table": "users",
        "filters": [
            {"field": "status", "operator": "eq", "value": "active"}
        ],
        "aggregations": [
            {"field": None, "function": "count", "alias": "total_users"},
            {"field": "age", "function": "avg", "alias": "avg_age"}
        ],
        "group_by": ["status", "country"]
    })
}

response = requests.post(
    "http://localhost:8000/reports/",
    json=report_data,
    headers={"Authorization": f"Bearer {access_token}"}
)

report_id = response.json()["report_id"]
print(f"Created report: {report_id}")
```

## Step 5: Create Charts from IbexDB

```python
# Create a chart that visualizes IbexDB data
chart_data = {
    "chart_name": "Users by Country",
    "chart_type": "bar",
    "report_id": report_id,
    "chart_configuration": {
        "x_field": "country",
        "y_field": "total_users",
        "title": "Active Users by Country",
        "color_scheme": "viridis"
    }
}

response = requests.post(
    "http://localhost:8000/charts/",
    json=chart_data,
    headers={"Authorization": f"Bearer {access_token}"}
)

chart_id = response.json()["chart_id"]
print(f"Created chart: {chart_id}")
```

## Step 6: Get Chart Data

```python
# Retrieve chart data for display
response = requests.post(
    f"http://localhost:8000/charts/{chart_id}/data",
    json={
        "chart_id": chart_id,
        "dashboard_filters": []  # Optional dashboard-level filters
    },
    headers={"Authorization": f"Bearer {access_token}"}
)

chart_data = response.json()
print(f"Chart has {len(chart_data['data'])} data points")
```

## Step 7: Add to Dashboard

```python
# Create dashboard with IbexDB charts
dashboard_data = {
    "dashboard_name": "IbexDB Analytics",
    "dashboard_layout": {
        "rows": [
            {
                "charts": [
                    {"chart_id": chart_id, "width": 12, "height": 6}
                ]
            }
        ]
    }
}

response = requests.post(
    "http://localhost:8000/dashboards/",
    json=dashboard_data,
    headers={"Authorization": f"Bearer {access_token}"}
)

dashboard_id = response.json()["dashboard_id"]
print(f"Created dashboard: {dashboard_id}")
```

## Advanced Features

### Multi-Source Dashboards

Combine IbexDB with other data sources in a single dashboard:

```python
dashboard_data = {
    "dashboard_name": "Unified Analytics",
    "dashboard_layout": {
        "rows": [
            {
                "charts": [
                    {"chart_id": "chart_from_postgres", "width": 6},
                    {"chart_id": "chart_from_ibexdb", "width": 6}
                ]
            }
        ]
    }
}
```

### Cross-Source Joins

Use AnalyticsManager to join data from IbexDB and other sources:

```python
# In analytics_manager.py
def join_data_sources(self, source1_id: str, source2_id: str, join_config: Dict):
    """Join data from multiple sources"""
    
    # Query first source
    df1 = self.execute_query(source1_id, join_config['source1_query'])
    
    # Query second source  
    df2 = self.execute_query(source2_id, join_config['source2_query'])
    
    # Join using Polars
    result = df1.join(
        df2,
        left_on=join_config['left_key'],
        right_on=join_config['right_key'],
        how=join_config.get('how', 'inner')
    )
    
    return result
```

### Real-time Updates

For real-time dashboards, write to IbexDB and query immediately:

```python
# Write event data to IbexDB
from ibexdb import IbexDB

db = IbexDB(tenant_id="my_company", namespace="production")

db.write("events", records=[
    {
        "event_id": "evt_123",
        "user_id": 456,
        "event_type": "page_view",
        "timestamp": datetime.now()
    }
])

# Query through Ajna Backend
# Data is immediately available for reports and dashboards
```

## Testing the Integration

### 1. Test Connection

```bash
curl -X POST http://localhost:8000/datasources/test \
  -H "Content-Type: application/json" \
  -d '{
    "data_source_id": "ibexdb-production"
  }'
```

### 2. List Tables

```bash
curl http://localhost:8000/datasources/ibexdb-production/tables \
  -H "Authorization: Bearer $TOKEN"
```

### 3. Execute Test Query

```python
# Test query through analytics endpoint
response = requests.post(
    "http://localhost:8000/analytics/query",
    json={
        "data_source_id": "ibexdb-production",
        "query": {
            "table": "users",
            "limit": 10
        }
    },
    headers={"Authorization": f"Bearer {access_token}"}
)

print(response.json())
```

## Performance Optimization

### 1. Enable Caching

IbexDB has built-in metadata caching. Configure in config.json:

```json
{
  "performance": {
    "metadata_cache_ttl": 300,
    "query_timeout_ms": 30000
  }
}
```

### 2. Use Partitioning

When creating tables in IbexDB, use partitioning for better query performance:

```python
db.create_table(
    "events",
    schema={"fields": {...}},
    partition={
        "partitions": [
            {"field": "event_date", "transform": "day"}
        ]
    }
)
```

### 3. Regular Compaction

Schedule compaction for tables with frequent writes:

```python
# Add to scheduled jobs
db.compact("users", force=False, expire_snapshots=True)
```

## Troubleshooting

### Issue: "Data source not found"

Check that IbexDB data source is registered in Config Manager and AnalyticsManager is refreshed.

### Issue: "Query timeout"

Increase timeout in config or optimize query with filters and partitioning.

### Issue: "Permission denied"

Ensure AWS credentials have access to S3 bucket and Glue catalog.

## Next Steps

- Explore [examples/](examples/) for more patterns
- Check out [query optimization guide](docs/query-optimization.md)
- Join our [Discord](https://discord.gg/ibexdb) for support

## Benefits of Integration

✅ **Unified Interface**: Query S3 data lakes through familiar Ajna UI  
✅ **ACID Guarantees**: Full consistency for all operations  
✅ **Cost Effective**: Pay only for S3 storage (~$0.023/GB/month)  
✅ **Scalable**: Handle petabytes of data with Apache Iceberg  
✅ **Time Travel**: Historical analysis built-in  
✅ **Schema Evolution**: Add columns without breaking existing queries  

