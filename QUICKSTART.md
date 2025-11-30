# IbexDB Quick Start Guide

Get up and running with IbexDB in 5 minutes!

## 1. Install the Library

```bash
# From source (local development)
cd /path/to/ibexdb
pip install -e .

# Or install from PyPI (when published)
pip install ibexdb
```

## 2. Set Environment Variables

```bash
# For local development with MinIO
export ENVIRONMENT=development
export BUCKET_NAME=test-bucket
export AWS_REGION=us-east-1
export CATALOG_URI=http://localhost:8181
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin

# For production with AWS
export ENVIRONMENT=production
export BUCKET_NAME=your-bucket
export AWS_REGION=us-east-1
# AWS credentials from IAM role or environment
```

## 3. Your First IbexDB Program

Create `test_ibexdb.py`:

```python
#!/usr/bin/env python3
"""
Quick test of IbexDB library
"""
from ibexdb import IbexDB
from datetime import datetime

# Initialize client
print("Initializing IbexDB...")
db = IbexDB.from_env()
print(f"✓ Connected: {db}")

# Create a table
print("\nCreating table 'users'...")
response = db.create_table(
    table="users",
    schema={
        "fields": {
            "id": {"type": "integer", "required": True},
            "name": {"type": "string", "required": True},
            "email": {"type": "string"},
            "age": {"type": "integer"},
            "created_at": {"type": "timestamp"}
        },
        "primary_key": ["id"]
    }
)
print(f"✓ Table created: {response.success}")

# Write data
print("\nWriting data...")
response = db.write(
    table="users",
    records=[
        {
            "id": 1,
            "name": "Alice Smith",
            "email": "alice@example.com",
            "age": 30,
            "created_at": datetime.now()
        },
        {
            "id": 2,
            "name": "Bob Johnson",
            "email": "bob@example.com",
            "age": 25,
            "created_at": datetime.now()
        },
        {
            "id": 3,
            "name": "Charlie Brown",
            "email": "charlie@example.com",
            "age": 35,
            "created_at": datetime.now()
        }
    ]
)
print(f"✓ Wrote {response.data.records_written} records")

# Query data
print("\nQuerying data...")
response = db.query(
    table="users",
    projection=["id", "name", "email", "age"],
    sort=[{"field": "age", "order": "asc"}]
)

if response.success:
    print(f"✓ Query returned {len(response.data.records)} records:")
    for record in response.data.records:
        print(f"  {record['id']}: {record['name']} (age {record['age']})")

# Query with filter
print("\nQuerying with filter (age >= 30)...")
response = db.query(
    table="users",
    filters=[
        {"field": "age", "operator": "gte", "value": 30}
    ]
)

if response.success:
    print(f"✓ Filtered query returned {len(response.data.records)} records:")
    for record in response.data.records:
        print(f"  {record['name']}: {record['age']} years old")

# Update data
print("\nUpdating records...")
response = db.update(
    table="users",
    updates={"age": 31},
    filters=[
        {"field": "id", "operator": "eq", "value": 1}
    ]
)
print(f"✓ Updated {response.data.records_updated} records")

# Aggregation
print("\nPerforming aggregation...")
response = db.query(
    table="users",
    aggregations=[
        {"field": None, "function": "count", "alias": "total"},
        {"field": "age", "function": "avg", "alias": "avg_age"}
    ]
)

if response.success and response.data.records:
    result = response.data.records[0]
    print(f"✓ Total users: {result['total']}")
    print(f"✓ Average age: {result['avg_age']:.1f}")

print("\n" + "="*50)
print("✓ Quick start complete!")
print("="*50)
```

## 4. Run It!

```bash
python test_ibexdb.py
```

Expected output:
```
Initializing IbexDB...
✓ Connected: IbexDB(tenant_id='default', namespace='default')

Creating table 'users'...
✓ Table created: True

Writing data...
✓ Wrote 3 records

Querying data...
✓ Query returned 3 records:
  1: Alice Smith (age 30)
  2: Bob Johnson (age 25)
  3: Charlie Brown (age 35)

Querying with filter (age >= 30)...
✓ Filtered query returned 2 records:
  Alice Smith: 30 years old
  Charlie Brown: 35 years old

Updating records...
✓ Updated 1 records

Performing aggregation...
✓ Total users: 3
✓ Average age: 30.7

==================================================
✓ Quick start complete!
==================================================
```

## 5. Integrate with Ajna Backend

Add to `ajna-db-backend/requirements.txt`:
```
ibexdb>=0.1.0
```

Update `app/services/analytics_manager.py`:
```python
from ibexdb.integrations import IbexDBDataSource

# In _setup_connections():
elif ds_type == 'ibexdb':
    config = ds.get('config', {})
    self.ibexdb_clients[ds_id] = IbexDBDataSource(config)
```

Configure in Config Manager:
```json
{
  "id": "ibexdb-prod",
  "type": "ibexdb",
  "config": {
    "tenant_id": "default",
    "namespace": "default"
  }
}
```

Create a report in Ajna pointing to `ibexdb-prod` data source!

## 6. Next Steps

- Read [README.md](README.md) for comprehensive documentation
- Check [examples/](examples/) for more patterns
- Follow [INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md) for Ajna integration
- Explore [complex types](examples/basic_usage.py) (arrays, structs, maps)

## Troubleshooting

### Error: "Environment variable not set"
Set required environment variables (see step 2)

### Error: "Failed to connect to catalog"
Check that catalog URI is correct and service is running

### Error: "Permission denied on S3"
Verify AWS credentials have S3 access permissions

### Error: "Table already exists"
Use `if_not_exists=True` in create_table or drop existing table

## Support

- 📖 Documentation: [README.md](README.md)
- 💡 Examples: [examples/](examples/)
- 🐛 Issues: GitHub Issues
- 💬 Community: Discord

Happy coding with IbexDB! 🚀

