# ⚡ Quick Test Guide

## One Command Test

```bash
cd ibexdb && ./test.sh
```

That's it! The script will:
1. ✅ Start 4 Docker services
2. ✅ Wait for them to be ready
3. ✅ Run comprehensive tests
4. ✅ Show you the results

## What Gets Tested

```
┌─────────────────────────────────────────────────────────┐
│                  IbexDB Library Tests                   │
└─────────────────────────────────────────────────────────┘

Test 1: Configuration from File
  └─ Load data sources from JSON
  └─ Parse PostgreSQL, MySQL, IbexDB configs

Test 2: PostgreSQL Connection
  ├─ Connect to PostgreSQL via DuckDB postgres_scanner
  ├─ Query users table (10 sample users)
  └─ Verify results

Test 3: MySQL Connection
  ├─ Connect to MySQL via DuckDB mysql_scanner
  ├─ Query orders table (10 sample orders)
  └─ Aggregate revenue by status

Test 4: IbexDB (Iceberg) Operations
  ├─ Create Iceberg table
  ├─ Write test events
  ├─ Query with filters
  └─ Verify ACID operations

Test 5: Federated Query (Multi-Source Join)
  ├─ Attach both PostgreSQL and MySQL
  ├─ Join users (PostgreSQL) with orders (MySQL)
  ├─ Aggregate customer spending
  └─ Test cross-database query

Test 6: QueryRequest API (Type-Safe)
  ├─ Create Pydantic QueryRequest
  ├─ Validate filters, aggregations
  └─ Verify request/response models
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Test Environment                     │
└─────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  PostgreSQL  │     │    MySQL     │     │    MinIO     │
│   :5434      │     │   :3308      │     │  :9000/9001  │
│              │     │              │     │              │
│ • users      │     │ • orders     │     │ • S3 bucket  │
│ • customers  │     │ • products   │     │ • Iceberg    │
└──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                    ┌───────▼────────┐
                    │  DuckDB Engine │
                    │                │
                    │ • postgres_    │
                    │   scanner      │
                    │ • mysql_       │
                    │   scanner      │
                    │ • iceberg_scan │
                    └────────────────┘
                            │
                    ┌───────▼────────┐
                    │ Federated      │
                    │ Query Engine   │
                    │                │
                    │ Cross-database │
                    │ joins & queries│
                    └────────────────┘
```

## Sample Test Output

```bash
$ ./test.sh

🚀 IbexDB Library Test Runner
================================

📦 Starting Docker services...
✅ Container ibexdb-postgres-test started
✅ Container ibexdb-mysql-test started
✅ Container ibexdb-minio-test started
✅ Container ibexdb-iceberg-rest started

⏳ Waiting for services (30 seconds)...

🔍 Checking service health...
NAME                    STATUS    PORTS
ibexdb-postgres-test    healthy   0.0.0.0:5434->5432/tcp
ibexdb-mysql-test       healthy   0.0.0.0:3308->3306/tcp
ibexdb-minio-test       healthy   0.0.0.0:9000-9001->9000-9001/tcp
ibexdb-iceberg-rest     running   0.0.0.0:8181->8181/tcp

🧪 Running tests...
============================================================
Test 1: Configuration from File
============================================================
✅ Loaded 3 data sources from file
  - PostgreSQL Test (postgres)
  - MySQL Test (mysql)
  - IbexDB Test (ibexdb)

============================================================
Test 2: PostgreSQL Connection
============================================================
✅ PostgreSQL query succeeded: 5 rows

Sample results:
  Frank Miller (45) - Canada
  Henry Ford (40) - UK
  Jack Ryan (38) - USA

============================================================
Test 3: MySQL Connection
============================================================
✅ MySQL query succeeded: 2 rows

Sample results:
  Status: completed
    Orders: 9
    Revenue: $23250.00
    Avg: $2583.33
  Status: pending
    Orders: 1
    Revenue: $250.00
    Avg: $250.00

============================================================
Test 5: Federated Query (Multi-Source Join)
============================================================
✅ Federated engine created with 2 sources
✅ Federated query succeeded: 5 customers

Top customers:
  Alice Smith (USA)
    Orders: 3, Spent: $5200.00
  Diana Prince (USA)
    Orders: 2, Spent: $9700.00

============================================================
Test Summary
============================================================
✅ PASS: Configuration from File
✅ PASS: PostgreSQL Connection
✅ PASS: MySQL Connection
✅ PASS: IbexDB Operations
✅ PASS: Federated Query
✅ PASS: QueryRequest API

Results: 6/6 tests passed

🎉 All tests passed!

To stop services:
  docker-compose -f docker-compose.test.yml down
```

## Manual Testing

Want to test manually? Here's how:

### 1. Start Services
```bash
docker-compose -f docker-compose.test.yml up -d
sleep 30
```

### 2. Test PostgreSQL
```bash
psql -h localhost -p 5434 -U testuser testdb
# Password: testpass

SELECT name, email FROM users LIMIT 5;
```

### 3. Test MySQL
```bash
mysql -h localhost -P 3308 -u testuser -ptestpass testdb

SELECT * FROM orders LIMIT 5;
```

### 4. Test Python Library
```python
from ibexdb import FederatedQueryEngine

fed = FederatedQueryEngine()

# Add PostgreSQL
fed.add_source("pg", "postgres", {
    "host": "localhost",
    "port": 5434,
    "database": "testdb",
    "user": "testuser",
    "password": "testpass"
})

# Query
result = fed.execute_sql("SELECT * FROM pg.users LIMIT 5")
print(result.data)
```

### 5. Stop Services
```bash
docker-compose -f docker-compose.test.yml down
```

## Cleanup

```bash
# Remove all containers and volumes
docker-compose -f docker-compose.test.yml down -v

# Remove test config
rm -rf config/
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Port already in use | Check `lsof -i :5434` and kill the process |
| Services not healthy | Wait longer: `sleep 60` |
| Connection refused | Restart: `docker-compose -f docker-compose.test.yml restart` |
| Tests fail | Check logs: `docker-compose -f docker-compose.test.yml logs` |

## Next Steps

✅ Tests passed? Great! Now:

1. **Install library**: `pip install -e .`
2. **Use in your project**: `from ibexdb import IbexDB, FederatedQueryEngine`
3. **Run full platform**: `cd .. && docker-compose up -d`
4. **Integrate with ajna-db-backend**

---

**Questions?** Check `README_TESTING.md` for detailed documentation.

