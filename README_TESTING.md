# Testing IbexDB Library with Docker

## Quick Start

```bash
# 1. Start minimal test services
cd ibexdb
docker-compose -f docker-compose.test.yml up -d

# 2. Wait for services (30 seconds)
sleep 30

# 3. Run tests
python test_with_docker.py
```

## What Gets Tested

✅ **Configuration Management**
- Loading from configuration file
- Data source registration

✅ **PostgreSQL**
- Connection
- Query execution
- DuckDB postgres_scanner

✅ **MySQL**
- Connection
- Query execution  
- DuckDB mysql_scanner

✅ **IbexDB (Iceberg)**
- Table creation
- Data writing
- Querying
- REST catalog integration

✅ **Federated Queries**
- Multi-source joins
- Cross-database queries
- PostgreSQL + MySQL joins

✅ **QueryRequest API**
- Type-safe requests
- Pydantic validation
- Response structure

## Services Included

| Service | Port | Purpose |
|---------|------|---------|
| **PostgreSQL** | 5434 | Test users & customers data |
| **MySQL** | 3308 | Test orders & products data |
| **MinIO** | 9000/9001 | S3-compatible storage |
| **Iceberg REST** | 8181 | Iceberg catalog |

## Test Data

### PostgreSQL (`testdb`)
- `users` table: 10 test users
- `customers` table: 5 test customers

### MySQL (`testdb`)
- `orders` table: 10 test orders
- `products` table: 5 test products

### IbexDB (Iceberg)
- Created dynamically during tests
- `events` table with sample events

## Commands

```bash
# Start services
docker-compose -f docker-compose.test.yml up -d

# Check service health
docker-compose -f docker-compose.test.yml ps

# View logs
docker-compose -f docker-compose.test.yml logs -f

# Stop services
docker-compose -f docker-compose.test.yml down

# Clean up (including data)
docker-compose -f docker-compose.test.yml down -v
```

## Manual Testing

### Connect to PostgreSQL
```bash
psql -h localhost -p 5434 -U testuser -d testdb
# Password: testpass

# Query
SELECT * FROM users LIMIT 5;
```

### Connect to MySQL
```bash
mysql -h localhost -P 3308 -u testuser -p testdb
# Password: testpass

# Query
SELECT * FROM orders LIMIT 5;
```

### Access MinIO Console
```
http://localhost:9001
Username: minioadmin
Password: minioadmin
```

## What's NOT Included

To keep testing lightweight, these are excluded:

❌ Redpanda (Kafka) - not needed for library testing
❌ Vault - not needed for testing  
❌ Listmonk - not needed for testing
❌ Full platform services - not needed
❌ UIs - not needed

## Minimal vs Full Setup

### Minimal (For Library Testing)
```bash
cd ibexdb
docker-compose -f docker-compose.test.yml up -d
```
- 4 services
- ~500MB memory
- Starts in ~30 seconds

### Full Platform
```bash
cd ..
docker-compose up -d
```
- 15+ services
- ~4GB memory
- Starts in ~2 minutes

## Troubleshooting

### Services won't start
```bash
# Check if ports are in use
lsof -i :5434  # PostgreSQL
lsof -i :3308  # MySQL
lsof -i :9000  # MinIO
lsof -i :8181  # Iceberg REST
```

### Tests fail with connection errors
```bash
# Wait longer for services
sleep 60

# Check service health
docker-compose -f docker-compose.test.yml ps

# View logs
docker-compose -f docker-compose.test.yml logs postgres-test
docker-compose -f docker-compose.test.yml logs mysql-test
```

### Clean slate
```bash
# Remove everything and start fresh
docker-compose -f docker-compose.test.yml down -v
docker-compose -f docker-compose.test.yml up -d
sleep 30
python test_with_docker.py
```

## Next Steps

Once tests pass:

1. **Use in ajna-db-backend**: `pip install -e ../ibexdb`
2. **Run full platform**: `cd .. && docker-compose up -d`
3. **Test integration**: Query across all sources

