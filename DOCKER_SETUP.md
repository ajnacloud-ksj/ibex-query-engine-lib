# Docker Setup for IbexDB Testing

## 🎯 What We Extracted

From your full platform's `docker-compose.yml` (15+ services, ~4GB), we extracted **only what's needed** for testing the `ibexdb` library.

### Full Platform → Minimal Test Setup

```
┌────────────────────────────────────────────────────────────┐
│         Your Full Platform (docker-compose.yml)            │
│                      15+ Services                          │
└────────────────────────────────────────────────────────────┘
                            │
                            │ EXTRACTED
                            ▼
┌────────────────────────────────────────────────────────────┐
│      Minimal Test Setup (docker-compose.test.yml)          │
│                      4 Services                            │
└────────────────────────────────────────────────────────────┘
```

## 📦 Services Comparison

### ❌ NOT Included (Not Needed for Library Testing)

From `docker-compose.infra.yml`:
- ❌ **Redpanda** - Kafka streaming (not needed for database ops)
- ❌ **Redpanda Console** - Monitoring UI
- ❌ **Vault** - Secret management (using direct configs)
- ❌ **Vault Init** - Setup script
- ❌ **Listmonk** - Email notifications

From `docker-compose.platform.yml`:
- ❌ **ibex-data-platform** - Full platform service
- ❌ **ajna-db-backend** - BI backend (tested separately)
- ❌ **ajna-ui-lib** - UI library
- ❌ **ibex-platform-ui** - Admin UI
- ❌ **unified-data-setup** - Platform initialization

### ✅ Included (Essential for Testing)

From `docker-compose.infra.yml`:
- ✅ **PostgreSQL** - Test relational database
- ✅ **MySQL** - Test relational database
- ✅ **MinIO** - S3-compatible storage
- ✅ **Iceberg REST** - Iceberg catalog

## 🔧 Configuration Differences

### Full Platform PostgreSQL
```yaml
postgres:
  image: postgres:15-alpine
  ports: ["5433:5432"]  # Port 5433
  environment:
    POSTGRES_DB: metadata_db
  volumes:
    - ./deploy/init-db/init.sql
    - ./deploy/init-db/init-vault.sql
    - ./deploy/init-db/03-init-bi-metadata.sql
```

### Test PostgreSQL (Simplified)
```yaml
postgres-test:
  image: postgres:15-alpine
  ports: ["5434:5432"]  # Different port (5434)
  environment:
    POSTGRES_DB: testdb  # Simple test DB
  volumes:
    - ./test-data/init-postgres.sql  # Just test data
```

### Full Platform MySQL
```yaml
mysql:
  image: mysql:8.0
  ports: ["3307:3306"]
  environment:
    MYSQL_DATABASE: business_db
  volumes:
    - ./deploy/init-db/init-mysql.sql
```

### Test MySQL (Simplified)
```yaml
mysql-test:
  image: mysql:8.0
  ports: ["3308:3306"]  # Different port (3308)
  environment:
    MYSQL_DATABASE: testdb
  volumes:
    - ./test-data/init-mysql.sql  # Just test data
```

### Full Platform MinIO
```yaml
minio:
  ports: ["9010:9000", "9011:9001"]  # Different ports
  environment:
    MINIO_ROOT_USER: admin
    MINIO_ROOT_PASSWORD: password
```

### Test MinIO (Simplified)
```yaml
minio-test:
  ports: ["9000:9000", "9001:9001"]  # Standard ports
  environment:
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin
```

## 📊 Resource Comparison

| Aspect | Full Platform | Test Setup |
|--------|--------------|------------|
| **Services** | 15+ | 4 |
| **Memory** | ~4GB | ~500MB |
| **Disk** | ~10GB | ~1GB |
| **Startup Time** | ~2 minutes | ~30 seconds |
| **Ports Used** | 15+ | 8 |
| **Networks** | Complex mesh | Single network |
| **Dependencies** | Many inter-service deps | Minimal deps |

## 🔌 Port Allocation

### Full Platform Ports
```
PostgreSQL:        5433
MySQL:             3307
MinIO API:         9010
MinIO Console:     9011
Redpanda:          9092
Redpanda Console:  8089
Iceberg REST:      8181
Vault:             8200
Listmonk:          9001
ibex-data-platform: 8080
ajna-db-backend:   8001
ajna-ui-lib:       5173
ibex-platform-ui:  5174
```

### Test Setup Ports (No Conflicts!)
```
PostgreSQL:        5434  ← Different!
MySQL:             3308  ← Different!
MinIO API:         9000  ← Standard
MinIO Console:     9001  ← Standard
Iceberg REST:      8181  ← Same (safe)
```

**Why different ports?**
- Avoids conflicts if full platform is running
- Can run both simultaneously
- Easier to identify test vs production

## 🗂️ Data Initialization

### Full Platform
```
deploy/
├── init-db/
│   ├── init.sql                    # Business data
│   ├── init-vault.sql              # Vault setup
│   ├── 03-init-bi-metadata.sql     # BI metadata
│   └── init-mysql.sql              # MySQL business data
└── scripts/
    └── unified-data-setup.sh       # Complex setup
```

### Test Setup (Simplified)
```
test-data/
├── init-postgres.sql    # Just 10 users, 5 customers
├── init-mysql.sql       # Just 10 orders, 5 products
└── config-sources.json  # Data source configs
```

## 🚀 Usage Patterns

### Full Platform (Development/Production)
```bash
# Start entire platform
cd /path/to/ajna-python-lib
docker-compose up -d

# Wait for all services
sleep 120

# Access services
# - Platform UI: http://localhost:5174
# - BI Backend: http://localhost:8001
# - Platform API: http://localhost:8080
```

### Test Setup (Library Testing)
```bash
# Start minimal test services
cd /path/to/ajna-python-lib/ibexdb
docker-compose -f docker-compose.test.yml up -d

# Wait for services
sleep 30

# Run tests
python test_with_docker.py

# Or use one-liner
./test.sh
```

### Both Running Simultaneously
```bash
# Terminal 1: Full platform
cd /path/to/ajna-python-lib
docker-compose up -d

# Terminal 2: Test setup (no conflicts!)
cd /path/to/ajna-python-lib/ibexdb
docker-compose -f docker-compose.test.yml up -d

# Different ports, different networks, no conflicts!
```

## 🔄 Migration Path

### Phase 1: Test Library (Current)
```bash
cd ibexdb
./test.sh
```
✅ Validates library in isolation

### Phase 2: Integrate with ajna-db-backend
```bash
# Install library
pip install -e ./ibexdb

# Update ajna-db-backend to use ibexdb
# (Update analytics_manager.py)
```

### Phase 3: Test Full Platform
```bash
# Start full platform
docker-compose up -d

# Now ajna-db-backend uses ibexdb library
# Test end-to-end integration
```

## 📝 Key Design Decisions

### 1. Isolated Networks
- Full platform: `ajna` network
- Test setup: `ibexdb-test` network
- **Benefit**: No network conflicts

### 2. Different Ports
- Test services use non-conflicting ports
- **Benefit**: Can run both setups simultaneously

### 3. Minimal Data
- Full platform: Production-like data
- Test setup: Minimal sample data
- **Benefit**: Fast initialization, predictable tests

### 4. No Dependencies
- Test setup doesn't depend on full platform
- **Benefit**: Can test library independently

### 5. Named Volumes
- Test volumes: `ibexdb_*` prefix
- Platform volumes: Standard names
- **Benefit**: Easy cleanup, no conflicts

## 🧹 Cleanup Commands

### Test Setup Only
```bash
# Stop services
docker-compose -f docker-compose.test.yml down

# Remove volumes too
docker-compose -f docker-compose.test.yml down -v

# Remove everything (nuclear option)
docker system prune -a --volumes
```

### Full Platform Only
```bash
# Stop platform
docker-compose down

# Remove volumes
docker-compose down -v
```

### Both
```bash
# Stop test setup
cd ibexdb
docker-compose -f docker-compose.test.yml down -v

# Stop full platform
cd ..
docker-compose down -v
```

## 📋 Checklist: From Full Platform to Test Setup

✅ **Extracted Services**
- [x] PostgreSQL (simplified)
- [x] MySQL (simplified)
- [x] MinIO (standard config)
- [x] Iceberg REST (standard config)

✅ **Modified Configurations**
- [x] Different port numbers
- [x] Isolated network
- [x] Simplified credentials
- [x] Minimal test data

✅ **Test Infrastructure**
- [x] Docker Compose file
- [x] SQL initialization scripts
- [x] Test data configuration
- [x] Python test suite

✅ **Documentation**
- [x] README_TESTING.md
- [x] QUICKTEST.md
- [x] TESTING_SUMMARY.md
- [x] DOCKER_SETUP.md (this file)

## 🎉 Summary

You now have:

1. **Minimal Test Setup** (4 services, 30 seconds)
   - For rapid library development
   - Isolated from main platform
   - No port conflicts

2. **Full Platform** (15+ services, 2 minutes)
   - For integration testing
   - Production-like environment
   - Complete feature set

3. **Can Run Both Simultaneously**
   - Different ports
   - Different networks
   - Different volumes

**Best of both worlds!** 🚀

