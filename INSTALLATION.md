## Installation Guide

This guide covers installation and configuration of the IbexDB Python library.

### Prerequisites

- Python 3.10 or higher
- pip or uv package manager
- AWS account (for production deployment)
- S3 bucket for data storage

### Basic Installation

```bash
# Install from PyPI (when published)
pip install ibexdb

# Or install from source
git clone https://github.com/ajnacloud/ibexdb.git
cd ibexdb
pip install -e .
```

### Optional Dependencies

```bash
# For FastAPI integration (local development)
pip install ibexdb[fastapi]

# For AWS Lambda deployment
pip install ibexdb[lambda]

# For development (includes testing, linting)
pip install ibexdb[dev]

# Install everything
pip install ibexdb[all]
```

### Configuration

#### 1. Environment Variables

Create a `.env` file or export these variables:

```bash
# Required
export ENVIRONMENT=production
export BUCKET_NAME=my-iceberg-bucket
export AWS_REGION=us-east-1

# Optional (for AWS Glue in production)
export AWS_ACCOUNT_ID=123456789012

# Optional (for local development with REST catalog)
export CATALOG_URI=http://localhost:8181
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
```

#### 2. Configuration File

For advanced configuration, create `config/config.json`:

```json
{
  "production": {
    "environment": "production",
    "s3": {
      "bucket_name": "${BUCKET_NAME}",
      "warehouse_path": "iceberg-warehouse",
      "region": "${AWS_REGION}",
      "use_ssl": true
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

### Quick Start

```python
from ibexdb import IbexDB

# Initialize from environment
db = IbexDB.from_env()

# Create a table
db.create_table(
    table="users",
    schema={
        "fields": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "email": {"type": "string"}
        }
    }
)

# Write data
db.write("users", records=[
    {"id": 1, "name": "Alice", "email": "alice@example.com"}
])

# Query data
results = db.query("users", limit=10)
print(results.data.records)
```

### Integration with Ajna Backend

To use IbexDB as a data source in Ajna Backend:

```python
# In ajna-db-backend, add to requirements.txt:
ibexdb>=0.1.0

# In app/services/analytics_manager.py:
from ibexdb.integrations import IbexDBDataSource

class AnalyticsManager:
    def _setup_connections(self):
        for ds_id, ds in self.data_sources.items():
            if ds.get('type') == 'ibexdb':
                config = ds.get('config', {})
                self.data_sources[ds_id] = IbexDBDataSource(config)
```

Then configure in Config Manager:

```json
{
  "id": "ibexdb-prod",
  "name": "IbexDB Production",
  "type": "ibexdb",
  "config": {
    "tenant_id": "my_company",
    "namespace": "production"
  }
}
```

### AWS Lambda Deployment

1. **Build Docker image:**

```bash
cd ibexdb
docker build -t ibexdb-lambda .
```

2. **Push to ECR:**

```bash
aws ecr create-repository --repository-name ibexdb-lambda
docker tag ibexdb-lambda:latest <account>.dkr.ecr.<region>.amazonaws.com/ibexdb-lambda:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/ibexdb-lambda:latest
```

3. **Create Lambda function:**

```bash
aws lambda create-function \
  --function-name ibexdb-api \
  --package-type Image \
  --code ImageUri=<account>.dkr.ecr.<region>.amazonaws.com/ibexdb-lambda:latest \
  --role arn:aws:iam::<account>:role/lambda-execution-role \
  --timeout 300 \
  --memory-size 3008 \
  --environment Variables={ENVIRONMENT=production,BUCKET_NAME=my-bucket,AWS_REGION=us-east-1}
```

### Local Development

For local testing with MinIO and REST catalog:

1. **Start services:**

```bash
docker-compose up -d
```

2. **Run tests:**

```bash
pytest tests/
```

3. **Run examples:**

```bash
python examples/basic_usage.py
```

### Troubleshooting

#### DuckDB Extensions

If you get errors about missing DuckDB extensions:

```bash
# Pre-install extensions
python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL iceberg'); conn.execute('INSTALL httpfs');"
```

#### AWS Permissions

Ensure your Lambda role has these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-iceberg-bucket/*",
        "arn:aws:s3:::my-iceberg-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable"
      ],
      "Resource": "*"
    }
  ]
}
```

### Next Steps

- Check out [examples/](examples/) for more usage patterns
- Read the [API Reference](docs/api-reference.md)
- Join our [Discord community](https://discord.gg/ibexdb)

