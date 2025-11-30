"""
Configuration Management Examples

Shows how to configure IbexDB with data sources from:
1. Config Manager endpoint (like ajna-db-backend)
2. Configuration file (JSON/YAML)
3. Manual configuration
4. QueryRequest/QueryResponse API (type-safe)
"""

from ibexdb import (
    IbexDB,
    FederatedQueryEngine,
    create_federated_engine,
    DataSourceConfigManager
)
from ibexdb.models import QueryRequest, Filter, AggregateField

# ============================================================================
# Example 1: Manual Configuration (Hardcoded)
# ============================================================================

def example_manual_config():
    """Manual source configuration"""
    
    print("\n" + "="*60)
    print("Example 1: Manual Configuration")
    print("="*60)
    
    # Create federated engine manually
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
    
    print(f"✓ Created engine with {len(fed.list_sources())} sources")
    for source in fed.list_sources():
        print(f"  - {source['id']} ({source['type']})")
    
    fed.close()


# ============================================================================
# Example 2: Config Manager Endpoint (Like ajna-db-backend!)
# ============================================================================

def example_config_manager_endpoint():
    """Fetch sources from Config Manager endpoint"""
    
    print("\n" + "="*60)
    print("Example 2: Config Manager Endpoint")
    print("(Like ajna-db-backend AnalyticsManager)")
    print("="*60)
    
    # Method A: Using DataSourceConfigManager directly
    config_mgr = DataSourceConfigManager.from_endpoint(
        url="http://ibex-data-platform:8080",
        api_key="my-api-key",
        auto_refresh=True,
        refresh_interval=60
    )
    
    # Get all sources
    sources = config_mgr.get_all_sources()
    print(f"✓ Fetched {len(sources)} sources from Config Manager")
    for source_id, source in sources.items():
        print(f"  - {source['name']} ({source['type']})")
    
    # Method B: Create federated engine from endpoint (one-liner!)
    fed = create_federated_engine(
        config_endpoint="http://ibex-data-platform:8080",
        api_key="my-api-key"
    )
    
    print(f"✓ Created federated engine with {len(fed.list_sources())} sources")
    
    # Query across all sources
    result = fed.execute_sql("""
        SELECT source, COUNT(*) as count
        FROM system_tables
        GROUP BY source
    """)
    
    print(f"✓ Query executed: {len(result)} results")
    
    fed.close()


# ============================================================================
# Example 3: Configuration File (JSON/YAML)
# ============================================================================

def example_config_file():
    """Load sources from configuration file"""
    
    print("\n" + "="*60)
    print("Example 3: Configuration File")
    print("="*60)
    
    # Create a sample config file
    import json
    from pathlib import Path
    
    config_data = {
        "data_sources": [
            {
                "id": "ibexdb-production",
                "name": "IbexDB Production",
                "type": "ibexdb",
                "enabled": True,
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
                "enabled": True,
                "config": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "analytics",
                    "user": "app_user",
                    "password": "secret"
                },
                "metadata": {
                    "description": "Operational analytics database"
                }
            },
            {
                "id": "mysql-orders",
                "name": "MySQL Orders",
                "type": "mysql",
                "enabled": True,
                "config": {
                    "host": "localhost",
                    "port": 3306,
                    "database": "ecommerce",
                    "user": "app_user",
                    "password": "secret"
                }
            }
        ]
    }
    
    # Write config file
    config_path = Path("datasources.json")
    with open(config_path, 'w') as f:
        json.dump(config_data, f, indent=2)
    
    print(f"✓ Created config file: {config_path}")
    
    # Load from file
    fed = create_federated_engine(
        config_file="datasources.json"
    )
    
    print(f"✓ Loaded {len(fed.list_sources())} sources from file")
    for source in fed.list_sources():
        print(f"  - {source['id']} ({source['type']})")
    
    # Clean up
    config_path.unlink()
    fed.close()


# ============================================================================
# Example 4: QueryRequest/QueryResponse API (Type-Safe!)
# ============================================================================

def example_query_request_api():
    """Use QueryRequest/QueryResponse models (like ibex-db-lambda)"""
    
    print("\n" + "="*60)
    print("Example 4: QueryRequest/QueryResponse API")
    print("(Type-safe, like ibex-db-lambda)")
    print("="*60)
    
    # Create federated engine
    fed = create_federated_engine(
        config_file="datasources.json"  # or config_endpoint
    )
    
    # Create type-safe QueryRequest
    request = QueryRequest(
        operation="QUERY",
        tenant_id="my_company",
        namespace="production",
        table="users",
        projection=["id", "name", "email", "status"],
        filters=[
            Filter(field="age", operator="gte", value=18),
            Filter(field="status", operator="eq", value="active")
        ],
        aggregations=[
            AggregateField(
                field=None,
                function="count",
                alias="total_users"
            ),
            AggregateField(
                field="age",
                function="avg",
                alias="avg_age"
            )
        ],
        group_by=["status"],
        limit=100
    )
    
    print(f"✓ Created QueryRequest")
    print(f"  Table: {request.table}")
    print(f"  Filters: {len(request.filters or [])}")
    print(f"  Aggregations: {len(request.aggregations or [])}")
    
    # Execute with type-safe response
    response = fed.execute_query_request(request)
    
    if response.success:
        print(f"✓ Query succeeded!")
        print(f"  Records: {len(response.data.records)}")
        print(f"  Execution time: {response.metadata.execution_time_ms:.2f}ms")
        
        for record in response.data.records[:5]:
            print(f"  {record}")
    else:
        print(f"✗ Query failed: {response.error.message}")
    
    fed.close()


# ============================================================================
# Example 5: Combining Config Manager with Manual Sources
# ============================================================================

def example_hybrid_config():
    """Combine Config Manager with manual sources"""
    
    print("\n" + "="*60)
    print("Example 5: Hybrid Configuration")
    print("(Config Manager + Manual Sources)")
    print("="*60)
    
    # Start with Config Manager sources
    fed = create_federated_engine(
        config_endpoint="http://config-manager:8080",
        api_key="my-api-key"
    )
    
    print(f"✓ Loaded {len(fed.list_sources())} sources from Config Manager")
    
    # Add additional manual source
    fed.add_source("temp_s3", "s3", {
        "endpoint": "http://localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "use_ssl": False
    })
    
    print(f"✓ Added manual source: temp_s3")
    print(f"✓ Total sources: {len(fed.list_sources())}")
    
    fed.close()


# ============================================================================
# Example 6: Auto-Refresh Configuration
# ============================================================================

def example_auto_refresh():
    """Automatically refresh configuration from Config Manager"""
    
    print("\n" + "="*60)
    print("Example 6: Auto-Refresh Configuration")
    print("="*60)
    
    # Create config manager with auto-refresh
    config_mgr = DataSourceConfigManager.from_endpoint(
        url="http://config-manager:8080",
        api_key="my-api-key",
        auto_refresh=True,
        refresh_interval=60  # Refresh every 60 seconds
    )
    
    # Start background refresh
    config_mgr.start_auto_refresh()
    
    print(f"✓ Config Manager started with auto-refresh (60s interval)")
    print(f"✓ Current sources: {len(config_mgr.get_all_sources())}")
    
    # Create federated engine
    fed = FederatedQueryEngine(config_manager=config_mgr)
    
    print(f"✓ Federated engine created")
    print(f"✓ Sources will auto-refresh from Config Manager")


# ============================================================================
# Example 7: Production Pattern (Like ajna-db-backend)
# ============================================================================

def example_production_pattern():
    """Production-ready pattern matching ajna-db-backend"""
    
    print("\n" + "="*60)
    print("Example 7: Production Pattern")
    print("(Matches ajna-db-backend AnalyticsManager)")
    print("="*60)
    
    import os
    
    # Get config from environment (like ajna-db-backend)
    config_manager_url = os.getenv(
        'CONFIG_MANAGER_URL',
        'http://ibex-data-platform:8080'
    )
    api_key = os.getenv('CONFIG_MANAGER_API_KEY', 'default-key')
    
    # Create federated engine from Config Manager
    fed = create_federated_engine(
        config_endpoint=config_manager_url,
        api_key=api_key
    )
    
    print(f"✓ Connected to Config Manager: {config_manager_url}")
    print(f"✓ Loaded {len(fed.list_sources())} data sources")
    
    # List sources by type (like AnalyticsManager does)
    ibexdb_sources = [s for s in fed.list_sources() if s['type'] == 'ibexdb']
    postgres_sources = [s for s in fed.list_sources() if s['type'] == 'postgres']
    mysql_sources = [s for s in fed.list_sources() if s['type'] == 'mysql']
    
    print(f"\nData Sources:")
    print(f"  IbexDB: {len(ibexdb_sources)}")
    print(f"  PostgreSQL: {len(postgres_sources)}")
    print(f"  MySQL: {len(mysql_sources)}")
    
    # Execute federated query (like AnalyticsManager)
    result = fed.execute_sql("""
        SELECT 
            source_type,
            COUNT(*) as table_count
        FROM information_schema.tables
        GROUP BY source_type
    """)
    
    print(f"\n✓ Federated query executed: {len(result)} result rows")
    
    fed.close()


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("="*60)
    print("IbexDB Configuration Management Examples")
    print("="*60)
    print("\nThree ways to configure:")
    print("1. Manual configuration (hardcoded)")
    print("2. Config Manager endpoint (like ajna-db-backend)")
    print("3. Configuration file (JSON/YAML)")
    print("\nPlus:")
    print("4. QueryRequest/QueryResponse API (type-safe)")
    print("5. Hybrid configuration")
    print("6. Auto-refresh")
    print("7. Production pattern")
    
    try:
        # Example 1: Manual
        example_manual_config()
        
        # Example 2: Config Manager endpoint
        # example_config_manager_endpoint()  # Uncomment when Config Manager is running
        
        # Example 3: Configuration file
        example_config_file()
        
        # Example 4: QueryRequest API
        # example_query_request_api()  # Uncomment when sources are configured
        
        # Example 5: Hybrid
        # example_hybrid_config()  # Uncomment when Config Manager is running
        
        # Example 6: Auto-refresh
        # example_auto_refresh()  # Uncomment when Config Manager is running
        
        # Example 7: Production pattern
        # example_production_pattern()  # Uncomment when Config Manager is running
        
        print("\n" + "="*60)
        print("✓ Examples completed!")
        print("="*60)
        print("\nKey Points:")
        print("1. ✓ Config Manager endpoint (like ajna-db-backend)")
        print("2. ✓ Configuration file (JSON/YAML)")
        print("3. ✓ Manual configuration")
        print("4. ✓ QueryRequest/QueryResponse (type-safe)")
        print("5. ✓ Auto-refresh from Config Manager")
        print("6. ✓ Production-ready patterns")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

