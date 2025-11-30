"""
Integration Example: Using IbexDB with Ajna DB Backend

This shows how to integrate IbexDB as a data source in the Ajna DB Backend
for creating reports, charts, and dashboards on IbexDB tables.
"""

import os
import json
from typing import Dict, List, Any
from datetime import datetime

from ibexdb import IbexDB, IbexConfig


# ============================================================================
# IbexDB Data Source Adapter for Ajna Backend
# ============================================================================

class IbexDBDataSource:
    """
    Adapter to use IbexDB as a data source in Ajna Backend
    
    This allows Ajna's AnalyticsManager to query IbexDB tables
    for reports, charts, and dashboards.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize IbexDB data source
        
        Args:
            config: Data source configuration with:
                - tenant_id: IbexDB tenant ID
                - namespace: IbexDB namespace
                - environment: Environment name (optional)
        """
        self.config = config
        self.tenant_id = config.get("tenant_id", "default")
        self.namespace = config.get("namespace", "default")
        
        # Initialize IbexDB client
        self.db = IbexDB(
            tenant_id=self.tenant_id,
            namespace=self.namespace
        )
    
    def execute_query(self, query_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute query and return results in Ajna format
        
        Args:
            query_config: Query configuration with:
                - table: Table name
                - filters: List of filter conditions
                - aggregations: List of aggregations
                - group_by: Group by fields
                - sort: Sort configuration
                - limit: Row limit
                
        Returns:
            List of records
        """
        table = query_config.get("table")
        if not table:
            raise ValueError("table is required in query_config")
        
        # Execute query using IbexDB client
        response = self.db.query(
            table=table,
            projection=query_config.get("projection"),
            filters=query_config.get("filters"),
            aggregations=query_config.get("aggregations"),
            group_by=query_config.get("group_by"),
            sort=query_config.get("sort"),
            limit=query_config.get("limit", 1000)
        )
        
        if not response.success:
            raise RuntimeError(f"Query failed: {response.error.message if response.error else 'Unknown error'}")
        
        return response.data.records
    
    def get_table_schema(self, table: str) -> Dict[str, Any]:
        """
        Get table schema for report configuration
        
        Args:
            table: Table name
            
        Returns:
            Schema definition with fields and types
        """
        response = self.db.describe_table(table)
        
        if not response.success:
            raise RuntimeError(f"Failed to describe table: {response.error.message if response.error else 'Unknown error'}")
        
        return response.data.table.table_schema
    
    def list_tables(self) -> List[str]:
        """
        List all available tables
        
        Returns:
            List of table names
        """
        response = self.db.list_tables()
        
        if not response.success:
            raise RuntimeError(f"Failed to list tables: {response.error.message if response.error else 'Unknown error'}")
        
        return response.data.tables
    
    def test_connection(self) -> bool:
        """
        Test if connection is working
        
        Returns:
            True if connection is healthy
        """
        try:
            self.list_tables()
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False


# ============================================================================
# Example: Registering IbexDB in Ajna Backend
# ============================================================================

def example_register_ibexdb_datasource():
    """
    Example of how to register IbexDB as a data source in Ajna Backend
    
    This would typically be done through the Config Manager or database.
    """
    
    # Data source configuration for Ajna Backend
    datasource_config = {
        "id": "ibexdb-production",
        "name": "IbexDB Production",
        "type": "ibexdb",
        "description": "Production IbexDB tables",
        "config": {
            "tenant_id": "my_company",
            "namespace": "production",
            "environment": "production"
        },
        "connection_string": None,  # Not needed for IbexDB
        "enabled": True
    }
    
    print("Data Source Configuration for Ajna Backend:")
    print(json.dumps(datasource_config, indent=2))
    
    # Test the data source
    ds = IbexDBDataSource(datasource_config["config"])
    if ds.test_connection():
        print("\n✓ IbexDB data source connection successful!")
        
        # List tables
        tables = ds.list_tables()
        print(f"\n✓ Found {len(tables)} tables:")
        for table in tables:
            print(f"  - {table}")
    else:
        print("\n✗ IbexDB data source connection failed")
    
    return datasource_config


# ============================================================================
# Example: Creating a Report on IbexDB Data
# ============================================================================

def example_create_report_from_ibexdb():
    """
    Example of creating an Ajna report that queries IbexDB
    """
    
    # Report configuration
    report_config = {
        "report_name": "User Analytics",
        "report_description": "Analytics on user data from IbexDB",
        "data_source_id": "ibexdb-production",
        "query": {
            "table": "users",
            "projection": ["status", "created_at"],
            "filters": [
                {"field": "status", "operator": "eq", "value": "active"}
            ],
            "aggregations": [
                {"field": None, "function": "count", "alias": "total_users"},
                {"field": "age", "function": "avg", "alias": "avg_age"}
            ],
            "group_by": ["status"]
        }
    }
    
    print("\nReport Configuration:")
    print(json.dumps(report_config, indent=2))
    
    # Execute query
    ds = IbexDBDataSource({"tenant_id": "my_company", "namespace": "production"})
    results = ds.execute_query(report_config["query"])
    
    print(f"\n✓ Report executed successfully!")
    print(f"  Results: {len(results)} rows")
    for row in results:
        print(f"  {row}")
    
    return report_config


# ============================================================================
# Example: Chart Data from IbexDB
# ============================================================================

def example_chart_from_ibexdb():
    """
    Example of creating chart data from IbexDB for Ajna dashboards
    """
    
    # Chart configuration
    chart_config = {
        "chart_name": "Users by Status",
        "chart_type": "pie",
        "data_source_id": "ibexdb-production",
        "query": {
            "table": "users",
            "projection": ["status"],
            "aggregations": [
                {"field": None, "function": "count", "alias": "user_count"}
            ],
            "group_by": ["status"]
        },
        "chart_configuration": {
            "x_field": "status",
            "y_field": "user_count",
            "title": "Users by Status"
        }
    }
    
    print("\nChart Configuration:")
    print(json.dumps(chart_config, indent=2))
    
    # Get chart data
    ds = IbexDBDataSource({"tenant_id": "my_company", "namespace": "production"})
    results = ds.execute_query(chart_config["query"])
    
    print(f"\n✓ Chart data retrieved!")
    print(f"  Data points: {len(results)}")
    
    return chart_config


# ============================================================================
# Example: Analytics Manager Integration
# ============================================================================

def example_analytics_manager_integration():
    """
    Example showing how Ajna's AnalyticsManager can use IbexDB
    
    This demonstrates the integration pattern where AnalyticsManager
    treats IbexDB as another data source alongside PostgreSQL, MySQL, etc.
    """
    
    print("\nAnalyticsManager Integration Pattern:")
    print("="*60)
    
    # In ajna-db-backend/app/services/analytics_manager.py
    integration_code = '''
    # Add IbexDB support to AnalyticsManager
    
    from ibexdb import IbexDB
    
    class AnalyticsManager:
        def _setup_connections(self):
            """Setup connections including IbexDB"""
            for ds_id, ds in self.data_sources.items():
                ds_type = ds.get('type')
                
                # ... existing code for postgres, mysql, etc ...
                
                # Add IbexDB support
                if ds_type == 'ibexdb':
                    config = ds.get('config', {})
                    self.ibexdb_clients[ds_id] = IbexDB(
                        tenant_id=config.get('tenant_id', 'default'),
                        namespace=config.get('namespace', 'default')
                    )
                    logger.info(f"✓ Connected to IbexDB: {ds_id}")
        
        def execute_query(self, data_source_id: str, query: str):
            """Execute query on appropriate data source"""
            ds = self.data_sources.get(data_source_id)
            if not ds:
                raise ValueError(f"Data source not found: {data_source_id}")
            
            ds_type = ds.get('type')
            
            if ds_type == 'ibexdb':
                # Execute IbexDB query
                client = self.ibexdb_clients[data_source_id]
                response = client.query(
                    table=query.get('table'),
                    filters=query.get('filters'),
                    aggregations=query.get('aggregations'),
                    # ... other parameters ...
                )
                return response.data.records
            
            # ... existing code for other data sources ...
    '''
    
    print(integration_code)


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("="*60)
    print("IbexDB + Ajna Backend Integration Examples")
    print("="*60)
    
    try:
        print("\n1. Register IbexDB Data Source")
        example_register_ibexdb_datasource()
        
        print("\n2. Create Report from IbexDB")
        example_create_report_from_ibexdb()
        
        print("\n3. Create Chart from IbexDB")
        example_chart_from_ibexdb()
        
        print("\n4. Analytics Manager Integration")
        example_analytics_manager_integration()
        
        print("\n" + "="*60)
        print("✓ Integration examples completed!")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

