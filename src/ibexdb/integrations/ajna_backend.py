"""
Ajna Backend Integration

This module provides seamless integration between IbexDB and Ajna DB Backend,
allowing IbexDB tables to be used as data sources for reports, charts, and dashboards.
"""

import logging
from typing import Any, Dict, List, Optional

from ibexdb import IbexDB

logger = logging.getLogger(__name__)


class IbexDBDataSource:
    """
    IbexDB adapter for Ajna Backend's AnalyticsManager

    This class adapts IbexDB to work with Ajna's data source interface,
    allowing reports and dashboards to query IbexDB tables.

    Usage in AnalyticsManager:
        ```python
        from ibexdb.integrations import IbexDBDataSource

        # In _setup_connections():
        if ds_type == 'ibexdb':
            config = ds.get('config', {})
            self.data_sources[ds_id] = IbexDBDataSource(config)
        ```
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize IbexDB data source

        Args:
            config: Configuration dictionary with:
                - tenant_id: IbexDB tenant ID (default: "default")
                - namespace: IbexDB namespace (default: "default")
                - environment: Environment name (optional)
        """
        self.config = config
        self.tenant_id = config.get("tenant_id", "default")
        self.namespace = config.get("namespace", "default")
        self.type = "ibexdb"

        # Initialize IbexDB client
        try:
            self.client = IbexDB(tenant_id=self.tenant_id, namespace=self.namespace)
            logger.info(f"✓ IbexDB data source initialized: {self.tenant_id}/{self.namespace}")
        except Exception as e:
            logger.error(f"✗ Failed to initialize IbexDB: {e}")
            raise

    def execute_query(
        self, query_dict: Dict[str, Any], limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute query and return results

        This matches the interface expected by Ajna's AnalyticsManager.

        Args:
            query_dict: Query configuration with:
                - table: Table name (required)
                - projection: List of fields to select
                - filters: List of filter conditions
                - aggregations: List of aggregations
                - group_by: Group by fields
                - sort: Sort configuration
                - limit: Row limit
            limit: Override limit (optional)

        Returns:
            List of result records

        Raises:
            ValueError: If table is not specified
            RuntimeError: If query execution fails
        """
        table = query_dict.get("table")
        if not table:
            raise ValueError("table is required in query configuration")

        # Execute query
        try:
            response = self.client.query(
                table=table,
                projection=query_dict.get("projection"),
                filters=query_dict.get("filters"),
                aggregations=query_dict.get("aggregations"),
                group_by=query_dict.get("group_by"),
                sort=query_dict.get("sort"),
                limit=limit or query_dict.get("limit", 1000),
            )

            if not response.success:
                error_msg = response.error.message if response.error else "Unknown error"
                raise RuntimeError(f"Query failed: {error_msg}")

            return response.data.records

        except Exception as e:
            logger.error(f"✗ IbexDB query failed: {e}")
            raise

    def execute_sql(self, sql: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query (not supported by IbexDB yet)

        This is included for interface compatibility but raises NotImplementedError.
        Use execute_query() with query_dict instead.

        Args:
            sql: SQL query string
            limit: Row limit

        Raises:
            NotImplementedError: SQL interface not yet supported
        """
        raise NotImplementedError(
            "IbexDB does not support raw SQL queries yet. "
            "Use execute_query() with a query dictionary instead."
        )

    def get_table_schema(self, table: str) -> Dict[str, Any]:
        """
        Get table schema

        Args:
            table: Table name

        Returns:
            Schema definition with fields and types

        Raises:
            RuntimeError: If schema retrieval fails
        """
        try:
            response = self.client.describe_table(table)

            if not response.success:
                error_msg = response.error.message if response.error else "Unknown error"
                raise RuntimeError(f"Failed to get schema: {error_msg}")

            return response.data.table.table_schema or {}

        except Exception as e:
            logger.error(f"✗ Failed to get table schema: {e}")
            raise

    def list_tables(self) -> List[str]:
        """
        List all tables in namespace

        Returns:
            List of table names

        Raises:
            RuntimeError: If listing fails
        """
        try:
            response = self.client.list_tables()

            if not response.success:
                error_msg = response.error.message if response.error else "Unknown error"
                raise RuntimeError(f"Failed to list tables: {error_msg}")

            return response.data.tables

        except Exception as e:
            logger.error(f"✗ Failed to list tables: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test if connection is working

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            self.list_tables()
            logger.info(f"✓ IbexDB connection test passed: {self.tenant_id}/{self.namespace}")
            return True
        except Exception as e:
            logger.error(f"✗ IbexDB connection test failed: {e}")
            return False

    def close(self):
        """
        Close connection (no-op for IbexDB)

        Included for interface compatibility. IbexDB connections are stateless.
        """
        pass

    def __repr__(self) -> str:
        return f"IbexDBDataSource(tenant_id='{self.tenant_id}', namespace='{self.namespace}')"


def create_ibexdb_datasource(config: Dict[str, Any]) -> IbexDBDataSource:
    """
    Factory function to create IbexDB data source

    This can be used by Ajna's AnalyticsManager to instantiate data sources.

    Args:
        config: Configuration dictionary

    Returns:
        IbexDBDataSource instance

    Example:
        ```python
        # In AnalyticsManager
        datasource_factories = {
            'postgres': create_postgres_datasource,
            'mysql': create_mysql_datasource,
            'ibexdb': create_ibexdb_datasource,  # Add this
        }

        # Use it
        datasource = datasource_factories[ds_type](config)
        ```
    """
    return IbexDBDataSource(config)


# ============================================================================
# Helper Functions for Ajna Integration
# ============================================================================


def convert_ajna_filters_to_ibexdb(ajna_filters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Ajna filter format to IbexDB filter format

    Ajna and IbexDB use similar but slightly different filter formats.
    This function handles any necessary conversions.

    Args:
        ajna_filters: Filters in Ajna format

    Returns:
        Filters in IbexDB format
    """
    # Currently both use the same format, but this provides a hook
    # for future format differences
    ibexdb_filters = []

    for f in ajna_filters:
        # Map column -> field if needed
        filter_dict = {
            "field": f.get("field") or f.get("column"),
            "operator": f.get("operator"),
            "value": f.get("value"),
        }
        ibexdb_filters.append(filter_dict)

    return ibexdb_filters


def convert_ajna_aggregations_to_ibexdb(ajna_aggs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Ajna aggregation format to IbexDB format

    Args:
        ajna_aggs: Aggregations in Ajna format

    Returns:
        Aggregations in IbexDB format
    """
    # Map common aggregation function names
    function_map = {
        "count": "count",
        "sum": "sum",
        "avg": "avg",
        "average": "avg",
        "min": "min",
        "max": "max",
        "mean": "avg",
    }

    ibexdb_aggs = []

    for agg in ajna_aggs:
        func = agg.get("function") or agg.get("op")
        ibexdb_aggs.append(
            {
                "field": agg.get("field"),
                "function": function_map.get(func, func),
                "alias": agg.get("alias"),
                "distinct": agg.get("distinct", False),
            }
        )

    return ibexdb_aggs
