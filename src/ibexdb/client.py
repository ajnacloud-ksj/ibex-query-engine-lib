"""
IbexDB Client - High-level interface for database operations

This provides a clean, pythonic API for interacting with IbexDB.
It wraps the underlying DatabaseOperations with a simpler interface.
"""

import os
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from ibexdb.config import Config as IbexConfig, get_config

logger = logging.getLogger(__name__)
from ibexdb.operations import FullIcebergOperations as DatabaseOperations
from ibexdb.models import (
    QueryRequest, QueryResponse,
    WriteRequest, WriteResponse, WriteMode,
    UpdateRequest, UpdateResponse,
    DeleteRequest, DeleteResponse, DeleteMode,
    HardDeleteRequest, HardDeleteResponse,
    CompactRequest, CompactResponse,
    CreateTableRequest, CreateTableResponse,
    ListTablesRequest, ListTablesResponse,
    DescribeTableRequest, DescribeTableResponse,
    Filter, SortField, AggregateField,
    SchemaDefinition, PartitionConfig, TableProperties,
)


class IbexDB:
    """
    High-level client for IbexDB operations
    
    This class provides a clean, pythonic interface for database operations.
    It handles request/response conversion and provides sensible defaults.
    
    Example:
        ```python
        from ibexdb import IbexDB
        
        # Initialize from environment
        db = IbexDB.from_env()
        
        # Or with explicit config
        db = IbexDB(tenant_id="my_app", namespace="production")
        
        # Query data
        results = db.query("users", filters=[
            {"field": "age", "operator": "gte", "value": 18}
        ])
        
        # Write data
        db.write("users", records=[
            {"id": 1, "name": "Alice", "age": 30}
        ])
        ```
    """
    
    def __init__(
        self,
        tenant_id: str = "default",
        namespace: str = "default",
        config: Optional[IbexConfig] = None,
        operations: Optional[DatabaseOperations] = None
    ):
        """
        Initialize IbexDB client
        
        Args:
            tenant_id: Multi-tenant identifier (default: "default")
            namespace: Table namespace (default: "default")
            config: IbexConfig instance (default: loads from env)
            operations: DatabaseOperations instance (default: creates new)
        """
        self.tenant_id = tenant_id
        self.namespace = namespace
        self._config = config or get_config()
        self._ops = operations or DatabaseOperations()
    
    @classmethod
    def from_env(
        cls,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None
    ) -> "IbexDB":
        """
        Create IbexDB client from environment variables
        
        Args:
            tenant_id: Override tenant_id (default: from env or "default")
            namespace: Override namespace (default: from env or "default")
            
        Returns:
            IbexDB client instance
            
        Environment Variables:
            IBEX_TENANT_ID: Default tenant ID
            IBEX_NAMESPACE: Default namespace
            ENVIRONMENT: Environment name (development, production, etc.)
        """
        tenant_id = tenant_id or os.getenv("IBEX_TENANT_ID", "default")
        namespace = namespace or os.getenv("IBEX_NAMESPACE", "default")
        return cls(tenant_id=tenant_id, namespace=namespace)
    
    # ========================================================================
    # Query Operations
    # ========================================================================
    
    def query(
        self,
        table: str,
        projection: Optional[List[str]] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        aggregations: Optional[List[Dict[str, Any]]] = None,
        group_by: Optional[List[str]] = None,
        sort: Optional[List[Dict[str, Any]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        distinct: bool = False,
        as_of: Optional[datetime] = None,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> QueryResponse:
        """
        Query data from a table
        
        Args:
            table: Table name
            projection: Fields to select (default: ["*"])
            filters: Filter conditions (all ANDed together)
            aggregations: Aggregation functions
            group_by: Group by fields
            sort: Sort order
            limit: Maximum rows to return
            offset: Number of rows to skip
            distinct: Return distinct rows
            as_of: Query data as of timestamp (time travel)
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            QueryResponse with data and metadata
            
        Example:
            ```python
            results = db.query(
                "users",
                projection=["id", "name", "email"],
                filters=[
                    {"field": "age", "operator": "gte", "value": 18},
                    {"field": "status", "operator": "eq", "value": "active"}
                ],
                sort=[{"field": "name", "order": "asc"}],
                limit=100
            )
            
            for record in results.data.records:
                print(record["name"])
            ```
        """
        # Build request
        request = QueryRequest(
            operation="QUERY",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
            table=table,
            projection=projection or ["*"],
            filters=[Filter(**f) for f in filters] if filters else None,
            aggregations=[AggregateField(**a) for a in aggregations] if aggregations else None,
            group_by=group_by,
            sort=[SortField(**s) for s in sort] if sort else None,
            limit=limit,
            offset=offset,
            distinct=distinct,
            as_of=as_of,
        )
        
        # Execute query
        return self._ops.query(request)
    
    # ========================================================================
    # Write Operations
    # ========================================================================
    
    def write(
        self,
        table: str,
        records: List[Dict[str, Any]],
        mode: str = "append",
        schema: Optional[Dict[str, Any]] = None,
        partition: Optional[Dict[str, Any]] = None,
        properties: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> WriteResponse:
        """
        Write records to a table
        
        Args:
            table: Table name
            records: List of records to write
            mode: Write mode - "append", "overwrite", "upsert" (default: "append")
            schema: Table schema (for first write)
            partition: Partitioning configuration
            properties: Table properties
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            WriteResponse with write results
            
        Example:
            ```python
            response = db.write(
                "users",
                records=[
                    {"id": 1, "name": "Alice", "email": "alice@example.com"},
                    {"id": 2, "name": "Bob", "email": "bob@example.com"}
                ],
                mode="append"
            )
            
            print(f"Wrote {response.data.records_written} records")
            ```
        """
        request = WriteRequest(
            operation="WRITE",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
            table=table,
            records=records,
            mode=WriteMode(mode),
            table_schema=SchemaDefinition(**schema) if schema else None,
            partition=PartitionConfig(**partition) if partition else None,
            properties=TableProperties(**properties) if properties else None,
        )
        
        return self._ops.write(request)
    
    def insert(
        self,
        table: str,
        records: List[Dict[str, Any]],
        **kwargs
    ) -> WriteResponse:
        """Alias for write() with mode='append'"""
        return self.write(table, records, mode="append", **kwargs)
    
    def upsert(
        self,
        table: str,
        records: List[Dict[str, Any]],
        **kwargs
    ) -> WriteResponse:
        """Alias for write() with mode='upsert'"""
        return self.write(table, records, mode="upsert", **kwargs)
    
    # ========================================================================
    # Update Operations
    # ========================================================================
    
    def update(
        self,
        table: str,
        updates: Dict[str, Any],
        filters: List[Dict[str, Any]],
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> UpdateResponse:
        """
        Update records matching filters
        
        Args:
            table: Table name
            updates: Dictionary of field -> new value
            filters: Filter conditions (all ANDed together)
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            UpdateResponse with update results
            
        Example:
            ```python
            response = db.update(
                "users",
                updates={"status": "active", "updated_at": datetime.now()},
                filters=[
                    {"field": "last_login", "operator": "gte", "value": "2024-01-01"}
                ]
            )
            
            print(f"Updated {response.data.records_updated} records")
            ```
        """
        request = UpdateRequest(
            operation="UPDATE",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
            table=table,
            updates=updates,
            filters=[Filter(**f) for f in filters],
        )
        
        return self._ops.update(request)
    
    # ========================================================================
    # Delete Operations
    # ========================================================================
    
    def delete(
        self,
        table: str,
        filters: List[Dict[str, Any]],
        mode: str = "soft",
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> DeleteResponse:
        """
        Delete records matching filters (soft delete by default)
        
        Args:
            table: Table name
            filters: Filter conditions (all ANDed together)
            mode: "soft" (mark as deleted) or "hard" (physical removal)
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            DeleteResponse with delete results
            
        Example:
            ```python
            # Soft delete (reversible)
            response = db.delete(
                "users",
                filters=[{"field": "status", "operator": "eq", "value": "inactive"}],
                mode="soft"
            )
            
            print(f"Deleted {response.data.records_deleted} records")
            ```
        """
        request = DeleteRequest(
            operation="DELETE",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
            table=table,
            filters=[Filter(**f) for f in filters],
            mode=DeleteMode(mode),
        )
        
        return self._ops.delete(request)
    
    def hard_delete(
        self,
        table: str,
        filters: List[Dict[str, Any]],
        confirm: bool = False,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> HardDeleteResponse:
        """
        Physically delete records (irreversible)
        
        Args:
            table: Table name
            filters: Filter conditions (all ANDed together)
            confirm: Must be True to confirm physical deletion
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            HardDeleteResponse with delete results
            
        Warning:
            This operation is irreversible. Data will be physically removed.
            
        Example:
            ```python
            response = db.hard_delete(
                "users",
                filters=[{"field": "created_at", "operator": "lt", "value": "2020-01-01"}],
                confirm=True  # Must explicitly confirm
            )
            ```
        """
        if not confirm:
            raise ValueError(
                "hard_delete requires confirm=True to prevent accidental data loss"
            )
        
        request = HardDeleteRequest(
            operation="HARD_DELETE",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
            table=table,
            filters=[Filter(**f) for f in filters],
            confirm=confirm,
        )
        
        return self._ops.hard_delete(request)
    
    # ========================================================================
    # Table Operations
    # ========================================================================
    
    def create_table(
        self,
        table: str,
        schema: Dict[str, Any],
        partition: Optional[Dict[str, Any]] = None,
        properties: Optional[Dict[str, Any]] = None,
        if_not_exists: bool = True,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> CreateTableResponse:
        """
        Create a new table
        
        Args:
            table: Table name
            schema: Table schema definition
            partition: Partitioning configuration
            properties: Table properties
            if_not_exists: Don't error if table exists
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            CreateTableResponse with creation results
            
        Example:
            ```python
            db.create_table(
                "users",
                schema={
                    "fields": {
                        "id": {"type": "integer", "required": True},
                        "name": {"type": "string", "required": True},
                        "email": {"type": "string"},
                        "created_at": {"type": "timestamp"}
                    },
                    "primary_key": ["id"]
                }
            )
            ```
        """
        request = CreateTableRequest(
            operation="CREATE_TABLE",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
            table=table,
            table_schema=SchemaDefinition(**schema),
            partition=PartitionConfig(**partition) if partition else None,
            properties=TableProperties(**properties) if properties else None,
            if_not_exists=if_not_exists,
        )
        
        return self._ops.create_table(request)
    
    def list_tables(
        self,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> ListTablesResponse:
        """
        List all tables in namespace
        
        Args:
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            ListTablesResponse with table names
            
        Example:
            ```python
            response = db.list_tables()
            for table_name in response.data.tables:
                print(table_name)
            ```
        """
        request = ListTablesRequest(
            operation="LIST_TABLES",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
        )
        
        return self._ops.list_tables(request)
    
    def describe_table(
        self,
        table: str,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> DescribeTableResponse:
        """
        Get table schema and metadata
        
        Args:
            table: Table name
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            DescribeTableResponse with table details
            
        Example:
            ```python
            response = db.describe_table("users")
            print(f"Table: {response.data.table.table_name}")
            print(f"Rows: {response.data.table.row_count}")
            print(f"Size: {response.data.table.size_bytes}")
            ```
        """
        request = DescribeTableRequest(
            operation="DESCRIBE_TABLE",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
            table=table,
        )
        
        return self._ops.describe_table(request)
    
    # ========================================================================
    # Maintenance Operations
    # ========================================================================
    
    def compact(
        self,
        table: str,
        force: bool = False,
        target_file_size_mb: Optional[int] = None,
        expire_snapshots: bool = True,
        snapshot_retention_hours: int = 168,  # 7 days
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> CompactResponse:
        """
        Compact table to merge small files
        
        Args:
            table: Table name
            force: Force compaction even if thresholds not met
            target_file_size_mb: Target file size in MB
            expire_snapshots: Expire old snapshots after compaction
            snapshot_retention_hours: Hours to retain snapshots
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            CompactResponse with compaction results
            
        Example:
            ```python
            response = db.compact(
                "users",
                force=False,
                target_file_size_mb=128,
                expire_snapshots=True
            )
            
            if response.data.compacted:
                stats = response.data.stats
                print(f"Files: {stats.files_before} → {stats.files_after}")
                print(f"Saved: {stats.bytes_saved / 1024**2:.2f} MB")
            ```
        """
        request = CompactRequest(
            operation="COMPACT",
            tenant_id=tenant_id or self.tenant_id,
            namespace=namespace or self.namespace,
            table=table,
            force=force,
            target_file_size_mb=target_file_size_mb,
            expire_snapshots=expire_snapshots,
            snapshot_retention_hours=snapshot_retention_hours,
        )
        
        return self._ops.compact(request)
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def table_exists(
        self,
        table: str,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> bool:
        """
        Check if table exists
        
        Args:
            table: Table name
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            response = self.list_tables(tenant_id=tenant_id, namespace=namespace)
            return table in response.data.tables
        except Exception:
            return False
    
    def get_row_count(
        self,
        table: str,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> Optional[int]:
        """
        Get approximate row count for table
        
        Args:
            table: Table name
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            Row count or None if unavailable
        """
        try:
            response = self.describe_table(table, tenant_id=tenant_id, namespace=namespace)
            return response.data.table.row_count
        except Exception:
            return None
    
    # ========================================================================
    # SQL Query Support (Alternative to Structured API)
    # ========================================================================
    
    def execute_sql(
        self,
        sql: str,
        tenant_id: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute raw SQL query on Iceberg tables
        
        Note: This uses DuckDB's iceberg_scan() function under the hood.
        For complex queries or joins across sources, use FederatedQueryEngine instead.
        
        Args:
            sql: SQL query string
            tenant_id: Override tenant_id
            namespace: Override namespace
            
        Returns:
            List of result records
            
        Example:
            ```python
            results = db.execute_sql(\"\"\"
                SELECT status, COUNT(*) as total, AVG(age) as avg_age
                FROM users
                WHERE age >= 18
                GROUP BY status
                ORDER BY total DESC
            \"\"\")
            ```
        """
        import duckdb
        
        # Initialize DuckDB connection
        conn = duckdb.connect(':memory:')
        
        # Load required extensions
        try:
            conn.execute("INSTALL iceberg;")
            conn.execute("LOAD iceberg;")
            conn.execute("INSTALL httpfs;")
            conn.execute("LOAD httpfs;")
        except Exception as e:
            logger.warning(f"Extension loading failed: {e}")
        
        # Configure S3 access
        s3_config = self._config.s3
        conn.execute(f"SET s3_endpoint='{s3_config.get('endpoint', '')}';")
        conn.execute(f"SET s3_region='{s3_config.get('region', 'us-east-1')}';")
        
        if 'access_key_id' in s3_config:
            conn.execute(f"SET s3_access_key_id='{s3_config['access_key_id']}';")
            conn.execute(f"SET s3_secret_access_key='{s3_config['secret_access_key']}';")
        
        # Execute query
        try:
            result = conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            
            # Convert to list of dicts
            return [
                {col: val for col, val in zip(columns, row)}
                for row in rows
            ]
        finally:
            conn.close()
    
    def __repr__(self) -> str:
        return f"IbexDB(tenant_id='{self.tenant_id}', namespace='{self.namespace}')"

