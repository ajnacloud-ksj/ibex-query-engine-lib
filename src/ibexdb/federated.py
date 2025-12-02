"""
Federated Query Engine for IbexDB

Enables querying across multiple data sources (Iceberg, PostgreSQL, MySQL, etc.)
with support for both structured requests (type-safe) and SQL queries.

Key Features:
- Multi-source queries (Iceberg + PostgreSQL + MySQL + S3)
- Structured API (no SQL knowledge needed)
- SQL API (for advanced users)
- Automatic query optimization
- Type-safe with Pydantic validation
"""

import logging
from typing import Any, Dict, List, Optional

import duckdb
import polars as pl

from ibexdb import IbexDB
from ibexdb.config_manager import DataSourceConfigManager
from ibexdb.models import (
    ErrorDetail,
    QueryMetadata,
    QueryRequest,
    QueryResponse,
    QueryResponseData,
    ResponseMetadata,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Data Source Configuration Models
# ============================================================================


class DataSourceConfig:
    """Configuration for a data source"""

    def __init__(self, source_id: str, source_type: str, config: Dict[str, Any]):
        self.source_id = source_id
        self.source_type = source_type
        self.config = config
        self.connection = None  # Initialized on connect


# ============================================================================
# Federated Query Engine
# ============================================================================


class FederatedQueryEngine:
    """
    Federated query engine that can query across multiple data sources

    Supports:
    - IbexDB (Iceberg tables on S3)
    - PostgreSQL
    - MySQL
    - S3 Parquet files
    - And more via DuckDB extensions

    Example:
        ```python
        from ibexdb import FederatedQueryEngine

        # Initialize engine
        fed = FederatedQueryEngine()

        # Add data sources
        fed.add_source("ibexdb", type="ibexdb", config={
            "tenant_id": "company",
            "namespace": "production"
        })

        fed.add_source("postgres", type="postgres", config={
            "host": "localhost",
            "port": 5432,
            "database": "mydb",
            "user": "user",
            "password": "pass"
        })

        # Structured query (no SQL!)
        result = fed.query(
            sources={
                "users": {"source": "postgres", "table": "users"},
                "events": {"source": "ibexdb", "table": "events"}
            },
            join={
                "type": "inner",
                "left": "users",
                "right": "events",
                "on": {"users.id": "events.user_id"}
            }
        )

        # Or use SQL for complex queries
        result = fed.execute_sql(\"\"\"
            SELECT u.*, COUNT(e.id) as event_count
            FROM postgres.users u
            LEFT JOIN ibexdb.events e ON u.id = e.user_id
            GROUP BY u.id
        \"\"\")
        ```
    """

    def __init__(
        self,
        duckdb_path: str = ':memory:',
        config_manager: Optional[DataSourceConfigManager] = None
    ):
        """
        Initialize federated query engine

        Args:
            duckdb_path: Path to DuckDB database (default: in-memory)
            config_manager: Config manager for automatic source loading
            
        Note:
            For multi-tenant operations, tenant_id and namespace are passed
            per-request, not at initialization.
        """
        self.sources: Dict[str, DataSourceConfig] = {}
        self.conn = duckdb.connect(duckdb_path)
        self.config_manager = config_manager
        
        # Initialize IbexDB operations engine (tenant_id will be per-request)
        from ibexdb.operations import FullIcebergOperations as DatabaseOperations
        self._db_ops = DatabaseOperations()
        
        self._setup_duckdb()

        # Load sources from config manager if provided
        if self.config_manager:
            self._load_sources_from_config()

        logger.info("✅ Federated Query Engine initialized")

    def _setup_duckdb(self):
        """Setup DuckDB with required extensions"""
        extensions = ["httpfs", "postgres_scanner", "mysql_scanner", "iceberg"]

        for ext in extensions:
            try:
                self.conn.execute(f"INSTALL {ext};")
                self.conn.execute(f"LOAD {ext};")
                logger.debug(f"✅ Loaded extension: {ext}")
            except Exception as e:
                logger.warning(f"⚠️  Failed to load {ext}: {e}")

    def _load_sources_from_config(self):
        """Load data sources from config manager"""
        logger.info("📡 Loading data sources from Config Manager")

        sources = self.config_manager.get_all_sources(enabled_only=True)

        for source_id, source_config in sources.items():
            try:
                self.add_source(
                    source_id=source_id,
                    source_type=source_config["type"],
                    config=source_config["config"],
                )
            except Exception as e:
                logger.error(f"❌ Failed to add source {source_id}: {e}")

        logger.info(f"✅ Loaded {len(self.sources)} sources from Config Manager")

    # ========================================================================
    # Data Source Management
    # ========================================================================

    def add_source(
        self, source_id: str, source_type: str, config: Dict[str, Any]
    ) -> "FederatedQueryEngine":
        """
        Add a data source to the federated engine

        Args:
            source_id: Unique identifier for this source
            source_type: Type of source (ibexdb, postgres, mysql, s3)
            config: Source configuration (can be full source object or just connection config)

        Returns:
            Self for method chaining

        Supported Types:
            - ibexdb: IbexDB (Iceberg) tables
            - postgres: PostgreSQL database
            - mysql: MySQL database
            - s3: S3 Parquet files

        Example:
            ```python
            fed.add_source("prod_db", "ibexdb", {
                "tenant_id": "company",
                "namespace": "production"
            })
            ```
        """
        # Handle both full source object and just connection config
        # If config has a "config" key, it's the full source object
        if "config" in config:
            # Full source object - extract connection config
            connection_config = config["config"]
            # Store the full config for database name resolution
            source_config = DataSourceConfig(source_id, source_type, config)
        else:
            # Just connection config - use as-is
            connection_config = config
            source_config = DataSourceConfig(source_id, source_type, {"config": config})
        
        self.sources[source_id] = source_config

        # Connect based on type (use connection_config for actual connection)
        if source_type == "ibexdb":
            self._connect_ibexdb_with_config(source_config, connection_config)
        elif source_type == "postgres":
            self._connect_postgres_with_config(source_config, connection_config)
        elif source_type == "mysql":
            self._connect_mysql_with_config(source_config, connection_config)
        elif source_type == "s3":
            self._configure_s3(source_config)
        else:
            logger.warning(f"⚠️  Unknown source type: {source_type}")

        logger.info(f"✅ Added source: {source_id} ({source_type})")
        return self
    
    def _connect_postgres_with_config(self, source_config: DataSourceConfig, connection_config: Dict[str, Any]):
        """Connect to PostgreSQL source with explicit connection config"""
        schema_name = source_config.source_id.replace("-", "_")

        attach_sql = f"""
        ATTACH 'host={connection_config["host"]} port={connection_config.get("port", 5432)}
                dbname={connection_config["database"]} user={connection_config["user"]}
                password={connection_config["password"]}'
        AS {schema_name} (TYPE POSTGRES);
        """

        try:
            self.conn.execute(attach_sql)
            logger.info(f"✅ Connected PostgreSQL: {source_config.source_id}")
        except Exception as e:
            logger.error(f"❌ Failed to connect PostgreSQL: {e}")
            raise
    
    def _connect_mysql_with_config(self, source_config: DataSourceConfig, connection_config: Dict[str, Any]):
        """Connect to MySQL source with explicit connection config"""
        schema_name = source_config.source_id.replace("-", "_")

        attach_sql = f"""
        ATTACH 'host={connection_config["host"]} port={connection_config.get("port", 3306)}
                database={connection_config["database"]} user={connection_config["user"]}
                password={connection_config["password"]}'
        AS {schema_name} (TYPE MYSQL);
        """

        try:
            self.conn.execute(attach_sql)
            logger.info(f"✅ Connected MySQL: {source_config.source_id}")
        except Exception as e:
            logger.error(f"❌ Failed to connect MySQL: {e}")
            raise
    
    def _connect_ibexdb_with_config(self, source_config: DataSourceConfig, connection_config: Dict[str, Any]):
        """Connect to IbexDB source with explicit connection config"""
        # For IbexDB, we don't need to do anything special
        # The connection is handled by the DatabaseOperations instance
        pass

    def _connect_ibexdb(self, source_config: DataSourceConfig):
        """Connect to IbexDB source"""
        config = source_config.config

        # Initialize IbexDB client
        client = IbexDB(
            tenant_id=config.get("tenant_id", "default"),
            namespace=config.get("namespace", "default"),
        )

        source_config.connection = client

        # Note: IbexDB queries are handled through client API,
        # not direct DuckDB attachment

    def _connect_postgres(self, source_config: DataSourceConfig):
        """Connect to PostgreSQL source"""
        config = source_config.config

        schema_name = source_config.source_id.replace("-", "_")

        attach_sql = f"""
        ATTACH 'host={config["host"]} port={config.get("port", 5432)}
                dbname={config["database"]} user={config["user"]}
                password={config["password"]}'
        AS {schema_name} (TYPE POSTGRES);
        """

        try:
            self.conn.execute(attach_sql)
            logger.info(f"✅ Connected PostgreSQL: {source_config.source_id}")
        except Exception as e:
            logger.error(f"❌ Failed to connect PostgreSQL: {e}")
            raise

    def _connect_mysql(self, source_config: DataSourceConfig):
        """Connect to MySQL source"""
        config = source_config.config

        schema_name = source_config.source_id.replace("-", "_")

        attach_sql = f"""
        ATTACH 'host={config["host"]} port={config.get("port", 3306)}
                database={config["database"]} user={config["user"]}
                password={config["password"]}'
        AS {schema_name} (TYPE MYSQL);
        """

        try:
            self.conn.execute(attach_sql)
            logger.info(f"✅ Connected MySQL: {source_config.source_id}")
        except Exception as e:
            logger.error(f"❌ Failed to connect MySQL: {e}")
            raise

    def _configure_s3(self, source_config: DataSourceConfig):
        """Configure S3 access"""
        config = source_config.config

        try:
            self.conn.execute(f"SET s3_endpoint='{config['endpoint']}';")
            self.conn.execute(f"SET s3_access_key_id='{config['access_key']}';")
            self.conn.execute(f"SET s3_secret_access_key='{config['secret_key']}';")
            self.conn.execute("SET s3_url_style='path';")
            self.conn.execute(
                f"SET s3_use_ssl={'true' if config.get('use_ssl', False) else 'false'};"
            )
            logger.info(f"✅ Configured S3: {source_config.source_id}")
        except Exception as e:
            logger.error(f"❌ Failed to configure S3: {e}")
            raise

    # ========================================================================
    # QueryRequest/QueryResponse API (Type-Safe)
    # ========================================================================

    def execute_query_request(self, request: QueryRequest) -> QueryResponse:
        """
        Execute a QueryRequest (type-safe structured query)

        This uses the same QueryRequest/QueryResponse models as ibex-db-lambda
        for consistency across the platform.

        Args:
            request: QueryRequest with filters, aggregations, etc.

        Returns:
            QueryResponse with standardized structure

        Example:
            ```python
            from ibexdb.models import QueryRequest, Filter

            request = QueryRequest(
                operation="QUERY",
                tenant_id="company",
                namespace="production",
                table="users",
                filters=[
                    Filter(field="age", operator="gte", value=18)
                ],
                limit=100
            )

            response = fed.execute_query_request(request)

            if response.success:
                for record in response.data.records:
                    print(record)
            ```
        """
        import time

        start_time = time.time()
        request_id = f"fed_{int(start_time * 1000)}"

        try:
            # Check if we have an IbexDB source for this request
            source_id = f"{request.tenant_id}_{request.namespace}"

            # If source exists, use it; otherwise try to query directly
            if source_id in self.sources:
                source_config = self.sources[source_id]
                if source_config.source_type == "ibexdb" and source_config.connection:
                    client: IbexDB = source_config.connection
                    return client.query(
                        table=request.table,
                        projection=request.projection,
                        filters=[f.model_dump() for f in (request.filters or [])],
                        aggregations=[a.model_dump() for a in (request.aggregations or [])],
                        group_by=request.group_by,
                        sort=[s.model_dump() for s in (request.sort or [])],
                        limit=request.limit,
                        offset=request.offset,
                        distinct=request.distinct,
                        as_of=request.as_of,
                    )

            # Otherwise build and execute SQL
            sql = self._build_sql_from_request(request)
            df = self.execute_sql(sql)

            execution_time_ms = (time.time() - start_time) * 1000

            return QueryResponse(
                success=True,
                data=QueryResponseData(
                    records=df.to_dicts(),
                    query_metadata=QueryMetadata(
                        row_count=len(df),
                        execution_time_ms=execution_time_ms,
                        scanned_rows=len(df),
                        cache_hit=False,
                    ),
                ),
                metadata=ResponseMetadata(
                    request_id=request_id, execution_time_ms=execution_time_ms
                ),
            )

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000

            return QueryResponse(
                success=False,
                metadata=ResponseMetadata(
                    request_id=request_id, execution_time_ms=execution_time_ms
                ),
                error=ErrorDetail(code="QUERY_ERROR", message=str(e)),
            )

    def _build_sql_from_request(self, request: QueryRequest) -> str:
        """Build SQL from QueryRequest"""
        # SELECT clause
        if request.aggregations:
            agg_exprs = []
            for agg in request.aggregations:
                field = agg.field
                func = agg.function.upper()
                alias = agg.alias

                if field:
                    expr = f"{func}({field})"
                else:
                    expr = f"{func}(*)"

                expr = f"{expr} AS {alias}"
                agg_exprs.append(expr)

            select_clause = ", ".join(agg_exprs)
        elif request.projection:
            select_clause = ", ".join(request.projection)
        else:
            select_clause = "*"

        # FROM clause
        from_clause = request.table

        # WHERE clause
        where_clauses = []
        if request.filters:
            for f in request.filters:
                field = f.field
                operator = f.operator
                value = f.value

                # Convert operator to SQL
                op_map = {
                    "eq": "=",
                    "ne": "!=",
                    "gt": ">",
                    "gte": ">=",
                    "lt": "<",
                    "lte": "<=",
                    "like": "LIKE",
                    "in": "IN",
                }
                sql_op = op_map.get(operator, "=")

                # Format value
                if isinstance(value, str):
                    value_str = f"'{value}'"
                elif isinstance(value, list):
                    value_str = f"({', '.join(repr(v) for v in value)})"
                else:
                    value_str = str(value)

                where_clauses.append(f"{field} {sql_op} {value_str}")

        # Build SQL
        sql_parts = [f"SELECT {select_clause}", f"FROM {from_clause}"]

        if where_clauses:
            sql_parts.append(f"WHERE {' AND '.join(where_clauses)}")

        if request.group_by:
            sql_parts.append(f"GROUP BY {', '.join(request.group_by)}")

        if request.sort:
            sort_exprs = []
            for s in request.sort:
                sort_exprs.append(f"{s.field} {s.order.upper()}")
            sql_parts.append(f"ORDER BY {', '.join(sort_exprs)}")

        if request.limit:
            sql_parts.append(f"LIMIT {request.limit}")

        if request.offset:
            sql_parts.append(f"OFFSET {request.offset}")

        return "\n".join(sql_parts)

    # ========================================================================
    # Structured Query API (No SQL Required)
    # ========================================================================

    def query(
        self,
        sources: Dict[str, Dict[str, str]],
        projection: Optional[List[str]] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
        join: Optional[Dict[str, Any]] = None,
        aggregations: Optional[List[Dict[str, Any]]] = None,
        group_by: Optional[List[str]] = None,
        sort: Optional[List[Dict[str, Any]]] = None,
        limit: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Execute a structured federated query (no SQL knowledge needed!)

        Args:
            sources: Source definitions
                {"alias": {"source": "source_id", "table": "table_name"}}
            projection: Fields to select (default: all)
            filters: Filter conditions per source
            join: Join configuration
            aggregations: Aggregation functions
            group_by: Group by fields
            sort: Sort order
            limit: Row limit

        Returns:
            Polars DataFrame with results

        Example:
            ```python
            result = fed.query(
                sources={
                    "u": {"source": "postgres", "table": "users"},
                    "e": {"source": "ibexdb", "table": "events"}
                },
                projection=["u.name", "COUNT(e.id) as event_count"],
                join={
                    "type": "left",
                    "left": "u",
                    "right": "e",
                    "on": {"u.id": "e.user_id"}
                },
                filters=[
                    {"source": "u", "field": "status", "operator": "eq", "value": "active"}
                ],
                group_by=["u.name"],
                sort=[{"field": "event_count", "order": "desc"}],
                limit=100
            )
            ```
        """
        # Build SQL from structured query
        sql = self._build_federated_sql(
            sources=sources,
            projection=projection,
            filters=filters,
            join=join,
            aggregations=aggregations,
            group_by=group_by,
            sort=sort,
            limit=limit,
        )

        logger.info(f"🔍 Generated SQL:\n{sql}")

        # Execute
        return self.execute_sql(sql)

    def _build_federated_sql(
        self,
        sources: Dict[str, Dict[str, str]],
        projection: Optional[List[str]],
        filters: Optional[List[Dict[str, Any]]],
        join: Optional[Dict[str, Any]],
        aggregations: Optional[List[Dict[str, Any]]],
        group_by: Optional[List[str]],
        sort: Optional[List[Dict[str, Any]]],
        limit: Optional[int],
    ) -> str:
        """Build SQL query from structured parameters"""

        # SELECT clause
        if projection:
            select_clause = ", ".join(projection)
        elif aggregations:
            agg_exprs = []
            for agg in aggregations:
                field = agg.get("field")
                func = agg.get("function").upper()
                alias = agg.get("alias")

                if field:
                    expr = f"{func}({field})"
                else:
                    expr = f"{func}(*)"

                if alias:
                    expr = f"{expr} AS {alias}"

                agg_exprs.append(expr)

            select_clause = ", ".join(agg_exprs)
        else:
            select_clause = "*"

        # FROM clause
        main_alias = list(sources.keys())[0]
        main_source = sources[main_alias]
        from_clause = f"{self._get_table_ref(main_source)} AS {main_alias}"

        # JOIN clauses
        join_clauses = []
        if join and len(sources) > 1:
            for alias, source_def in list(sources.items())[1:]:
                join_type = join.get("type", "inner").upper()
                table_ref = self._get_table_ref(source_def)

                # Build ON condition
                on_conditions = []
                on_dict = join.get("on", {})
                for left_col, right_col in on_dict.items():
                    on_conditions.append(f"{left_col} = {right_col}")

                on_clause = " AND ".join(on_conditions) if on_conditions else "1=1"

                join_clauses.append(f"{join_type} JOIN {table_ref} AS {alias} ON {on_clause}")

        # WHERE clause
        where_clauses = []
        if filters:
            for f in filters:
                source_alias = f.get("source")
                field = f.get("field")
                operator = f.get("operator")
                value = f.get("value")

                # Convert operator to SQL
                op_map = {
                    "eq": "=",
                    "ne": "!=",
                    "gt": ">",
                    "gte": ">=",
                    "lt": "<",
                    "lte": "<=",
                    "like": "LIKE",
                }
                sql_op = op_map.get(operator, "=")

                # Format value
                if isinstance(value, str):
                    value_str = f"'{value}'"
                else:
                    value_str = str(value)

                where_clauses.append(f"{source_alias}.{field} {sql_op} {value_str}")

        # GROUP BY clause
        group_by_clause = ""
        if group_by:
            group_by_clause = f"GROUP BY {', '.join(group_by)}"

        # ORDER BY clause
        order_by_clause = ""
        if sort:
            sort_exprs = []
            for s in sort:
                field = s.get("field")
                order = s.get("order", "asc").upper()
                sort_exprs.append(f"{field} {order}")
            order_by_clause = f"ORDER BY {', '.join(sort_exprs)}"

        # LIMIT clause
        limit_clause = f"LIMIT {limit}" if limit else ""

        # Assemble SQL
        sql_parts = [f"SELECT {select_clause}", f"FROM {from_clause}"]

        if join_clauses:
            sql_parts.extend(join_clauses)

        if where_clauses:
            sql_parts.append(f"WHERE {' AND '.join(where_clauses)}")

        if group_by_clause:
            sql_parts.append(group_by_clause)

        if order_by_clause:
            sql_parts.append(order_by_clause)

        if limit_clause:
            sql_parts.append(limit_clause)

        return "\n".join(sql_parts)

    def _get_table_ref(self, source_def: Dict[str, str]) -> str:
        """Get table reference for SQL query"""
        source_id = source_def["source"]
        table = source_def["table"]

        source_config = self.sources.get(source_id)
        if not source_config:
            raise ValueError(f"Source not found: {source_id}")

        if source_config.source_type == "ibexdb":
            # For IbexDB, we need to query through client and create temp table
            # Or use iceberg_scan if configured
            # For now, return schema.table format
            schema_name = source_id.replace("-", "_")
            return f"{schema_name}.{table}"
        else:
            # For attached databases (postgres, mysql)
            schema_name = source_id.replace("-", "_")
            return f"{schema_name}.{table}"

    # ========================================================================
    # SQL Query API (For Advanced Users)
    # ========================================================================

    def execute_sql(self, sql: str) -> pl.DataFrame:
        """
        Execute raw SQL query across federated sources

        Args:
            sql: SQL query string

        Returns:
            Polars DataFrame with results

        Example:
            ```python
            result = fed.execute_sql(\"\"\"
                SELECT
                    u.name,
                    COUNT(e.id) as event_count,
                    SUM(o.total) as revenue
                FROM postgres.users u
                LEFT JOIN ibexdb.events e ON u.id = e.user_id
                LEFT JOIN mysql.orders o ON u.id = o.user_id
                WHERE u.status = 'active'
                GROUP BY u.name
                ORDER BY revenue DESC
                LIMIT 100
            \"\"\")
            ```
        """
        try:
            result = self.conn.execute(sql)
            df = result.pl()
            logger.info(f"✅ Query executed: {len(df)} rows returned")
            return df
        except Exception as e:
            logger.error(f"❌ Query failed: {e}")
            logger.error(f"SQL: {sql}")
            raise

    def execute_query_dict(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute SQL and return results as list of dictionaries

        Args:
            sql: SQL query string

        Returns:
            List of dictionaries (one per row)
        """
        df = self.execute_sql(sql)
        return df.to_dicts()

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def list_sources(self) -> List[Dict[str, str]]:
        """List all configured data sources"""
        return [
            {
                "id": source_id,
                "type": config.source_type,
                "connected": config.connection is not None,
            }
            for source_id, config in self.sources.items()
        ]

    def get_tables(self, source_id: str) -> List[str]:
        """List tables in a specific source"""
        source_config = self.sources.get(source_id)
        if not source_config:
            raise ValueError(f"Source not found: {source_id}")

        if source_config.source_type == "ibexdb":
            client: IbexDB = source_config.connection
            response = client.list_tables()
            return response.data.tables if response.success else []

        elif source_config.source_type in ["postgres", "mysql"]:
            schema_name = source_id.replace("-", "_")
            result = self.conn.execute(f"SHOW TABLES FROM {schema_name}")
            return [row[0] for row in result.fetchall()]

        else:
            return []

    def close(self):
        """Close all connections"""
        self.conn.close()
        logger.info("✅ Federated Query Engine closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ========================================================================
    # CRUD Operations (Delegated to DatabaseOperations)
    # ========================================================================

    def create_table(self, request):
        """Delegate create_table to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.create_table(request)

    def write(self, request):
        """Delegate write to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.write(request)

    def update(self, request):
        """Delegate update to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.update(request)

    def delete(self, request):
        """Delegate delete to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.delete(request)

    def hard_delete(self, request):
        """Delegate hard_delete to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.hard_delete(request)

    def compact(self, request):
        """Delegate compact to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.compact(request)

    def list_tables(self, request):
        """Delegate list_tables to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.list_tables(request)

    def describe_table(self, request):
        """Delegate describe_table to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.describe_table(request)
    
    def query_request(self, request: QueryRequest) -> QueryResponse:
        """
        Execute QueryRequest across all sources (Iceberg, Postgres, MySQL)
        
        This is the unified query method that works for both Iceberg and external sources.
        It resolves logical database names and builds SQL from the QueryRequest.
        
        Args:
            request: QueryRequest with table, filters, projections, etc.
        
        Returns:
            QueryResponse with results
        
        Examples:
            # Iceberg query
            query_request(QueryRequest(table="users", filters=[...]))
            
            # Postgres query (logical name)
            query_request(QueryRequest(table="userdb.users", filters=[...]))
            
            # MySQL query (logical name)
            query_request(QueryRequest(table="orderdb.orders", aggregations=[...]))
        """
        # 1. Resolve logical database name to DuckDB schema name
        resolved_table = self._resolve_table_name(request.table)
        
        # 2. Check if this is an Iceberg table (no dot in resolved name)
        if "." not in resolved_table:
            # Iceberg table - delegate to DatabaseOperations
            return self._db_ops.query(request)
        
        # 3. External source - build SQL and execute via DuckDB
        sql = self._build_sql_from_query_request(request, resolved_table)
        
        logger.info(f"🔍 Executing federated query: {sql}")
        
        try:
            result = self.conn.execute(sql)
            records = [dict(zip([col[0] for col in result.description], row)) 
                      for row in result.fetchall()]
            
            return QueryResponse(
                success=True,
                data=QueryResponseData(
                    records=records,
                    query_metadata=QueryMetadata(
                        row_count=len(records),
                        execution_time_ms=0.0,  # TODO: Track actual execution time
                        query_id=sql  # Include generated SQL for debugging
                    )
                ),
                metadata=ResponseMetadata(request_id="federated-query", execution_time_ms=0.0)
            )
        except Exception as e:
            logger.error(f"❌ Query failed: {e}")
            logger.error(f"SQL: {sql}")
            return QueryResponse(
                success=False,
                error=ErrorDetail(
                    code="QUERY_ERROR",
                    message=str(e)
                ),
                metadata=ResponseMetadata(request_id="federated-query", execution_time_ms=0.0)  # Add required fields
            )
    
    def _resolve_table_name(self, table: str) -> str:
        """
        Resolve logical database name to DuckDB schema name
        
        Examples:
            "users" → "users" (Iceberg, no change)
            "userdb.users" → "postgres.users" (resolved)
            "orderdb.orders" → "mysql.orders" (resolved)
            "postgres.users" → "postgres.users" (already resolved)
        """
        if "." not in table:
            return table  # Iceberg table, no resolution needed
        
        db_name, table_name = table.split(".", 1)
        
        # Look up logical database name → DuckDB schema name
        for source in self.sources.values():
            logical_name = source.config.get("database")
            if logical_name and logical_name == db_name:
                resolved = f"{source.source_id}.{table_name}"
                logger.info(f"📍 Resolved: {table} → {resolved}")
                return resolved
        
        # Not found - assume it's already a DuckDB schema name (e.g., "postgres.users")
        logger.info(f"📍 Using as-is: {table}")
        return table
    
    def _build_sql_from_query_request(self, request: QueryRequest, table: str) -> str:
        """
        Convert QueryRequest to SQL
        
        Args:
            request: QueryRequest object
            table: Resolved table name (e.g., "postgres.users")
        
        Returns:
            SQL query string
        """
        # SELECT clause
        if request.aggregations:
            fields = []
            for agg in request.aggregations:
                func = agg.function.upper()
                field = agg.field if agg.field != "*" else "*"
                alias = agg.alias or f"{func.lower()}_{agg.field}"
                fields.append(f"{func}({field}) as {alias}")
        else:
            fields = request.projection if request.projection else ["*"]
        
        sql = f"SELECT {', '.join(fields)} FROM {table}"
        
        # Table alias
        if request.alias:
            sql += f" AS {request.alias}"
        
        # JOIN clauses (for federated cross-database joins)
        if request.join:
            for join_clause in request.join:
                # Resolve the join table name (might be logical name like "orderdb.orders")
                join_table = self._resolve_table_name(join_clause.table)
                
                # Join type
                join_type = join_clause.type.value.upper() if hasattr(join_clause.type, 'value') else join_clause.type.upper()
                sql += f" {join_type} JOIN {join_table}"
                
                # Join table alias
                if join_clause.alias:
                    sql += f" AS {join_clause.alias}"
                
                # ON conditions
                if join_clause.on:
                    on_conditions = []
                    for condition in join_clause.on:
                        op_map = {"eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
                        operator = op_map.get(condition.operator, "=")
                        on_conditions.append(f"{condition.left_field} {operator} {condition.right_field}")
                    sql += f" ON {' AND '.join(on_conditions)}"
        
        # WHERE clause
        if request.filters:
            conditions = []
            for f in request.filters:
                # Handle string vs numeric values
                if isinstance(f.value, str):
                    value = f"'{f.value}'"
                elif isinstance(f.value, list):
                    value = f"({', '.join([repr(v) for v in f.value])})"
                else:
                    value = str(f.value)
                
                # Map operators
                op_map = {
                    "eq": "=",
                    "ne": "!=",
                    "gt": ">",
                    "gte": ">=",
                    "lt": "<",
                    "lte": "<=",
                    "in": "IN",
                    "like": "LIKE"
                }
                operator = op_map.get(f.operator, f.operator)
                conditions.append(f"{f.field} {operator} {value}")
            
            sql += f" WHERE {' AND '.join(conditions)}"
        
        # GROUP BY clause
        if request.group_by:
            sql += f" GROUP BY {', '.join(request.group_by)}"
        
        # HAVING clause
        if request.having:
            having_conditions = []
            for h in request.having:
                value = f"'{h.value}'" if isinstance(h.value, str) else str(h.value)
                op_map = {"eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
                operator = op_map.get(h.operator, h.operator)
                having_conditions.append(f"{h.field} {operator} {value}")
            sql += f" HAVING {' AND '.join(having_conditions)}"
        
        # ORDER BY clause
        if request.sort:
            order_by = [f"{s.field} {s.order.upper() if hasattr(s.order, 'upper') else s.order.value.upper()}" for s in request.sort]
            sql += f" ORDER BY {', '.join(order_by)}"
        
        # DISTINCT
        if request.distinct:
            sql = sql.replace("SELECT ", "SELECT DISTINCT ", 1)
        
        # LIMIT and OFFSET
        if request.limit:
            sql += f" LIMIT {request.limit}"
        if request.offset:
            sql += f" OFFSET {request.offset}"
        
        return sql


    def query(self, request):
        """Delegate query to DatabaseOperations (supports per-request tenant_id)"""
        return self._db_ops.query(request)




# ============================================================================
# Convenience Functions
# ============================================================================


def create_federated_engine(
    sources: Optional[Dict[str, Dict[str, Any]]] = None,
    config_endpoint: Optional[str] = None,
    config_file: Optional[str] = None,
    api_key: Optional[str] = None,
) -> FederatedQueryEngine:
    """
    Create a federated query engine with sources

    Args:
        sources: Dictionary of source configurations (manual)
        config_endpoint: Config Manager endpoint URL (automatic)
        config_file: Configuration file path (automatic)
        api_key: API key for Config Manager

    Returns:
        FederatedQueryEngine instance

    Examples:
        ```python
        # Method 1: Manual sources
        fed = create_federated_engine({
            "prod_db": {
                "type": "ibexdb",
                "config": {"tenant_id": "company", "namespace": "prod"}
            }
        })

        # Method 2: From Config Manager endpoint (like ajna-db-backend)
        fed = create_federated_engine(
            config_endpoint="http://config-manager:8080",
            api_key="my-api-key"
        )

        # Method 3: From configuration file
        fed = create_federated_engine(
            config_file="config/datasources.json"
        )
        ```
    """
    # Initialize config manager if endpoint or file provided
    config_manager = None

    if config_endpoint:
        config_manager = DataSourceConfigManager.from_endpoint(url=config_endpoint, api_key=api_key)
    elif config_file:
        config_manager = DataSourceConfigManager.from_file(config_file)

    # Create engine with config manager
    engine = FederatedQueryEngine(config_manager=config_manager)

    # Add manual sources if provided
    if sources:
        for source_id, source_def in sources.items():
            engine.add_source(source_id, source_def["type"], source_def["config"])

    return engine
