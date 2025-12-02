"""
IbexDB - Serverless ACID Database on S3 with Apache Iceberg

A Python library for building production-ready data applications on S3
with full ACID guarantees, time-travel, and schema evolution.
"""

__version__ = "0.1.0"
__author__ = "Ajna Team"
__license__ = "MIT"

# Setup logging on import
from ibexdb.logger import setup_logging

setup_logging()

# Core client
from ibexdb.client import IbexDB  # noqa: E402

# Configuration
from ibexdb.config import Config as IbexConfig  # noqa: E402
from ibexdb.config import get_config  # noqa: E402
from ibexdb.config_manager import DataSourceConfigManager  # noqa: E402

# Federated Query Engine
from ibexdb.federated import FederatedQueryEngine, create_federated_engine  # noqa: E402

# Models - Request Models
from ibexdb.models import (  # noqa: E402
    AggregateField,
    CompactRequest,
    CompactResponse,
    CreateTableRequest,
    CreateTableResponse,
    DeleteMode,
    DeleteRequest,
    DeleteResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    FieldDefinition,
    FieldType,
    # Supporting models
    Filter,
    HardDeleteRequest,
    HardDeleteResponse,
    ListTablesRequest,
    ListTablesResponse,
    # Enums
    OperationType,
    PartitionConfig,
    ProjectionField,
    # Core operation requests
    QueryRequest,
    # Response models
    QueryResponse,
    SchemaDefinition,
    SortField,
    SortOrder,
    TableProperties,
    UpdateRequest,
    UpdateResponse,
    WriteMode,
    WriteRequest,
    WriteResponse,
)

# Operations (advanced usage)
from ibexdb.operations import FullIcebergOperations as DatabaseOperations  # noqa: E402

# Query Builder (advanced usage)
from ibexdb.query_builder import TypeSafeQueryBuilder  # noqa: E402

__all__ = [
    # Core
    "IbexDB",
    "FederatedQueryEngine",
    "create_federated_engine",
    "IbexConfig",
    "get_config",
    "DataSourceConfigManager",
    "DatabaseOperations",
    "TypeSafeQueryBuilder",
    # Request Models
    "QueryRequest",
    "WriteRequest",
    "UpdateRequest",
    "DeleteRequest",
    "HardDeleteRequest",
    "CompactRequest",
    "CreateTableRequest",
    "ListTablesRequest",
    "DescribeTableRequest",
    # Response Models
    "QueryResponse",
    "WriteResponse",
    "UpdateResponse",
    "DeleteResponse",
    "HardDeleteResponse",
    "CompactResponse",
    "CreateTableResponse",
    "ListTablesResponse",
    "DescribeTableResponse",
    # Supporting Models
    "Filter",
    "SortField",
    "AggregateField",
    "ProjectionField",
    "SchemaDefinition",
    "FieldDefinition",
    "PartitionConfig",
    "TableProperties",
    # Enums
    "OperationType",
    "SortOrder",
    "WriteMode",
    "DeleteMode",
    "FieldType",
]
