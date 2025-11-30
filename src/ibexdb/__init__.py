"""
IbexDB - Serverless ACID Database on S3 with Apache Iceberg

A Python library for building production-ready data applications on S3
with full ACID guarantees, time-travel, and schema evolution.
"""

__version__ = "0.1.0"
__author__ = "Ajna Team"
__license__ = "MIT"

# Core client
from ibexdb.client import IbexDB

# Federated Query Engine
from ibexdb.federated import FederatedQueryEngine, create_federated_engine

# Configuration
from ibexdb.config import Config as IbexConfig, get_config
from ibexdb.config_manager import DataSourceConfigManager

# Models - Request Models
from ibexdb.models import (
    # Core operation requests
    QueryRequest,
    WriteRequest,
    UpdateRequest,
    DeleteRequest,
    HardDeleteRequest,
    CompactRequest,
    CreateTableRequest,
    ListTablesRequest,
    DescribeTableRequest,
    
    # Response models
    QueryResponse,
    WriteResponse,
    UpdateResponse,
    DeleteResponse,
    HardDeleteResponse,
    CompactResponse,
    CreateTableResponse,
    ListTablesResponse,
    DescribeTableResponse,
    
    # Supporting models
    Filter,
    SortField,
    AggregateField,
    ProjectionField,
    SchemaDefinition,
    FieldDefinition,
    PartitionConfig,
    TableProperties,
    
    # Enums
    OperationType,
    SortOrder,
    WriteMode,
    DeleteMode,
    FieldType,
)

# Operations (advanced usage)
from ibexdb.operations import FullIcebergOperations as DatabaseOperations

# Query Builder (advanced usage)
from ibexdb.query_builder import TypeSafeQueryBuilder

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

