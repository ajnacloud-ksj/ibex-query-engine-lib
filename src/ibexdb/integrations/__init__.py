"""
IbexDB Integrations

Integration modules for various frameworks and platforms.
"""

# Import integrations when dependencies are available
try:
    from ibexdb.integrations.ajna_backend import IbexDBDataSource, create_ibexdb_datasource
    __all__ = ["IbexDBDataSource", "create_ibexdb_datasource"]
except ImportError:
    __all__ = []

