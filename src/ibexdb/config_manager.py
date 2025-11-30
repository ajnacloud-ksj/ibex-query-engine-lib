"""
Config Manager Integration for IbexDB

Fetches data source configurations from a Config Manager endpoint
(like ajna-db-backend does) or from a configuration file.

This allows dynamic data source discovery and management without
hardcoding connection details.
"""

import json
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


class DataSourceConfigManager:
    """
    Manages data source configurations from Config Manager or file
    
    Supports two modes:
    1. Config Manager endpoint (like ajna-db-backend)
    2. Configuration file (YAML or JSON)
    
    Example:
        ```python
        # From Config Manager endpoint
        config_mgr = DataSourceConfigManager.from_endpoint(
            url="http://config-manager:8080",
            api_key="my-api-key"
        )
        
        # From configuration file
        config_mgr = DataSourceConfigManager.from_file(
            "config/datasources.json"
        )
        
        # Get all data sources
        sources = config_mgr.get_all_sources()
        ```
    """
    
    def __init__(
        self,
        endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        config_file: Optional[str] = None,
        auto_refresh: bool = False,
        refresh_interval: int = 60
    ):
        """
        Initialize Config Manager
        
        Args:
            endpoint: Config Manager endpoint URL
            api_key: API key for authentication
            config_file: Path to configuration file
            auto_refresh: Enable automatic refresh
            refresh_interval: Refresh interval in seconds
        """
        self.endpoint = endpoint
        self.api_key = api_key
        self.config_file = config_file
        self.auto_refresh = auto_refresh
        self.refresh_interval = refresh_interval
        
        self._sources: Dict[str, Dict[str, Any]] = {}
        self._last_refresh: Optional[float] = None
        
        # Initial load
        self.refresh()
    
    @classmethod
    def from_endpoint(
        cls,
        url: str,
        api_key: Optional[str] = None,
        auto_refresh: bool = False,
        refresh_interval: int = 60
    ) -> 'DataSourceConfigManager':
        """
        Create config manager from endpoint
        
        Args:
            url: Config Manager endpoint URL
            api_key: API key for authentication
            auto_refresh: Enable automatic refresh
            refresh_interval: Refresh interval in seconds
            
        Returns:
            DataSourceConfigManager instance
            
        Example:
            ```python
            config_mgr = DataSourceConfigManager.from_endpoint(
                url="http://ibex-data-platform:8080",
                api_key="my-api-key"
            )
            ```
        """
        return cls(
            endpoint=url,
            api_key=api_key,
            auto_refresh=auto_refresh,
            refresh_interval=refresh_interval
        )
    
    @classmethod
    def from_file(
        cls,
        config_file: str,
        auto_refresh: bool = False,
        refresh_interval: int = 60
    ) -> 'DataSourceConfigManager':
        """
        Create config manager from file
        
        Args:
            config_file: Path to configuration file (JSON or YAML)
            auto_refresh: Enable file watching
            refresh_interval: Check file interval in seconds
            
        Returns:
            DataSourceConfigManager instance
            
        Example:
            ```python
            config_mgr = DataSourceConfigManager.from_file(
                "config/datasources.json"
            )
            ```
        """
        return cls(
            config_file=config_file,
            auto_refresh=auto_refresh,
            refresh_interval=refresh_interval
        )
    
    def refresh(self) -> int:
        """
        Refresh data source configurations
        
        Returns:
            Number of sources loaded
        """
        if self.endpoint:
            return self._fetch_from_endpoint()
        elif self.config_file:
            return self._load_from_file()
        else:
            logger.warning("No endpoint or file configured")
            return 0
    
    def _fetch_from_endpoint(self) -> int:
        """Fetch data sources from Config Manager endpoint"""
        logger.info(f"📡 Fetching data sources from: {self.endpoint}")
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                headers = {}
                if self.api_key:
                    headers['Authorization'] = f"Bearer {self.api_key}"
                    headers['X-API-Key'] = self.api_key
                
                response = httpx.get(
                    f"{self.endpoint}/api/data-sources",
                    headers=headers,
                    timeout=10.0
                )
                response.raise_for_status()
                
                data = response.json()
                
                if not data.get('success'):
                    raise Exception(f"Config Manager returned error: {data.get('error')}")
                
                # Parse data sources
                self._sources.clear()
                
                for ds in data.get('data_sources', []):
                    source_id = ds['id']
                    source_type = ds['type']
                    source_name = ds.get('name', source_id)
                    
                    # Parse connection config
                    conn_config = ds.get('connection_config', {})
                    if isinstance(conn_config, str):
                        conn_config = json.loads(conn_config)
                    
                    self._sources[source_id] = {
                        'id': source_id,
                        'name': source_name,
                        'type': source_type,
                        'config': conn_config,
                        'enabled': ds.get('enabled', True),
                        'metadata': ds.get('metadata', {})
                    }
                    
                    logger.info(f"✅ Loaded: {source_name} ({source_type})")
                
                logger.info(f"✅ Fetched {len(self._sources)} data sources")
                
                import time
                self._last_refresh = time.time()
                
                return len(self._sources)
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️  Attempt {attempt + 1}/{max_retries} failed: {e}")
                    logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                    import time
                    time.sleep(retry_delay)
                else:
                    logger.error(f"❌ Failed to fetch after {max_retries} attempts: {e}")
                    return 0
        
        return 0
    
    def _load_from_file(self) -> int:
        """Load data sources from configuration file"""
        logger.info(f"📁 Loading data sources from: {self.config_file}")
        
        try:
            config_path = Path(self.config_file)
            
            if not config_path.exists():
                logger.error(f"❌ Config file not found: {self.config_file}")
                return 0
            
            # Read file
            with open(config_path, 'r') as f:
                if config_path.suffix in ['.yaml', '.yml']:
                    import yaml
                    config_data = yaml.safe_load(f)
                else:
                    config_data = json.load(f)
            
            # Parse data sources
            self._sources.clear()
            
            data_sources = config_data.get('data_sources', [])
            
            for ds in data_sources:
                source_id = ds['id']
                source_type = ds['type']
                source_name = ds.get('name', source_id)
                
                self._sources[source_id] = {
                    'id': source_id,
                    'name': source_name,
                    'type': source_type,
                    'config': ds.get('config', {}),
                    'enabled': ds.get('enabled', True),
                    'metadata': ds.get('metadata', {})
                }
                
                logger.info(f"✅ Loaded: {source_name} ({source_type})")
            
            logger.info(f"✅ Loaded {len(self._sources)} data sources from file")
            
            import time
            self._last_refresh = time.time()
            
            return len(self._sources)
            
        except Exception as e:
            logger.error(f"❌ Failed to load config file: {e}")
            return 0
    
    def get_all_sources(self, enabled_only: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        Get all configured data sources
        
        Args:
            enabled_only: Only return enabled sources
            
        Returns:
            Dictionary of source_id -> source config
        """
        if enabled_only:
            return {
                sid: source
                for sid, source in self._sources.items()
                if source.get('enabled', True)
            }
        return self._sources.copy()
    
    def get_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """
        Get specific data source configuration
        
        Args:
            source_id: Source identifier
            
        Returns:
            Source configuration or None
        """
        return self._sources.get(source_id)
    
    def get_sources_by_type(self, source_type: str) -> Dict[str, Dict[str, Any]]:
        """
        Get all sources of a specific type
        
        Args:
            source_type: Type of source (ibexdb, postgres, mysql, etc.)
            
        Returns:
            Dictionary of matching sources
        """
        return {
            sid: source
            for sid, source in self._sources.items()
            if source['type'] == source_type and source.get('enabled', True)
        }
    
    def should_refresh(self) -> bool:
        """Check if configuration should be refreshed"""
        if not self.auto_refresh:
            return False
        
        if self._last_refresh is None:
            return True
        
        import time
        elapsed = time.time() - self._last_refresh
        return elapsed >= self.refresh_interval
    
    def start_auto_refresh(self):
        """Start background auto-refresh task"""
        if not self.auto_refresh:
            logger.warning("Auto-refresh not enabled")
            return
        
        import threading
        import time
        
        def refresh_loop():
            while True:
                try:
                    time.sleep(self.refresh_interval)
                    logger.info("🔄 Auto-refreshing data source configuration")
                    count = self.refresh()
                    logger.info(f"✅ Refreshed {count} data sources")
                except Exception as e:
                    logger.error(f"❌ Auto-refresh failed: {e}")
        
        thread = threading.Thread(target=refresh_loop, daemon=True)
        thread.start()
        logger.info(f"✅ Auto-refresh started (interval: {self.refresh_interval}s)")


# ============================================================================
# Configuration File Format
# ============================================================================

"""
Example configuration file (datasources.json):

{
  "data_sources": [
    {
      "id": "ibexdb-production",
      "name": "IbexDB Production",
      "type": "ibexdb",
      "enabled": true,
      "config": {
        "tenant_id": "my_company",
        "namespace": "production",
        "environment": "production"
      },
      "metadata": {
        "description": "Production data lake on S3",
        "owner": "data-team"
      }
    },
    {
      "id": "postgres-analytics",
      "name": "PostgreSQL Analytics",
      "type": "postgres",
      "enabled": true,
      "config": {
        "host": "postgres-server",
        "port": 5432,
        "database": "analytics",
        "user": "app_user",
        "password": "${POSTGRES_PASSWORD}"
      },
      "metadata": {
        "description": "Operational analytics database"
      }
    },
    {
      "id": "mysql-orders",
      "name": "MySQL Orders",
      "type": "mysql",
      "enabled": true,
      "config": {
        "host": "mysql-server",
        "port": 3306,
        "database": "ecommerce",
        "user": "app_user",
        "password": "${MYSQL_PASSWORD}"
      },
      "metadata": {
        "description": "E-commerce order database"
      }
    }
  ]
}
"""

