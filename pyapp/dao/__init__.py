"""
DAO Layer - Factory and Utilities
Provides factory methods for creating DAO instances and utility functions
"""

from typing import Dict, Any, Optional, Union
from ..conf import settings
from .mysql_dao import MySQLDAO
from .elasticsearch_dao import ElasticsearchDAO
from .base import BaseDAO, SearchResponse, StatResult, QueryFilter, SortOption, PaginationOptions
from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG


class DAOFactory:
    """Factory class for creating DAO instances"""
    
    @staticmethod
    def create_mysql_dao() -> MySQLDAO:
        """Create MySQL DAO instance"""
        config = {
            'address': settings.mysql.address,
            'port': settings.mysql.port,
            'username': settings.mysql.username,
            'password': settings.mysql.password,
            'dbname': settings.mysql.dbname,
            'charset': settings.mysql.charset
        }
        return MySQLDAO(config)
    
    @staticmethod
    def create_elasticsearch_dao() -> ElasticsearchDAO:
        """Create Elasticsearch DAO instance"""
        config = {
            'uri': settings.es.uri,
            'username': settings.es.username,
            'password': settings.es.password
        }
        return ElasticsearchDAO(config)
    
    @staticmethod
    def create_dao(dao_type: str) -> BaseDAO:
        """Create DAO instance by type"""
        if dao_type.lower() == 'mysql':
            return DAOFactory.create_mysql_dao()
        elif dao_type.lower() == 'elasticsearch' or dao_type.lower() == 'es':
            return DAOFactory.create_elasticsearch_dao()
        else:
            raise ValueError(f"Unsupported DAO type: {dao_type}")


class DatabaseManager:
    """Manager class for handling multiple database connections"""
    
    def __init__(self):
        self.mysql_dao = None
        self.elasticsearch_dao = None
        self.connections = {}
    
    async def initialize_all(self) -> bool:
        """Initialize all database connections"""
        success = True
        
        # Initialize MySQL
        if settings.mysql.enable:
            try:
                self.mysql_dao = DAOFactory.create_mysql_dao()
                await self.mysql_dao.connect()
                self.connections['mysql'] = self.mysql_dao
                LOG_INFO("MySQL connection initialized")
            except Exception as e:
                LOG_ERROR(f"Failed to initialize MySQL: {e}")
                success = False
        
        # Initialize Elasticsearch
        if settings.es.enable:
            try:
                self.elasticsearch_dao = DAOFactory.create_elasticsearch_dao()
                await self.elasticsearch_dao.connect()
                self.connections['elasticsearch'] = self.elasticsearch_dao
                LOG_INFO("Elasticsearch connection initialized")
            except Exception as e:
                LOG_ERROR(f"Failed to initialize Elasticsearch: {e}")
                success = False
        
        return success
    
    async def close_all(self) -> bool:
        """Close all database connections"""
        success = True
        
        for name, dao in self.connections.items():
            try:
                await dao.disconnect()
                LOG_INFO(f"{name} connection closed")
            except Exception as e:
                LOG_ERROR(f"Failed to close {name} connection: {e}")
                success = False
        
        self.connections.clear()
        return success
    
    def get_mysql_dao(self) -> Optional[MySQLDAO]:
        """Get MySQL DAO instance"""
        return self.mysql_dao
    
    def get_elasticsearch_dao(self) -> Optional[ElasticsearchDAO]:
        """Get Elasticsearch DAO instance"""
        return self.elasticsearch_dao
    
    def get_dao(self, dao_type: str) -> Optional[BaseDAO]:
        """Get DAO instance by type"""
        return self.connections.get(dao_type.lower())


# Global database manager instance
db_manager = DatabaseManager()


async def get_dao(dao_type: str) -> BaseDAO:
    """Get DAO instance (convenience function)"""
    dao = db_manager.get_dao(dao_type)
    if not dao:
        raise RuntimeError(f"DAO {dao_type} not initialized")
    return dao


async def search_by_keywords(
    index: str,
    key_fields: Dict[str, Any],
    agg_field: Optional[str] = None,
    offset: int = 0,
    size: int = 100
) -> SearchResponse:
    """Search by keywords (convenience function similar to Go version)"""
    try:
        # Convert key_fields to QueryFilters
        filters = []
        for field, value in key_fields.items():
            if value is not None:
                # Simple equality filter
                filters.append(QueryFilter(field, "eq", value))
        
        # Get ES DAO
        es_dao = await get_dao('elasticsearch')
        
        # Perform search
        pagination = PaginationOptions(offset=offset, limit=size)
        result = await es_dao.search(index, filters, pagination=pagination)
        
        # If aggregation field is specified, perform aggregation
        if agg_field:
            agg_result = await es_dao.aggregate(index, filters, agg_field)
            result.stats = {agg_field: agg_result}
        
        return result
        
    except Exception as e:
        LOG_ERROR(f"Search by keywords failed: {e}")
        raise


def convert_millis_to_format14(millis: int) -> str:
    """Convert milliseconds timestamp to 14-digit format (YYYYMMDDHHMMSS)"""
    from datetime import datetime
    dt = datetime.fromtimestamp(millis / 1000)
    return dt.strftime("%Y%m%d%H%M%S")


def is_time_field(field_name: str) -> bool:
    """Check if field is a time field"""
    time_fields = {
        'SJSJ', 'XJSJ', 'RKSJ', 'SAVETIME',
        'KDSJ', 'ZYRKSJ', 'GXSJ', 'CJSJ',
        'JZSJ', 'TJSJ', 'XXSXSJ', 'GGSJXXXRKSJ',
        'JSSJ', 'KSSJ', 'NWRKSJ', 'ZDSJ', 'QXSJ', 'ZYKRKSJ',
        'CCSJ', 'JCSJ', 'SCSJ', 'LDSJ', 'RZSJ',
        'HCSJ', 'XXRKSJ', 'JYSJ', 'CSRQ', 'CSSJ', 'DDSJ',
        'JPSJ', 'YDSJ', 'LKSJ', 'CFSJ', 'YDCFSJ', 'DDCJSJ', 'FKSJ', 'TBSJ'
    }
    return field_name in time_fields


def is_flexible_time_field(field_name: str) -> bool:
    """Check if field is a flexible time field (needs 14-digit format)"""
    flexible_time_fields = {'SJSJ', 'XJSJ'}
    return field_name in flexible_time_fields


def parse_time_range(time_range_str: str, to_millis: bool = True) -> Any:
    """Parse time range string to appropriate format"""
    # This is a simplified implementation
    # In the real version, this would handle various date/time formats
    # like the Go version does
    
    if ',' in time_range_str:
        parts = time_range_str.split(',')
        if len(parts) == 2:
            start, end = parts
            return {'start': start.strip(), 'end': end.strip()}
    
    return time_range_str


# Export classes and functions
__all__ = [
    'DAOFactory',
    'DatabaseManager',
    'db_manager',
    'get_dao',
    'search_by_keywords',
    'convert_millis_to_format14',
    'is_time_field',
    'is_flexible_time_field',
    'parse_time_range',
    'MySQLDAO',
    'ElasticsearchDAO',
    'BaseDAO',
    'SearchResponse',
    'StatResult',
    'QueryFilter',
    'SortOption',
    'PaginationOptions'
]