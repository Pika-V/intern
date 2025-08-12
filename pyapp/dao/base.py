"""
DAO Layer - Base Classes and Interfaces
Provides base data access objects for different database types
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class SearchResponse:
    """Standard search response"""
    hits: List[Dict[str, Any]]
    total: int
    stats: Dict[str, 'StatResult']


@dataclass
class StatResult:
    """Statistical result for aggregation queries"""
    count: int = 0
    sum: float = 0.0
    average: float = 0.0
    min: float = 0.0
    max: float = 0.0


@dataclass
class QueryFilter:
    """Query filter for database operations"""
    field: str
    operator: str  # eq, ne, gt, lt, gte, lte, like, in, not_in
    value: Any


@dataclass
class SortOption:
    """Sort option for query results"""
    field: str
    direction: str = "asc"  # asc, desc


@dataclass
class PaginationOptions:
    """Pagination options for queries"""
    offset: int = 0
    limit: int = 100
    max_limit: int = 1000


class BaseDAO(ABC):
    """Base Data Access Object interface"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.connection = None
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish database connection"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """Close database connection"""
        pass
    
    @abstractmethod
    async def execute_query(
        self,
        query: str,
        params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        pass
    
    @abstractmethod
    async def execute_update(
        self,
        query: str,
        params: Optional[List[Any]] = None
    ) -> int:
        """Execute an update/delete operation and return affected rows"""
        pass
    
    @abstractmethod
    async def insert_one(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> Optional[str]:
        """Insert a single record and return ID"""
        pass
    
    @abstractmethod
    async def insert_many(
        self,
        table: str,
        data: List[Dict[str, Any]]
    ) -> List[str]:
        """Insert multiple records and return IDs"""
        pass
    
    @abstractmethod
    async def update_one(
        self,
        table: str,
        record_id: Union[str, int],
        data: Dict[str, Any]
    ) -> bool:
        """Update a single record"""
        pass
    
    @abstractmethod
    async def delete_one(
        self,
        table: str,
        record_id: Union[str, int]
    ) -> bool:
        """Delete a single record"""
        pass
    
    @abstractmethod
    async def search(
        self,
        index: str,
        filters: List[QueryFilter],
        sort_options: Optional[List[SortOption]] = None,
        pagination: Optional[PaginationOptions] = None
    ) -> SearchResponse:
        """Search with filters and pagination"""
        pass
    
    @abstractmethod
    async def aggregate(
        self,
        index: str,
        filters: List[QueryFilter],
        agg_field: str
    ) -> StatResult:
        """Perform aggregation query"""
        pass


class ConnectionMixin:
    """Mixin class for connection management"""
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()
    
    def is_connected(self) -> bool:
        """Check if connection is active"""
        return self.connection is not None


class ValidationMixin:
    """Mixin class for data validation"""
    
    def validate_query_params(self, params: List[Any]) -> bool:
        """Validate query parameters"""
        return params is not None
    
    def validate_search_filters(self, filters: List[QueryFilter]) -> bool:
        """Validate search filters"""
        if not filters:
            return True
        
        for filter_obj in filters:
            if not filter_obj.field or not filter_obj.operator:
                return False
        return True
    
    def sanitize_input(self, value: Any) -> Any:
        """Sanitize input values"""
        if isinstance(value, str):
            # Basic SQL injection prevention
            value = value.replace("'", "''")
            value = value.replace(";", "")
            value = value.replace("--", "")
        return value


class QueryBuilder:
    """Helper class for building complex queries"""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset query builder"""
        self.query = ""
        self.params = []
        self.filters = []
        self.sorts = []
        self.pagination = None
    
    def add_filter(self, field: str, operator: str, value: Any) -> 'QueryBuilder':
        """Add a filter condition"""
        self.filters.append(QueryFilter(field, operator, value))
        return self
    
    def add_sort(self, field: str, direction: str = "asc") -> 'QueryBuilder':
        """Add a sort option"""
        self.sorts.append(SortOption(field, direction))
        return self
    
    def paginate(self, offset: int, limit: int) -> 'QueryBuilder':
        """Add pagination"""
        self.pagination = PaginationOptions(offset, limit)
        return self
    
    def build_where_clause(self) -> str:
        """Build WHERE clause from filters"""
        if not self.filters:
            return ""
        
        where_clauses = []
        for filter_obj in self.filters:
            clause = self._build_filter_clause(filter_obj)
            where_clauses.append(clause)
        
        return "WHERE " + " AND ".join(where_clauses)
    
    def _build_filter_clause(self, filter_obj: QueryFilter) -> str:
        """Build individual filter clause"""
        param_placeholder = f"${len(self.params) + 1}"
        
        if filter_obj.operator == "eq":
            self.params.append(filter_obj.value)
            return f"{filter_obj.field} = {param_placeholder}"
        elif filter_obj.operator == "ne":
            self.params.append(filter_obj.value)
            return f"{filter_obj.field} != {param_placeholder}"
        elif filter_obj.operator == "gt":
            self.params.append(filter_obj.value)
            return f"{filter_obj.field} > {param_placeholder}"
        elif filter_obj.operator == "lt":
            self.params.append(filter_obj.value)
            return f"{filter_obj.field} < {param_placeholder}"
        elif filter_obj.operator == "gte":
            self.params.append(filter_obj.value)
            return f"{filter_obj.field} >= {param_placeholder}"
        elif filter_obj.operator == "lte":
            self.params.append(filter_obj.value)
            return f"{filter_obj.field} <= {param_placeholder}"
        elif filter_obj.operator == "like":
            self.params.append(f"%{filter_obj.value}%")
            return f"{filter_obj.field} LIKE {param_placeholder}"
        elif filter_obj.operator == "in":
            placeholders = []
            for i, val in enumerate(filter_obj.value):
                self.params.append(val)
                placeholders.append(f"${len(self.params)}")
            return f"{filter_obj.field} IN ({', '.join(placeholders)})"
        elif filter_obj.operator == "not_in":
            placeholders = []
            for i, val in enumerate(filter_obj.value):
                self.params.append(val)
                placeholders.append(f"${len(self.params)}")
            return f"{filter_obj.field} NOT IN ({', '.join(placeholders)})"
        else:
            raise ValueError(f"Unsupported operator: {filter_obj.operator}")
    
    def build_order_clause(self) -> str:
        """Build ORDER BY clause"""
        if not self.sorts:
            return ""
        
        order_clauses = []
        for sort_obj in self.sorts:
            direction = "ASC" if sort_obj.direction.lower() == "asc" else "DESC"
            order_clauses.append(f"{sort_obj.field} {direction}")
        
        return "ORDER BY " + ", ".join(order_clauses)
    
    def build_pagination_clause(self) -> str:
        """Build pagination clause"""
        if not self.pagination:
            return ""
        
        return f"LIMIT {self.pagination.limit} OFFSET {self.pagination.offset}"