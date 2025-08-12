"""
DAO Layer - Elasticsearch Implementation
Provides Elasticsearch-specific data access operations
"""

import asyncio
from typing import Dict, List, Any, Optional, Union
import json
import re
from datetime import datetime
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from .base import BaseDAO, ConnectionMixin, ValidationMixin, QueryBuilder, SearchResponse, StatResult, QueryFilter, SortOption, PaginationOptions
from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG


class ElasticsearchDAO(BaseDAO, ConnectionMixin, ValidationMixin):
    """Elasticsearch Data Access Object implementation"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.client: Optional[AsyncElasticsearch] = None
    
    async def connect(self) -> bool:
        """Establish Elasticsearch connection"""
        try:
            hosts = [self.config.get('uri', 'http://localhost:9200')]
            
            # Build authentication
            auth = None
            username = self.config.get('username')
            password = self.config.get('password')
            if username and password:
                auth = (username, password)
            
            self.client = AsyncElasticsearch(
                hosts=hosts,
                basic_auth=auth,
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # Test connection
            info = await self.client.info()
            LOG_INFO(f"Elasticsearch connected successfully: {info.get('version', {}).get('number', 'unknown')}")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to connect to Elasticsearch: {e}")
            return False
    
    async def disconnect(self) -> bool:
        """Close Elasticsearch connection"""
        try:
            if self.client:
                await self.client.close()
                LOG_INFO("Elasticsearch connection closed successfully")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to close Elasticsearch connection: {e}")
            return False
    
    async def execute_query(
        self,
        query: str,
        params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        # For ES, we expect query to be a JSON string
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            query_dict = json.loads(query) if isinstance(query, str) else query
            
            # This is a simplified implementation - in reality, ES queries are more complex
            # We'll need to know the index to search against
            raise NotImplementedError("ES query execution requires index specification")
        except Exception as e:
            LOG_ERROR(f"ES query execution failed: {e}")
            raise
    
    async def execute_update(
        self,
        query: str,
        params: Optional[List[Any]] = None
    ) -> int:
        """Execute an update/delete operation and return affected rows"""
        # For ES, this would be a bulk operation or update_by_query
        raise NotImplementedError("ES update operations not yet implemented")
    
    async def insert_one(
        self,
        index: str,
        data: Dict[str, Any]
    ) -> Optional[str]:
        """Insert a single document and return ID"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            response = await self.client.index(
                index=index,
                body=data,
                refresh='wait_for'
            )
            
            doc_id = response.get('_id')
            LOG_INFO(f"Inserted document with ID: {doc_id}")
            return doc_id
        except Exception as e:
            LOG_ERROR(f"ES insert failed: {e}")
            raise
    
    async def insert_many(
        self,
        index: str,
        data: List[Dict[str, Any]]
    ) -> List[str]:
        """Insert multiple documents and return IDs"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            # Prepare bulk actions
            actions = []
            for doc in data:
                action = {
                    '_index': index,
                    '_source': doc
                }
                actions.append(action)
            
            # Execute bulk operation
            success_count, errors = await async_bulk(self.client, actions, refresh='wait_for')
            
            if errors:
                LOG_ERROR(f"Bulk insert had {len(errors)} errors")
                raise Exception(f"Bulk insert failed with {len(errors)} errors")
            
            # Generate IDs (ES might auto-generate them)
            doc_ids = [f"doc_{i}" for i in range(len(data))]
            
            LOG_INFO(f"Inserted {success_count} documents")
            return doc_ids
        except Exception as e:
            LOG_ERROR(f"ES bulk insert failed: {e}")
            raise
    
    async def update_one(
        self,
        index: str,
        doc_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """Update a single document"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            await self.client.update(
                index=index,
                id=doc_id,
                body={'doc': data},
                refresh='wait_for'
            )
            
            LOG_INFO(f"Updated document {doc_id}")
            return True
        except Exception as e:
            LOG_ERROR(f"ES update failed: {e}")
            raise
    
    async def delete_one(
        self,
        index: str,
        doc_id: str
    ) -> bool:
        """Delete a single document"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            await self.client.delete(
                index=index,
                id=doc_id,
                refresh='wait_for'
            )
            
            LOG_INFO(f"Deleted document {doc_id}")
            return True
        except Exception as e:
            LOG_ERROR(f"ES delete failed: {e}")
            raise
    
    async def search(
        self,
        index: str,
        filters: List[QueryFilter],
        sort_options: Optional[List[SortOption]] = None,
        pagination: Optional[PaginationOptions] = None
    ) -> SearchResponse:
        """Search with filters and pagination"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            # Build ES query
            query_body = self._build_search_query(filters, sort_options, pagination)
            
            # Execute search
            response = await self.client.search(
                index=index,
                body=query_body
            )
            
            # Extract results
            hits = []
            for hit in response.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                source['_id'] = hit.get('_id')
                source['_score'] = hit.get('_score')
                hits.append(source)
            
            total = response.get('hits', {}).get('total', {}).get('value', 0)
            
            return SearchResponse(
                hits=hits,
                total=total,
                stats={}
            )
        except Exception as e:
            LOG_ERROR(f"ES search failed: {e}")
            raise
    
    async def aggregate(
        self,
        index: str,
        filters: List[QueryFilter],
        agg_field: str
    ) -> StatResult:
        """Perform aggregation query"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            # Build aggregation query
            query_body = {
                'size': 0,
                'query': self._build_bool_query(filters),
                'aggs': {
                    'total_count': {
                        'value_count': {'field': agg_field}
                    },
                    'total_sum': {
                        'sum': {'field': agg_field}
                    },
                    'total_avg': {
                        'avg': {'field': agg_field}
                    },
                    'total_min': {
                        'min': {'field': agg_field}
                    },
                    'total_max': {
                        'max': {'field': agg_field}
                    }
                }
            }
            
            # Execute aggregation
            response = await self.client.search(
                index=index,
                body=query_body
            )
            
            # Extract aggregation results
            aggregations = response.get('aggregations', {})
            
            return StatResult(
                count=int(aggregations.get('total_count', {}).get('value', 0)),
                sum=float(aggregations.get('total_sum', {}).get('value', 0)),
                average=float(aggregations.get('total_avg', {}).get('value', 0)),
                min=float(aggregations.get('total_min', {}).get('value', 0)),
                max=float(aggregations.get('total_max', {}).get('value', 0))
            )
        except Exception as e:
            LOG_ERROR(f"ES aggregation failed: {e}")
            raise
    
    def _build_search_query(
        self,
        filters: List[QueryFilter],
        sort_options: Optional[List[SortOption]] = None,
        pagination: Optional[PaginationOptions] = None
    ) -> Dict[str, Any]:
        """Build ES search query"""
        query_body = {
            'query': self._build_bool_query(filters),
            'size': pagination.limit if pagination else 100,
            'from': pagination.offset if pagination else 0
        }
        
        # Add sorting
        if sort_options:
            sorts = []
            for sort_obj in sort_options:
                sort_dict = {sort_obj.field: {'order': sort_obj.direction.lower()}}
                sorts.append(sort_dict)
            query_body['sort'] = sorts
        
        return query_body
    
    def _build_bool_query(self, filters: List[QueryFilter]) -> Dict[str, Any]:
        """Build ES bool query from filters"""
        if not filters:
            return {'match_all': {}}
        
        must_clauses = []
        
        for filter_obj in filters:
            clause = self._build_filter_clause(filter_obj)
            if clause:
                must_clauses.append(clause)
        
        return {'bool': {'must': must_clauses}}
    
    def _build_filter_clause(self, filter_obj: QueryFilter) -> Optional[Dict[str, Any]]:
        """Build individual ES filter clause"""
        field = filter_obj.field
        operator = filter_obj.operator
        value = filter_obj.value
        
        # Handle time field detection
        if self._is_time_field(field):
            value = self._convert_time_value(value)
        
        if operator == "eq":
            # Check if it's a numeric value
            if isinstance(value, (int, float)):
                return {'term': {field: value}}
            else:
                # Use wildcard for string values to support partial matching
                return {'wildcard': {field: f"*{value}*"}}
        
        elif operator == "ne":
            return {'bool': {'must_not': {'term': {field: value}}}}
        
        elif operator == "gt":
            return {'range': {field: {'gt': value}}}
        
        elif operator == "lt":
            return {'range': {field: {'lt': value}}}
        
        elif operator == "gte":
            return {'range': {field: {'gte': value}}}
        
        elif operator == "lte":
            return {'range': {field: {'lte': value}}}
        
        elif operator == "like":
            return {'wildcard': {field: f"*{value}*"}}
        
        elif operator == "in":
            return {'terms': {field: value}}
        
        elif operator == "not_in":
            return {'bool': {'must_not': {'terms': {field: value}}}}
        
        else:
            LOG_WARNING(f"Unsupported operator: {operator}")
            return None
    
    def _is_time_field(self, field_name: str) -> bool:
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
    
    def _convert_time_value(self, value: Any) -> Any:
        """Convert time value for ES queries"""
        if isinstance(value, str):
            # Handle comma-separated timestamp ranges
            if ',' in value and re.match(r'^\d+,\d+$', value):
                start_ts, end_ts = value.split(',')
                return {
                    'gte': int(start_ts),
                    'lte': int(end_ts)
                }
            
            # Handle date ranges
            elif ',' in value and '-' in value:
                # Parse date range logic here
                pass
        
        return value
    
    async def index_exists(self, index: str) -> bool:
        """Check if index exists"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            return await self.client.indices.exists(index=index)
        except Exception as e:
            LOG_ERROR(f"Failed to check index existence: {e}")
            return False
    
    async def create_index(self, index: str, mapping: Optional[Dict[str, Any]] = None) -> bool:
        """Create index with optional mapping"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            if mapping:
                await self.client.indices.create(index=index, body={'mappings': mapping})
            else:
                await self.client.indices.create(index=index)
            
            LOG_INFO(f"Created index: {index}")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to create index: {e}")
            return False
    
    async def delete_index(self, index: str) -> bool:
        """Delete index"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            await self.client.indices.delete(index=index)
            LOG_INFO(f"Deleted index: {index}")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to delete index: {e}")
            return False
    
    async def get_mapping(self, index: str) -> Dict[str, Any]:
        """Get index mapping"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            response = await self.client.indices.get_mapping(index=index)
            return response
        except Exception as e:
            LOG_ERROR(f"Failed to get mapping: {e}")
            return {}
    
    async def scroll_search(
        self,
        index: str,
        query: Dict[str, Any],
        scroll_size: int = 1000,
        scroll_timeout: str = '2m'
    ) -> List[Dict[str, Any]]:
        """Perform scroll search for large result sets"""
        if not self.client:
            raise RuntimeError("Elasticsearch client not initialized")
        
        try:
            all_hits = []
            
            # Initial search
            response = await self.client.search(
                index=index,
                body=query,
                scroll=scroll_timeout,
                size=scroll_size
            )
            
            scroll_id = response.get('_scroll_id')
            hits = response.get('hits', {}).get('hits', [])
            all_hits.extend(hits)
            
            # Keep scrolling
            while hits:
                response = await self.client.scroll(
                    scroll_id=scroll_id,
                    scroll=scroll_timeout
                )
                
                scroll_id = response.get('_scroll_id')
                hits = response.get('hits', {}).get('hits', [])
                all_hits.extend(hits)
                
                if not hits:
                    break
            
            # Clear scroll
            if scroll_id:
                await self.client.clear_scroll(scroll_id=scroll_id)
            
            return [hit.get('_source', {}) for hit in all_hits]
        except Exception as e:
            LOG_ERROR(f"Scroll search failed: {e}")
            raise