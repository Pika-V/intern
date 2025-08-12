"""
DAO Layer - MySQL Implementation
Provides MySQL-specific data access operations
"""

import asyncio
from typing import Dict, List, Any, Optional, Union
import aiomysql
from datetime import datetime

from .base import BaseDAO, ConnectionMixin, ValidationMixin, QueryBuilder, SearchResponse, StatResult, QueryFilter, SortOption, PaginationOptions
from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG


class MySQLDAO(BaseDAO, ConnectionMixin, ValidationMixin):
    """MySQL Data Access Object implementation"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.pool = None
    
    async def connect(self) -> bool:
        """Establish MySQL connection pool"""
        try:
            self.pool = await aiomysql.create_pool(
                host=self.config.get('address', 'localhost'),
                port=self.config.get('port', 3306),
                user=self.config.get('username', 'root'),
                password=self.config.get('password', ''),
                db=self.config.get('dbname', 'uec'),
                charset=self.config.get('charset', 'utf8mb4'),
                minsize=1,
                maxsize=10,
                autocommit=True
            )
            LOG_INFO("MySQL connection pool created successfully")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to create MySQL connection pool: {e}")
            return False
    
    async def disconnect(self) -> bool:
        """Close MySQL connection pool"""
        try:
            if self.pool:
                self.pool.close()
                await self.pool.wait_closed()
                LOG_INFO("MySQL connection pool closed successfully")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to close MySQL connection pool: {e}")
            return False
    
    async def execute_query(
        self,
        query: str,
        params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        if not self.pool:
            raise RuntimeError("MySQL connection pool not initialized")
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    LOG_DEBUG(f"Executing query: {query}")
                    LOG_DEBUG(f"Query params: {params}")
                    
                    await cursor.execute(query, params or [])
                    results = await cursor.fetchall()
                    
                    LOG_DEBUG(f"Query returned {len(results)} rows")
                    return results
        except Exception as e:
            LOG_ERROR(f"Query execution failed: {e}")
            raise
    
    async def execute_update(
        self,
        query: str,
        params: Optional[List[Any]] = None
    ) -> int:
        """Execute an update/delete operation and return affected rows"""
        if not self.pool:
            raise RuntimeError("MySQL connection pool not initialized")
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    LOG_DEBUG(f"Executing update: {query}")
                    LOG_DEBUG(f"Update params: {params}")
                    
                    await cursor.execute(query, params or [])
                    affected_rows = cursor.rowcount
                    
                    LOG_DEBUG(f"Update affected {affected_rows} rows")
                    return affected_rows
        except Exception as e:
            LOG_ERROR(f"Update execution failed: {e}")
            raise
    
    async def insert_one(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> Optional[str]:
        """Insert a single record and return ID"""
        if not data:
            return None
        
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ['%s'] * len(values)
        
        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, values)
                    record_id = cursor.lastrowid
                    
                    LOG_INFO(f"Inserted record with ID: {record_id}")
                    return str(record_id)
        except Exception as e:
            LOG_ERROR(f"Insert failed: {e}")
            raise
    
    async def insert_many(
        self,
        table: str,
        data: List[Dict[str, Any]]
    ) -> List[str]:
        """Insert multiple records and return IDs"""
        if not data:
            return []
        
        columns = list(data[0].keys())
        placeholders = ['%s'] * len(columns)
        values_list = [[row.get(col) for col in columns] for row in data]
        
        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.executemany(query, values_list)
                    record_id = cursor.lastrowid
                    
                    # Generate IDs for bulk insert
                    record_ids = [str(record_id + i) for i in range(len(data))]
                    
                    LOG_INFO(f"Inserted {len(data)} records")
                    return record_ids
        except Exception as e:
            LOG_ERROR(f"Bulk insert failed: {e}")
            raise
    
    async def update_one(
        self,
        table: str,
        record_id: Union[str, int],
        data: Dict[str, Any]
    ) -> bool:
        """Update a single record"""
        if not data:
            return False
        
        set_clauses = [f"{col} = %s" for col in data.keys()]
        values = list(data.values())
        values.append(record_id)
        
        query = f"""
            UPDATE {table}
            SET {', '.join(set_clauses)}
            WHERE id = %s
        """
        
        try:
            affected_rows = await self.execute_update(query, values)
            return affected_rows > 0
        except Exception as e:
            LOG_ERROR(f"Update failed: {e}")
            raise
    
    async def delete_one(
        self,
        table: str,
        record_id: Union[str, int]
    ) -> bool:
        """Delete a single record"""
        query = f"DELETE FROM {table} WHERE id = %s"
        
        try:
            affected_rows = await self.execute_update(query, [record_id])
            return affected_rows > 0
        except Exception as e:
            LOG_ERROR(f"Delete failed: {e}")
            raise
    
    async def search(
        self,
        table: str,
        filters: List[QueryFilter],
        sort_options: Optional[List[SortOption]] = None,
        pagination: Optional[PaginationOptions] = None
    ) -> SearchResponse:
        """Search with filters and pagination"""
        builder = QueryBuilder()
        
        # Add filters
        for filter_obj in filters:
            builder.add_filter(filter_obj.field, filter_obj.operator, filter_obj.value)
        
        # Add sorting
        if sort_options:
            for sort_obj in sort_options:
                builder.add_sort(sort_obj.field, sort_obj.direction)
        
        # Add pagination
        if pagination:
            builder.paginate(pagination.offset, pagination.limit)
        
        # Build query
        where_clause = builder.build_where_clause()
        order_clause = builder.build_order_clause()
        pagination_clause = builder.build_pagination_clause()
        
        query = f"""
            SELECT * FROM {table}
            {where_clause}
            {order_clause}
            {pagination_clause}
        """
        
        # Execute query
        results = await self.execute_query(query, builder.params)
        
        # Get total count
        count_query = f"""
            SELECT COUNT(*) as total FROM {table}
            {where_clause}
        """
        
        count_result = await self.execute_query(count_query, builder.params)
        total = count_result[0]['total'] if count_result else 0
        
        return SearchResponse(
            hits=results,
            total=total,
            stats={}
        )
    
    async def aggregate(
        self,
        table: str,
        filters: List[QueryFilter],
        agg_field: str
    ) -> StatResult:
        """Perform aggregation query"""
        builder = QueryBuilder()
        
        # Add filters
        for filter_obj in filters:
            builder.add_filter(filter_obj.field, filter_obj.operator, filter_obj.value)
        
        where_clause = builder.build_where_clause()
        
        # Build aggregation query
        query = f"""
            SELECT 
                COUNT({agg_field}) as count,
                SUM({agg_field}) as sum,
                AVG({agg_field}) as average,
                MIN({agg_field}) as min,
                MAX({agg_field}) as max
            FROM {table}
            {where_clause}
        """
        
        try:
            result = await self.execute_query(query, builder.params)
            
            if result:
                row = result[0]
                return StatResult(
                    count=row['count'] or 0,
                    sum=float(row['sum'] or 0),
                    average=float(row['average'] or 0),
                    min=float(row['min'] or 0),
                    max=float(row['max'] or 0)
                )
            else:
                return StatResult()
        except Exception as e:
            LOG_ERROR(f"Aggregation failed: {e}")
            raise
    
    async def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema information"""
        query = """
            SELECT 
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_KEY as column_key,
                COLUMN_DEFAULT as column_default,
                COLUMN_COMMENT as column_comment
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        
        return await self.execute_query(query, [self.config.get('dbname'), table_name])
    
    async def get_table_list(self) -> List[Dict[str, Any]]:
        """Get list of tables in database"""
        query = """
            SELECT 
                TABLE_NAME as table_name,
                TABLE_COMMENT as table_comment
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
        """
        
        return await self.execute_query(query, [self.config.get('dbname')])
    
    async def execute_transaction(self, operations: List[Dict[str, Any]]) -> bool:
        """Execute multiple operations in a transaction"""
        if not self.pool:
            raise RuntimeError("MySQL connection pool not initialized")
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    # Start transaction
                    await conn.begin()
                    
                    try:
                        for operation in operations:
                            query = operation['query']
                            params = operation.get('params', [])
                            
                            await cursor.execute(query, params)
                        
                        # Commit transaction
                        await conn.commit()
                        LOG_INFO("Transaction completed successfully")
                        return True
                        
                    except Exception as e:
                        # Rollback transaction
                        await conn.rollback()
                        LOG_ERROR(f"Transaction failed, rolled back: {e}")
                        raise
                        
        except Exception as e:
            LOG_ERROR(f"Transaction execution failed: {e}")
            raise
    
    async def backup_table(self, table_name: str, backup_suffix: str = "_backup") -> bool:
        """Create a backup of a table"""
        backup_table = f"{table_name}{backup_suffix}"
        
        # Drop backup table if it exists
        drop_query = f"DROP TABLE IF EXISTS {backup_table}"
        await self.execute_update(drop_query)
        
        # Create backup
        backup_query = f"CREATE TABLE {backup_table} AS SELECT * FROM {table_name}"
        await self.execute_update(backup_query)
        
        LOG_INFO(f"Table {table_name} backed up to {backup_table}")
        return True