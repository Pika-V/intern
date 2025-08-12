"""
Service Layer - Base Classes and Interfaces
Provides base service classes for business logic
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Type
from dataclasses import dataclass
from datetime import datetime
import json

from ..dao.base import SearchResponse, StatResult, QueryFilter, SortOption, PaginationOptions
from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG


@dataclass
class ServiceResult:
    """Standard service result"""
    success: bool
    data: Any = None
    message: str = ""
    error_code: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SearchResult:
    """Search result with pagination"""
    items: List[Dict[str, Any]]
    total: int
    page: int
    size: int
    total_pages: int
    stats: Optional[Dict[str, StatResult]] = None


@dataclass
class ServiceConfig:
    """Service configuration"""
    service_name: str
    version: str = "1.0.0"
    enabled: bool = True
    cache_enabled: bool = False
    cache_ttl: int = 3600  # seconds


class BaseService(ABC):
    """Base service class"""
    
    def __init__(self, config: ServiceConfig):
        self.config = config
        self._initialized = False
    
    async def initialize(self) -> bool:
        """Initialize service"""
        try:
            await self._on_initialize()
            self._initialized = True
            LOG_INFO(f"Service {self.config.service_name} initialized successfully")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to initialize service {self.config.service_name}: {e}")
            return False
    
    async def shutdown(self) -> bool:
        """Shutdown service"""
        try:
            await self._on_shutdown()
            self._initialized = False
            LOG_INFO(f"Service {self.config.service_name} shutdown successfully")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to shutdown service {self.config.service_name}: {e}")
            return False
    
    @abstractmethod
    async def _on_initialize(self) -> None:
        """Service-specific initialization logic"""
        pass
    
    @abstractmethod
    async def _on_shutdown(self) -> None:
        """Service-specific shutdown logic"""
        pass
    
    def is_initialized(self) -> bool:
        """Check if service is initialized"""
        return self._initialized
    
    def create_success_result(
        self,
        data: Any = None,
        message: str = "Success",
        metadata: Optional[Dict[str, Any]] = None
    ) -> ServiceResult:
        """Create success result"""
        return ServiceResult(
            success=True,
            data=data,
            message=message,
            metadata=metadata
        )
    
    def create_error_result(
        self,
        message: str,
        error_code: Optional[str] = None,
        data: Any = None
    ) -> ServiceResult:
        """Create error result"""
        return ServiceResult(
            success=False,
            data=data,
            message=message,
            error_code=error_code
        )
    
    def validate_required_fields(self, data: Dict[str, Any], required_fields: List[str]) -> bool:
        """Validate required fields in data"""
        for field in required_fields:
            if field not in data or data[field] is None:
                LOG_ERROR(f"Missing required field: {field}")
                return False
        return True


class DataService(BaseService):
    """Base data service with common CRUD operations"""
    
    def __init__(self, config: ServiceConfig, dao_type: str = "elasticsearch"):
        super().__init__(config)
        self.dao_type = dao_type
        self._dao = None
    
    async def _on_initialize(self) -> None:
        """Initialize data service"""
        from ..dao import get_dao
        self._dao = await get_dao(self.dao_type)
    
    async def search_data(
        self,
        index: str,
        filters: List[QueryFilter],
        sort_options: Optional[List[SortOption]] = None,
        pagination: Optional[PaginationOptions] = None
    ) -> SearchResult:
        """Search data with filters and pagination"""
        if not self._dao:
            raise RuntimeError("Service not initialized")
        
        try:
            response = await self._dao.search(index, filters, sort_options, pagination)
            
            # Calculate pagination info
            page = (pagination.offset // pagination.limit) + 1 if pagination else 1
            size = pagination.limit if pagination else len(response.hits)
            total_pages = (response.total + size - 1) // size if size > 0 else 1
            
            return SearchResult(
                items=response.hits,
                total=response.total,
                page=page,
                size=size,
                total_pages=total_pages,
                stats=response.stats
            )
        except Exception as e:
            LOG_ERROR(f"Search failed: {e}")
            raise
    
    async def get_by_id(
        self,
        index: str,
        record_id: Union[str, int]
    ) -> Optional[Dict[str, Any]]:
        """Get record by ID"""
        if not self._dao:
            raise RuntimeError("Service not initialized")
        
        try:
            filters = [QueryFilter("id", "eq", record_id)]
            response = await self._dao.search(index, filters, pagination=PaginationOptions(limit=1))
            
            return response.hits[0] if response.hits else None
        except Exception as e:
            LOG_ERROR(f"Get by ID failed: {e}")
            raise
    
    async def create_record(
        self,
        index: str,
        data: Dict[str, Any]
    ) -> ServiceResult:
        """Create new record"""
        if not self._dao:
            raise RuntimeError("Service not initialized")
        
        try:
            record_id = await self._dao.insert_one(index, data)
            return self.create_success_result(
                data={"id": record_id},
                message="Record created successfully"
            )
        except Exception as e:
            LOG_ERROR(f"Create record failed: {e}")
            return self.create_error_result(f"Failed to create record: {str(e)}")
    
    async def update_record(
        self,
        index: str,
        record_id: Union[str, int],
        data: Dict[str, Any]
    ) -> ServiceResult:
        """Update record"""
        if not self._dao:
            raise RuntimeError("Service not initialized")
        
        try:
            success = await self._dao.update_one(index, record_id, data)
            if success:
                return self.create_success_result(
                    data={"id": record_id},
                    message="Record updated successfully"
                )
            else:
                return self.create_error_result("Record not found")
        except Exception as e:
            LOG_ERROR(f"Update record failed: {e}")
            return self.create_error_result(f"Failed to update record: {str(e)}")
    
    async def delete_record(
        self,
        index: str,
        record_id: Union[str, int]
    ) -> ServiceResult:
        """Delete record"""
        if not self._dao:
            raise RuntimeError("Service not initialized")
        
        try:
            success = await self._dao.delete_one(index, record_id)
            if success:
                return self.create_success_result(
                    data={"id": record_id},
                    message="Record deleted successfully"
                )
            else:
                return self.create_error_result("Record not found")
        except Exception as e:
            LOG_ERROR(f"Delete record failed: {e}")
            return self.create_error_result(f"Failed to delete record: {str(e)}")
    
    async def aggregate_data(
        self,
        index: str,
        filters: List[QueryFilter],
        agg_field: str
    ) -> StatResult:
        """Aggregate data"""
        if not self._dao:
            raise RuntimeError("Service not initialized")
        
        try:
            return await self._dao.aggregate(index, filters, agg_field)
        except Exception as e:
            LOG_ERROR(f"Aggregate failed: {e}")
            raise


class MCPService(BaseService):
    """Base MCP (Model Context Protocol) service"""
    
    def __init__(self, config: ServiceConfig):
        super().__init__(config)
        self.tools = {}
        self.server = None
    
    async def _on_initialize(self) -> None:
        """Initialize MCP service"""
        await self._register_tools()
    
    async def _on_shutdown(self) -> None:
        """Shutdown MCP service"""
        await self._unregister_tools()
    
    @abstractmethod
    async def _register_tools(self) -> None:
        """Register MCP tools"""
        pass
    
    async def _unregister_tools(self) -> None:
        """Unregister MCP tools"""
        self.tools.clear()
    
    def register_tool(self, name: str, tool: Any) -> None:
        """Register a single tool"""
        self.tools[name] = tool
        LOG_DEBUG(f"Registered tool: {name}")
    
    def unregister_tool(self, name: str) -> None:
        """Unregister a single tool"""
        if name in self.tools:
            del self.tools[name]
            LOG_DEBUG(f"Unregistered tool: {name}")
    
    def get_tools(self) -> Dict[str, Any]:
        """Get all registered tools"""
        return self.tools.copy()
    
    async def execute_tool(
        self,
        tool_name: str,
        params: Dict[str, Any]
    ) -> ServiceResult:
        """Execute a tool by name"""
        if tool_name not in self.tools:
            return self.create_error_result(f"Tool not found: {tool_name}")
        
        try:
            tool = self.tools[tool_name]
            if hasattr(tool, '__call__'):
                result = await tool(**params)
                return self.create_success_result(data=result)
            else:
                return self.create_error_result(f"Tool is not callable: {tool_name}")
        except Exception as e:
            LOG_ERROR(f"Tool execution failed: {e}")
            return self.create_error_result(f"Tool execution failed: {str(e)}")


class ServiceRegistry:
    """Registry for managing services"""
    
    def __init__(self):
        self._services: Dict[str, BaseService] = {}
        self._initialized = False
    
    def register_service(self, service: BaseService) -> None:
        """Register a service"""
        self._services[service.config.service_name] = service
        LOG_INFO(f"Registered service: {service.config.service_name}")
    
    def unregister_service(self, service_name: str) -> None:
        """Unregister a service"""
        if service_name in self._services:
            del self._services[service_name]
            LOG_INFO(f"Unregistered service: {service_name}")
    
    def get_service(self, service_name: str) -> Optional[BaseService]:
        """Get service by name"""
        return self._services.get(service_name)
    
    async def initialize_all(self) -> bool:
        """Initialize all registered services"""
        success = True
        
        for service in self._services.values():
            if not service.is_initialized():
                result = await service.initialize()
                if not result:
                    success = False
        
        if success:
            self._initialized = True
            LOG_INFO("All services initialized successfully")
        else:
            LOG_ERROR("Some services failed to initialize")
        
        return success
    
    async def shutdown_all(self) -> bool:
        """Shutdown all registered services"""
        success = True
        
        for service in self._services.values():
            if service.is_initialized():
                result = await service.shutdown()
                if not result:
                    success = False
        
        if success:
            self._initialized = False
            LOG_INFO("All services shutdown successfully")
        else:
            LOG_ERROR("Some services failed to shutdown")
        
        return success
    
    def list_services(self) -> List[str]:
        """List all registered services"""
        return list(self._services.keys())
    
    def is_initialized(self) -> bool:
        """Check if all services are initialized"""
        return self._initialized


# Global service registry
service_registry = ServiceRegistry()