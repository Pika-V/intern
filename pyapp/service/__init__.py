"""
Service Layer - Main Module
Provides business logic services and MCP tool management
"""

from .base import (
    BaseService,
    DataService,
    MCPService,
    ServiceResult,
    SearchResult,
    ServiceConfig,
    ServiceRegistry,
    service_registry
)
from .mcp_service import (
    MCPServiceImplementation,
    mcp_service,
    register_mcp_tools,
    execute_mcp_tool,
    mcp_tool
)
from .modules.zxyc_tools import (
    ZXYCDataService,
    zxyc_service,
    register_zxyc_tools
)

# Export main classes and functions
__all__ = [
    # Base classes
    'BaseService',
    'DataService',
    'MCPService',
    'ServiceResult',
    'SearchResult',
    'ServiceConfig',
    
    # Registry
    'ServiceRegistry',
    'service_registry',
    
    # MCP Service
    'MCPServiceImplementation',
    'mcp_service',
    'register_mcp_tools',
    'execute_mcp_tool',
    'mcp_tool',
    
    # ZXYC Services
    'ZXYCDataService',
    'zxyc_service',
    'register_zxyc_tools',
]


async def initialize_services() -> bool:
    """Initialize all services"""
    try:
        # Initialize service registry
        success = await service_registry.initialize_all()
        
        # Register MCP tools
        await register_mcp_tools()
        
        return success
    except Exception as e:
        from ..logger import LOG_ERROR
        LOG_ERROR(f"Failed to initialize services: {e}")
        return False


async def shutdown_services() -> bool:
    """Shutdown all services"""
    try:
        return await service_registry.shutdown_all()
    except Exception as e:
        from ..logger import LOG_ERROR
        LOG_ERROR(f"Failed to shutdown services: {e}")
        return False


def get_service(service_name: str) -> BaseService:
    """Get service by name"""
    return service_registry.get_service(service_name)