"""
Service Layer - MCP Service Implementation
Provides MCP (Model Context Protocol) service functionality
"""

import asyncio
from typing import Dict, List, Any, Optional, Callable
import json
from mcp import ClientSession, StdioServerParameters
import mcp.types as types

from .base import MCPService, ServiceResult, ServiceConfig
from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG
from ..conf import settings


class MCPServerManager:
    """Manages MCP server connections and tool registration"""
    
    def __init__(self):
        self.server = None
        self.session = None
        self.tools = {}
        self.available = False
    
    async def connect_stdio(self, command: str, args: List[str] = None, env: Dict[str, str] = None) -> bool:
        """Connect to MCP server via stdio"""
        try:
            server_params = StdioServerParameters(
                command=command,
                args=args or [],
                env=env or {}
            )
            
            self.session = ClientSession(server_params)
            await self.session.initialize()
            
            self.available = True
            LOG_INFO("MCP stdio connection established")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to connect to MCP stdio server: {e}")
            return False
    
    async def connect_sse(self, url: str) -> bool:
        """Connect to MCP server via SSE"""
        try:
            # Note: SSE connection implementation depends on MCP library version
            # This is a placeholder implementation
            LOG_INFO(f"Connecting to MCP SSE server at: {url}")
            self.available = True
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to connect to MCP SSE server: {e}")
            return False
    
    async def list_tools(self) -> List[types.Tool]:
        """List available tools from MCP server"""
        if not self.session or not self.available:
            return []
        
        try:
            tools_result = await self.session.list_tools()
            return tools_result.tools
        except Exception as e:
            LOG_ERROR(f"Failed to list MCP tools: {e}")
            return []
    
    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Any:
        """Call a tool on the MCP server"""
        if not self.session or not self.available:
            raise RuntimeError("MCP server not connected")
        
        try:
            result = await self.session.call_tool(name, arguments)
            return result
        except Exception as e:
            LOG_ERROR(f"Failed to call MCP tool {name}: {e}")
            raise
    
    async def close(self) -> None:
        """Close MCP server connection"""
        if self.session:
            try:
                await self.session.close()
                LOG_INFO("MCP server connection closed")
            except Exception as e:
                LOG_ERROR(f"Failed to close MCP server connection: {e}")
        finally:
            self.session = None
            self.available = False


class MCPServiceImplementation(MCPService):
    """MCP service implementation"""
    
    def __init__(self):
        config = ServiceConfig(
            service_name="mcp_service",
            version="1.0.0",
            enabled=settings.mcp_tool.mtype in ["stdio", "sse"]
        )
        super().__init__(config)
        self.server_manager = MCPServerManager()
        self.tool_handlers = {}
    
    async def _on_initialize(self) -> None:
        """Initialize MCP service"""
        await self._connect_to_server()
        await self._register_tools()
    
    async def _on_shutdown(self) -> None:
        """Shutdown MCP service"""
        await self.server_manager.close()
        self.tool_handlers.clear()
    
    async def _connect_to_server(self) -> None:
        """Connect to MCP server based on configuration"""
        mcp_type = settings.mcp_tool.mtype
        
        if mcp_type == "stdio":
            await self._connect_stdio()
        elif mcp_type == "sse":
            await self._connect_sse()
        else:
            LOG_ERROR(f"Unsupported MCP type: {mcp_type}")
    
    async def _connect_stdio(self) -> None:
        """Connect to stdio MCP server"""
        try:
            command = settings.mcp_tool.stdio_command
            env = {}
            args = []
            
            if settings.mcp_tool.stdio_env:
                env_pairs = settings.mcp_tool.stdio_env.split(";")
                for pair in env_pairs:
                    if "=" in pair:
                        key, value = pair.split("=", 1)
                        env[key.strip()] = value.strip()
            
            if settings.mcp_tool.stdio_args:
                args = settings.mcp_tool.stdio_args.split(";")
            
            success = await self.server_manager.connect_stdio(command, args, env)
            if success:
                LOG_INFO("MCP stdio server connected successfully")
            else:
                LOG_ERROR("Failed to connect to MCP stdio server")
        except Exception as e:
            LOG_ERROR(f"Error connecting to MCP stdio server: {e}")
    
    async def _connect_sse(self) -> None:
        """Connect to SSE MCP server"""
        try:
            url = settings.mcp_tool.sse_url
            if url:
                success = await self.server_manager.connect_sse(url)
                if success:
                    LOG_INFO("MCP SSE server connected successfully")
                else:
                    LOG_ERROR("Failed to connect to MCP SSE server")
            else:
                LOG_ERROR("MCP SSE URL not configured")
        except Exception as e:
            LOG_ERROR(f"Error connecting to MCP SSE server: {e}")
    
    async def _register_tools(self) -> None:
        """Register MCP tools"""
        try:
            if not self.server_manager.available:
                LOG_WARNING("MCP server not available, skipping tool registration")
                return
            
            # Get available tools from MCP server
            tools = await self.server_manager.list_tools()
            
            # Register tool handlers
            for tool in tools:
                self.register_tool(tool.name, tool)
            
            LOG_INFO(f"Registered {len(tools)} MCP tools")
        except Exception as e:
            LOG_ERROR(f"Failed to register MCP tools: {e}")
    
    def register_tool_handler(self, tool_name: str, handler: Callable) -> None:
        """Register a tool handler function"""
        self.tool_handlers[tool_name] = handler
        LOG_DEBUG(f"Registered tool handler: {tool_name}")
    
    async def execute_tool(
        self,
        tool_name: str,
        params: Dict[str, Any]
    ) -> ServiceResult:
        """Execute a tool by name"""
        # Check if we have a local handler
        if tool_name in self.tool_handlers:
            try:
                result = await self.tool_handlers[tool_name](**params)
                return self.create_success_result(data=result)
            except Exception as e:
                LOG_ERROR(f"Local tool handler failed: {e}")
                return self.create_error_result(f"Tool execution failed: {str(e)}")
        
        # Otherwise, try to call MCP server
        if self.server_manager.available:
            try:
                result = await self.server_manager.call_tool(tool_name, params)
                return self.create_success_result(data=result)
            except Exception as e:
                LOG_ERROR(f"MCP tool call failed: {e}")
                return self.create_error_result(f"Tool execution failed: {str(e)}")
        
        return self.create_error_result(f"Tool not found: {tool_name}")
    
    async def list_available_tools(self) -> List[str]:
        """List all available tools"""
        if self.server_manager.available:
            tools = await self.server_manager.list_tools()
            return [tool.name for tool in tools]
        return list(self.tools.keys())
    
    async def get_tool_info(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific tool"""
        if self.server_manager.available:
            try:
                tools = await self.server_manager.list_tools()
                for tool in tools:
                    if tool.name == tool_name:
                        return {
                            "name": tool.name,
                            "description": tool.description,
                            "inputSchema": tool.inputSchema
                        }
            except Exception as e:
                LOG_ERROR(f"Failed to get tool info: {e}")
        
        return None


# Tool decorator for registering MCP tools
def mcp_tool(name: str, description: str = ""):
    """Decorator to register a function as an MCP tool"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        
        wrapper._mcp_tool_name = name
        wrapper._mcp_tool_description = description
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator


# Global MCP service instance
mcp_service = MCPServiceImplementation()


async def register_mcp_tools():
    """Register all MCP tools from various modules"""
    try:
        # Import and register tools from different modules
        # This would be called during application startup
        
        # Example: Register ZXYC tools
        from .modules.zxyc_tools import register_zxyc_tools
        await register_zxyc_tools(mcp_service)
        
        # Example: Register BCP tools
        from .modules.bcp_tools import register_bcp_tools
        await register_bcp_tools(mcp_service)
        
        LOG_INFO("MCP tools registered successfully")
    except Exception as e:
        LOG_ERROR(f"Failed to register MCP tools: {e}")


async def execute_mcp_tool(tool_name: str, params: Dict[str, Any]) -> ServiceResult:
    """Execute an MCP tool (convenience function)"""
    return await mcp_service.execute_tool(tool_name, params)