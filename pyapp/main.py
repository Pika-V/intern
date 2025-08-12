"""
Main Application Entry Point
FastAPI application with AI agent capabilities
"""

import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
import uvicorn

from .conf import settings
from .logger import LOG_INFO, LOG_ERROR, LOG_DEBUG
from .dao import db_manager
from .service import initialize_services, shutdown_services
from .agent import initialize_agents, shutdown_agents, process_message
from .service import execute_mcp_tool


# Pydantic models for API
class ChatRequest(BaseModel):
    """Chat request model"""
    message: str = Field(..., description="User message")
    agent_name: Optional[str] = Field("query_assistant_agent", description="Agent name to use")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class ChatResponse(BaseModel):
    """Chat response model"""
    response: str = Field(..., description="Assistant response")
    tool_calls: List[Dict[str, Any]] = Field(default_factory=list, description="Tool calls made")
    usage_stats: Optional[Dict[str, Any]] = Field(None, description="Usage statistics")


class ToolRequest(BaseModel):
    """Tool execution request model"""
    tool_name: str = Field(..., description="Tool name to execute")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Tool parameters")


class ToolResponse(BaseModel):
    """Tool execution response model"""
    success: bool = Field(..., description="Whether tool execution was successful")
    data: Any = Field(None, description="Tool execution result")
    message: str = Field(..., description="Response message")


class HealthResponse(BaseModel):
    """Health check response model"""
    status: str = Field(..., description="Application status")
    services: Dict[str, str] = Field(default_factory=dict, description="Service statuses")
    agents: Dict[str, str] = Field(default_factory=dict, description="Agent statuses")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    LOG_INFO("Starting application...")
    
    try:
        # Initialize database connections
        db_success = await db_manager.initialize_all()
        if not db_success:
            LOG_ERROR("Failed to initialize database connections")
        
        # Initialize services
        service_success = await initialize_services()
        if not service_success:
            LOG_ERROR("Failed to initialize services")
        
        # Initialize agents
        agent_success = await initialize_agents()
        if not agent_success:
            LOG_ERROR("Failed to initialize agents")
        
        LOG_INFO("Application started successfully")
        
    except Exception as e:
        LOG_ERROR(f"Failed to start application: {e}")
        raise
    
    yield
    
    # Shutdown
    LOG_INFO("Shutting down application...")
    
    try:
        # Shutdown agents
        await shutdown_agents()
        
        # Shutdown services
        await shutdown_services()
        
        # Close database connections
        await db_manager.close_all()
        
        LOG_INFO("Application shutdown successfully")
        
    except Exception as e:
        LOG_ERROR(f"Error during shutdown: {e}")


# Create FastAPI application
app = FastAPI(
    title="ZX AI Framework - Python Version",
    description="AI-powered data querying and analysis framework",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint"""
    return {
        "message": "ZX AI Framework - Python Version",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        # Check database connections
        db_status = {}
        for name, dao in db_manager.connections.items():
            db_status[name] = "connected" if dao else "disconnected"
        
        # Check service status
        from .service import service_registry
        service_status = {}
        for service_name in service_registry.list_services():
            service = service_registry.get_service(service_name)
            service_status[service_name] = "initialized" if service and service.is_initialized() else "not_initialized"
        
        # Check agent status
        from .agent import agent_registry
        agent_status = {}
        for agent_name in agent_registry.list_agents():
            agent = agent_registry.get_agent(agent_name)
            agent_status[agent_name] = "initialized" if agent and agent.is_initialized() else "not_initialized"
        
        return HealthResponse(
            status="healthy",
            services=service_status,
            agents=agent_status
        )
    
    except Exception as e:
        LOG_ERROR(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            services={},
            agents={}
        )


@app.post("/api/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    """Chat endpoint for interacting with AI agents"""
    try:
        # Process message with agent
        response = await process_message(
            message=request.message,
            agent_name=request.agent_name,
            context=request.context
        )
        
        # Convert tool calls to dict format
        tool_calls = []
        for tool_call in response.tool_calls:
            tool_calls.append({
                "tool_name": tool_call.tool_name,
                "parameters": tool_call.parameters
            })
        
        return ChatResponse(
            response=response.message.content,
            tool_calls=tool_calls,
            usage_stats=response.usage_stats
        )
    
    except Exception as e:
        LOG_ERROR(f"Chat endpoint failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/tool", response_model=ToolResponse)
async def tool_endpoint(request: ToolRequest):
    """Tool execution endpoint"""
    try:
        # Execute MCP tool
        result = await execute_mcp_tool(request.tool_name, request.parameters)
        
        return ToolResponse(
            success=result.success,
            data=result.data,
            message=result.message
        )
    
    except Exception as e:
        LOG_ERROR(f"Tool endpoint failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tools")
async def list_tools():
    """List available tools"""
    try:
        from .service import mcp_service
        tools = await mcp_service.list_available_tools()
        return {"tools": tools}
    
    except Exception as e:
        LOG_ERROR(f"List tools failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/agents")
async def list_agents():
    """List available agents"""
    try:
        from .agent import agent_registry
        agents = agent_registry.list_agents()
        return {"agents": agents}
    
    except Exception as e:
        LOG_ERROR(f"List agents failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tables")
async def list_tables():
    """List available data tables"""
    try:
        from .dao import DAOFactory
        mysql_dao = DAOFactory.create_mysql_dao()
        
        # Connect and get tables
        await mysql_dao.connect()
        tables = await mysql_dao.get_table_list()
        await mysql_dao.disconnect()
        
        return {"tables": tables}
    
    except Exception as e:
        LOG_ERROR(f"List tables failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class QueryRequest(BaseModel):
    """Data query request model"""
    table_name: str = Field(..., description="Table name to query")
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Query filters")
    limit: int = Field(100, description="Result limit")
    offset: int = Field(0, description="Result offset")


@app.post("/api/query")
async def query_data(request: QueryRequest):
    """Query data endpoint"""
    try:
        from .dao import DAOFactory, QueryFilter, PaginationOptions
        
        # Create DAO
        dao = DAOFactory.create_dao("elasticsearch")
        
        # Convert filters to QueryFilter objects
        filters = []
        for field, value in request.filters.items():
            if value is not None:
                filters.append(QueryFilter(field, "eq", value))
        
        # Create pagination
        pagination = PaginationOptions(
            offset=request.offset,
            limit=request.limit
        )
        
        # Execute search
        result = await dao.search(request.table_name, filters, pagination=pagination)
        
        return {
            "data": result.hits,
            "total": result.total,
            "page": (request.offset // request.limit) + 1,
            "size": request.limit,
            "total_pages": (result.total + request.limit - 1) // request.limit
        }
    
    except Exception as e:
        LOG_ERROR(f"Query endpoint failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def main():
    """Main function to run the application"""
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Get configuration
    host = "0.0.0.0"
    port = settings.platform.web_port
    
    LOG_INFO(f"Starting server on {host}:{port}")
    
    # Run the application
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()