"""
Service Layer - ZXYC MCP Tools
Provides ZXYC-specific MCP tools implementation
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import json

from .base import DataService, ServiceResult, ServiceConfig, QueryFilter
from .mcp_service import mcp_tool
from ..dao import get_dao, search_by_keywords
from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG


class ZXYCDataService(DataService):
    """ZXYC data service"""
    
    def __init__(self):
        config = ServiceConfig(
            service_name="zxyc_data_service",
            version="1.0.0",
            enabled=True
        )
        super().__init__(config, "elasticsearch")


# Global service instance
zxyc_service = ZXYCDataService()


@mcp_tool("query_security_hotel_info", "Query hotel stay information")
async def query_security_hotel_info(
    name: Optional[str] = None,
    id_card: Optional[str] = None,
    hotel_name: Optional[str] = None,
    check_in_time: Optional[str] = None,
    check_out_time: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Query hotel stay information"""
    try:
        # Build filters
        filters = []
        
        if name:
            filters.append(QueryFilter("name", "like", name))
        if id_card:
            filters.append(QueryFilter("id_card", "eq", id_card))
        if hotel_name:
            filters.append(QueryFilter("hotel_name", "like", hotel_name))
        if check_in_time:
            filters.append(QueryFilter("check_in_time", "gte", check_in_time))
        if check_out_time:
            filters.append(QueryFilter("check_out_time", "lte", check_out_time))
        
        # Search data
        result = await zxyc_service.search_data(
            "security_hotel_info",
            filters,
            pagination={"offset": offset, "limit": limit}
        )
        
        return result.items
    except Exception as e:
        LOG_ERROR(f"Failed to query hotel info: {e}")
        raise


@mcp_tool("query_security_person_info", "Query person basic information")
async def query_security_person_info(
    name: Optional[str] = None,
    id_card: Optional[str] = None,
    gender: Optional[str] = None,
    age_min: Optional[int] = None,
    age_max: Optional[int] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Query person basic information"""
    try:
        # Build filters
        filters = []
        
        if name:
            filters.append(QueryFilter("name", "like", name))
        if id_card:
            filters.append(QueryFilter("id_card", "eq", id_card))
        if gender:
            filters.append(QueryFilter("gender", "eq", gender))
        if age_min is not None:
            filters.append(QueryFilter("age", "gte", age_min))
        if age_max is not None:
            filters.append(QueryFilter("age", "lte", age_max))
        
        # Search data
        result = await zxyc_service.search_data(
            "security_person_info",
            filters,
            pagination={"offset": offset, "limit": limit}
        )
        
        return result.items
    except Exception as e:
        LOG_ERROR(f"Failed to query person info: {e}")
        raise


@mcp_tool("query_security_vehicle_info", "Query vehicle basic information")
async def query_security_vehicle_info(
    plate_number: Optional[str] = None,
    vehicle_type: Optional[str] = None,
    vehicle_color: Optional[str] = None,
    owner_name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Query vehicle basic information"""
    try:
        # Build filters
        filters = []
        
        if plate_number:
            filters.append(QueryFilter("plate_number", "like", plate_number))
        if vehicle_type:
            filters.append(QueryFilter("vehicle_type", "eq", vehicle_type))
        if vehicle_color:
            filters.append(QueryFilter("vehicle_color", "eq", vehicle_color))
        if owner_name:
            filters.append(QueryFilter("owner_name", "like", owner_name))
        
        # Search data
        result = await zxyc_service.search_data(
            "security_vehicle_info",
            filters,
            pagination={"offset": offset, "limit": limit}
        )
        
        return result.items
    except Exception as e:
        LOG_ERROR(f"Failed to query vehicle info: {e}")
        raise


@mcp_tool("query_security_subway_ride_info", "Query subway ride information")
async def query_security_subway_ride_info(
    person_name: Optional[str] = None,
    id_card: Optional[str] = None,
    ride_time: Optional[str] = None,
    station_name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Query subway ride information"""
    try:
        # Build filters
        filters = []
        
        if person_name:
            filters.append(QueryFilter("person_name", "like", person_name))
        if id_card:
            filters.append(QueryFilter("id_card", "eq", id_card))
        if ride_time:
            filters.append(QueryFilter("ride_time", "gte", ride_time))
        if station_name:
            filters.append(QueryFilter("station_name", "like", station_name))
        
        # Search data
        result = await zxyc_service.search_data(
            "security_subway_ride_info",
            filters,
            pagination={"offset": offset, "limit": limit}
        )
        
        return result.items
    except Exception as e:
        LOG_ERROR(f"Failed to query subway ride info: {e}")
        raise


@mcp_tool("query_security_ticket_info", "Query scenic area ticket information")
async def query_security_ticket_info(
    person_name: Optional[str] = None,
    id_card: Optional[str] = None,
    scenic_area: Optional[str] = None,
    visit_time: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Query scenic area ticket information"""
    try:
        # Build filters
        filters = []
        
        if person_name:
            filters.append(QueryFilter("person_name", "like", person_name))
        if id_card:
            filters.append(QueryFilter("id_card", "eq", id_card))
        if scenic_area:
            filters.append(QueryFilter("scenic_area", "like", scenic_area))
        if visit_time:
            filters.append(QueryFilter("visit_time", "gte", visit_time))
        
        # Search data
        result = await zxyc_service.search_data(
            "security_ticket_info",
            filters,
            pagination={"offset": offset, "limit": limit}
        )
        
        return result.items
    except Exception as e:
        LOG_ERROR(f"Failed to query ticket info: {e}")
        raise


@mcp_tool("query_security_internet_access_info", "Query internet access information")
async def query_security_internet_access_info(
    person_name: Optional[str] = None,
    id_card: Optional[str] = None,
    internet_bar: Optional[str] = None,
    access_time: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Query internet access information"""
    try:
        # Build filters
        filters = []
        
        if person_name:
            filters.append(QueryFilter("person_name", "like", person_name))
        if id_card:
            filters.append(QueryFilter("id_card", "eq", id_card))
        if internet_bar:
            filters.append(QueryFilter("internet_bar_name", "like", internet_bar))
        if access_time:
            filters.append(QueryFilter("access_time", "gte", access_time))
        
        # Search data
        result = await zxyc_service.search_data(
            "security_internet_access_info",
            filters,
            pagination={"offset": offset, "limit": limit}
        )
        
        return result.items
    except Exception as e:
        LOG_ERROR(f"Failed to query internet access info: {e}")
        raise


async def register_zxyc_tools(mcp_service):
    """Register all ZXYC tools with MCP service"""
    try:
        # Register all decorated tools
        import inspect
        
        # Get all functions with _mcp_tool_name attribute
        for name, obj in globals().items():
            if inspect.isfunction(obj) and hasattr(obj, '_mcp_tool_name'):
                tool_name = getattr(obj, '_mcp_tool_name')
                description = getattr(obj, '_mcp_tool_description', '')
                
                # Register tool handler
                mcp_service.register_tool_handler(tool_name, obj)
                LOG_INFO(f"Registered ZXYC tool: {tool_name}")
        
        # Also register the service
        from .base import service_registry
        service_registry.register_service(zxyc_service)
        
    except Exception as e:
        LOG_ERROR(f"Failed to register ZXYC tools: {e}")
        raise


# Additional ZXYC tools can be added here following the same pattern
# Each tool should be decorated with @mcp_tool and follow the same structure