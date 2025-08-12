"""
CodeBuild Module - Controller Generation
Generates FastAPI controllers from database schema
"""

from typing import Dict, List, Any, Optional
from jinja2 import Template
import os

from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG
from ..dao.mysql_dao import MySQLDAO
from .common import (
    Field, TableInfo, safe_str_convert, safe_int_convert,
    normalize_path, snake_to_camel
)


class ControllerBuilder:
    """Builds FastAPI controllers from database schema"""
    
    def __init__(self, mysql_dao: MySQLDAO, output_dir: str = "controller"):
        self.mysql_dao = mysql_dao
        self.output_dir = output_dir
        self.controller_template = self._create_controller_template()
    
    def _create_controller_template(self) -> Template:
        """Create Jinja2 template for FastAPI controllers"""
        template_str = '''"""
{{module_name}} Controller
{{description}} API endpoints generated from database schema
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, Field

from ..dao.{{module_name}}.{{table_name}} import {{class_name}}
from ..service.{{module_name}}.{{table_name}}_service import {{class_name}}Service

router = APIRouter(prefix="/api/{{module_name}}/{{table_name_lower}}", tags=["{{table_cn_name}}"])


class {{class_name}}Response(BaseModel):
    """{{table_cn_name}} response model"""
    data: List[{{class_name}}]
    total: int
    stats: Dict[str, Any]


class {{class_name}}Service:
    """{{table_cn_name}} service layer"""
    
    def __init__(self):
        pass
    
    async def get_data(
        self,
        field_filters: Optional[Dict[str, Any]] = None,
        agg_field: Optional[str] = None,
        offset: int = 0,
        limit: int = 200
    ) -> {{class_name}}Response:
        """Get {{table_cn_name}} data with filters"""
        try:
            # This would call the actual DAO layer
            # For now, return mock data
            mock_data = []
            stats = {}
            
            return {{class_name}}Response(
                data=mock_data,
                total=len(mock_data),
                stats=stats
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


# Service instance
{{table_name_lower}}_service = {{class_name}}Service()


@router.get("/", response_model={{class_name}}Response)
async def get_{{table_name_lower}}_data(
    field_filters: Optional[str] = Query(None, description="Field filters in JSON format"),
    agg_field: Optional[str] = Query(None, description="Aggregation field"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(200, ge=1, le=1000, description="Limit for pagination")
):
    """Get {{table_cn_name}} data"""
    try:
        # Parse field filters if provided
        filters = None
        if field_filters:
            import json
            filters = json.loads(field_filters)
        
        result = await {{table_name_lower}}_service.get_data(
            field_filters=filters,
            agg_field=agg_field,
            offset=offset,
            limit=limit
        )
        
        return result
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in field_filters")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_{{table_name_lower}}(
    query: str = Query(..., description="Search query"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(50, ge=1, le=200, description="Limit for pagination")
):
    """Search {{table_cn_name}} data"""
    try:
        # Implement search logic
        return {"message": "Search endpoint to be implemented"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{record_id}")
async def get_{{table_name_lower}}_by_id(record_id: str):
    """Get {{table_cn_name}} record by ID"""
    try:
        # Implement get by ID logic
        return {"message": f"Get by ID endpoint for {record_id} to be implemented"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''
        return Template(template_str)
    
    def build_controllers(self, app_filter: str = "ZXJS_BCP") -> List[str]:
        """Build controllers for all tables"""
        # Get table definitions
        tables_data = self._get_table_definitions(app_filter)
        built_controllers = []
        
        for table_data in tables_data:
            try:
                controller_path = self._build_single_controller(table_data)
                if controller_path:
                    built_controllers.append(controller_path)
                    LOG_INFO(f"Built controller for table: {table_data.get('TableName')}")
            except Exception as e:
                LOG_ERROR(f"Error building controller for table {table_data.get('TableName')}: {e}")
        
        return built_controllers
    
    def _get_table_definitions(self, app_filter: str) -> List[Dict[str, Any]]:
        """Get table definitions from MySQL"""
        query = """
            SELECT id, TableName, TableCnName, APPID 
            FROM security_table_define 
            WHERE APPID = %s 
            ORDER BY ID
        """
        return self.mysql_dao.execute_query(query, [app_filter])
    
    def _build_single_controller(self, table_data: Dict[str, Any]) -> Optional[str]:
        """Build controller for a single table"""
        table_id = safe_int_convert(table_data.get('id'))
        table_name = safe_str_convert(table_data.get('TableName'))
        table_cn_name = safe_str_convert(table_data.get('TableCnName'))
        app_id = safe_str_convert(table_data.get('APPID'))
        
        if not all([table_id, table_name, table_cn_name, app_id]):
            LOG_ERROR(f"Invalid table data: {table_data}")
            return None
        
        # Generate controller code
        controller_code = self._generate_controller_code(
            app_id=normalize_path(app_id),
            class_name=table_name.upper(),
            table_name=table_name,
            table_cn_name=table_cn_name
        )
        
        # Write to file
        return self._write_controller_file(app_id, table_name, controller_code)
    
    def _generate_controller_code(
        self,
        app_id: str,
        class_name: str,
        table_name: str,
        table_cn_name: str
    ) -> str:
        """Generate controller code"""
        return self.controller_template.render(
            module_name=app_id.lower(),
            table_name=table_name,
            table_name_lower=table_name.lower(),
            class_name=class_name,
            table_cn_name=table_cn_name
        )
    
    def _write_controller_file(self, app_id: str, table_name: str, code: str) -> str:
        """Write controller code to file"""
        # Create directory if it doesn't exist
        controller_dir = os.path.join(self.output_dir, app_id.lower(), table_name.lower())
        os.makedirs(controller_dir, exist_ok=True)
        
        # Write file
        file_path = os.path.join(controller_dir, f"{table_name}_controller.py")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(code)
        
        LOG_DEBUG(f"Controller file written: {file_path}")
        return file_path


def main():
    """Main function for testing"""
    # This would be used for testing the controller generation
    pass


if __name__ == "__main__":
    main()