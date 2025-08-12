"""
CodeBuild Module - MCP Tool Generation
Generates MCP (Model Context Protocol) tools from database schema
"""

from typing import Dict, List, Any, Optional
from jinja2 import Template
import os

from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG
from ..dao.mysql_dao import MySQLDAO
from .common import (
    Field, TableInfo, safe_str_convert, safe_int_convert,
    normalize_path, snake_to_camel, camel_to_snake
)


class MCPToolBuilder:
    """Builds MCP tools from database schema"""
    
    def __init__(self, mysql_dao: MySQLDAO, output_dir: str = "service/mcptools"):
        self.mysql_dao = mysql_dao
        self.output_dir = output_dir
        self.tool_template = self._create_tool_template()
    
    def _create_tool_template(self) -> Template:
        """Create Jinja2 template for MCP tools"""
        template_str = '''"""
{{module_name}} MCP Tools
{{description}} MCP tools generated from database schema
"""

from typing import List, Optional, Dict, Any
from mcp import Tool
from pydantic import BaseModel, Field

from ..dao.{{module_name}}.{{table_name}} import {{class_name}}


class {{class_name}}SearchParams(BaseModel):
    """{{table_cn_name}} search parameters"""
    
    {{- for field in fields }}
    {{field.name}}: Optional[{{field.type}}] = Field(
        default=None,
        description="{{field.description}}"
    )
    {{- end }}
    
    limit: Optional[int] = Field(
        default=100,
        description="Maximum number of results to return"
    )
    
    offset: Optional[int] = Field(
        default=0,
        description="Offset for pagination"
    )


class {{class_name}}Tool:
    """{{table_cn_name}} MCP tool"""
    
    def __init__(self):
        self.name = "query_{{table_name_lower}}"
        self.description = "Query {{table_cn_name}} data with various filters"
    
    async def search_{{table_name_lower}}(
        self,
        {{- for field in fields }}
        {{field.name}}: Optional[{{field.type}}] = None,
        {{- end }}
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Search {{table_cn_name}} data"""
        try:
            # Build query parameters
            params = {}
            {{- for field in fields }}
            if {{field.name}} is not None:
                params["{{field.original_name}}"] = {{field.name}}
            {{- end }}
            
            # This would call the actual DAO layer
            # For now, return mock data
            mock_results = []
            
            return mock_results
        except Exception as e:
            raise Exception(f"Error searching {{table_cn_name}}: {str(e)}")
    
    def get_tools(self) -> List[Tool]:
        """Get MCP tools"""
        return [
            Tool(
                name=self.name,
                description=self.description,
                inputSchema={
                    "type": "object",
                    "properties": {
                        {{- for field in fields }}
                        "{{field.original_name}}": {
                            "type": "{{field.json_type}}",
                            "description": "{{field.description}}"
                        },
                        {{- end }}
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of results"
                        },
                        "offset": {
                            "type": "integer", 
                            "description": "Offset for pagination"
                        }
                    },
                    "required": []
                }
            )
        ]


# Tool instance
{{table_name_lower}}_tool = {{class_name}}Tool()


async def query_{{table_name_lower}}(
    {{- for field in fields }}
    {{field.name}}: Optional[{{field.type}}] = None,
    {{- end }}
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Query {{table_cn_name}} data"""
    return await {{table_name_lower}}_tool.search_{{table_name_lower}}(
        {{- for field in fields }}
        {{field.name}}={{field.name}},
        {{- end }}
        limit=limit,
        offset=offset
    )


# Export tools
TOOLS = {{table_name_lower}}_tool.get_tools()
FUNCTIONS = [query_{{table_name_lower}}]
'''
        return Template(template_str)
    
    def build_tools(self, app_filter: str = "ZXJS_BCP") -> List[str]:
        """Build MCP tools for all tables"""
        # Get table definitions
        tables_data = self._get_table_definitions(app_filter)
        built_tools = []
        
        for table_data in tables_data:
            try:
                tool_path = self._build_single_tool(table_data)
                if tool_path:
                    built_tools.append(tool_path)
                    LOG_INFO(f"Built MCP tool for table: {table_data.get('TableName')}")
            except Exception as e:
                LOG_ERROR(f"Error building MCP tool for table {table_data.get('TableName')}: {e}")
        
        return built_tools
    
    def _get_table_definitions(self, app_filter: str) -> List[Dict[str, Any]]:
        """Get table definitions from MySQL"""
        query = """
            SELECT id, TableName, TableCnName, APPID 
            FROM security_table_define 
            WHERE APPID = %s 
            ORDER BY ID
        """
        return self.mysql_dao.execute_query(query, [app_filter])
    
    def _build_single_tool(self, table_data: Dict[str, Any]) -> Optional[str]:
        """Build MCP tool for a single table"""
        table_id = safe_int_convert(table_data.get('id'))
        table_name = safe_str_convert(table_data.get('TableName'))
        table_cn_name = safe_str_convert(table_data.get('TableCnName'))
        app_id = safe_str_convert(table_data.get('APPID'))
        
        if not all([table_id, table_name, table_cn_name, app_id]):
            LOG_ERROR(f"Invalid table data: {table_data}")
            return None
        
        # Get column definitions
        column_fields = self._get_column_definitions(table_id)
        
        # Generate tool code
        tool_code = self._generate_tool_code(
            app_id=normalize_path(app_id),
            class_name=table_name.upper(),
            table_name=table_name,
            table_cn_name=table_cn_name,
            column_fields=column_fields
        )
        
        # Write to file
        return self._write_tool_file(app_id, table_name, tool_code)
    
    def _get_column_definitions(self, table_id: int) -> List[Dict[str, Any]]:
        """Get column definitions from MySQL"""
        query = """
            SELECT ColName, ColCnName 
            FROM security_tablecolumn_define 
            WHERE tableid = %s
        """
        return self.mysql_dao.execute_query(query, [table_id])
    
    def _generate_tool_code(
        self,
        app_id: str,
        class_name: str,
        table_name: str,
        table_cn_name: str,
        column_fields: List[Dict[str, Any]]
    ) -> str:
        """Generate tool code"""
        # Prepare field data for template
        template_fields = []
        for col_field in column_fields:
            col_name = safe_str_convert(col_field.get('ColName'))
            col_cn_name = safe_str_convert(col_field.get('ColCnName'))
            
            if col_name:
                template_field = {
                    'name': snake_to_camel(col_name),
                    'original_name': col_name,
                    'type': 'str',  # Default to str, can be enhanced
                    'json_type': 'string',
                    'description': col_cn_name or col_name
                }
                template_fields.append(template_field)
        
        return self.tool_template.render(
            module_name=app_id.lower(),
            table_name=table_name,
            table_name_lower=table_name.lower(),
            class_name=class_name,
            table_cn_name=table_cn_name,
            fields=template_fields
        )
    
    def _write_tool_file(self, app_id: str, table_name: str, code: str) -> str:
        """Write tool code to file"""
        # Create directory if it doesn't exist
        tool_dir = os.path.join(self.output_dir, app_id.lower())
        os.makedirs(tool_dir, exist_ok=True)
        
        # Write file
        file_path = os.path.join(tool_dir, f"{table_name}_tools.py")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(code)
        
        LOG_DEBUG(f"MCP tool file written: {file_path}")
        return file_path


def main():
    """Main function for testing"""
    # This would be used for testing the MCP tool generation
    pass


if __name__ == "__main__":
    main()