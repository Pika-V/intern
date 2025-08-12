"""
CodeBuild Module - Model Generation
Generates Pydantic models from Elasticsearch mappings
"""

from typing import Dict, List, Any, Optional
from jinja2 import Template
import os
import re
from datetime import datetime

from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG
from ..dao.mysql_dao import MySQLDAO
from .common import (
    Field, TableInfo, MappingResponse, parse_mapping_response,
    extract_fields_from_mapping, snake_to_camel, capitalize_first,
    normalize_path, safe_str_convert, safe_int_convert
)


class ModelBuilder:
    """Builds Pydantic models from database schema"""
    
    def __init__(self, mysql_dao: MySQLDAO, output_dir: str = "modules"):
        self.mysql_dao = mysql_dao
        self.output_dir = output_dir
        self.model_template = self._create_model_template()
    
    def _create_model_template(self) -> Template:
        """Create Jinja2 template for Pydantic models"""
        template_str = '''"""
{{module_name}} Module
{{description}} models generated from Elasticsearch mapping
"""

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime
import json


class {{class_name}}(BaseModel):
    """{{table_cn_name}} model"""
    
    {{- for field in fields }}
    {{field.name}}: {{field.type}} = Field(
        default={{field.default if field.default is not none else 'None'}},
        description="{{field.description}}",
        alias="{{field.alias}}"
    )
    {{- end }}
    
    class Config:
        """Pydantic configuration"""
        populate_by_name = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self.model_dump(by_alias=True)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, default=str)
    
    @classmethod
    def from_es_hit(cls, hit: Dict[str, Any]) -> "{{class_name}}":
        """Create instance from Elasticsearch hit"""
        source = hit.get('_source', {})
        return cls(**source)
'''
        return Template(template_str)
    
    def build_models(self, app_filter: str = "ZXJS_BCP") -> List[TableInfo]:
        """Build models for all tables"""
        # Get table definitions
        tables_data = self._get_table_definitions(app_filter)
        table_infos = []
        
        for table_data in tables_data:
            try:
                table_info = self._build_single_model(table_data)
                if table_info:
                    table_infos.append(table_info)
                    LOG_INFO(f"Built model for table: {table_info.table_name}")
            except Exception as e:
                LOG_ERROR(f"Error building model for table {table_data.get('TableName')}: {e}")
        
        return table_infos
    
    def _get_table_definitions(self, app_filter: str) -> List[Dict[str, Any]]:
        """Get table definitions from MySQL"""
        query = """
            SELECT id, TableName, TableCnName, APPID 
            FROM security_table_define 
            WHERE APPID = %s 
            ORDER BY ID
        """
        return self.mysql_dao.execute_query(query, [app_filter])
    
    def _build_single_model(self, table_data: Dict[str, Any]) -> Optional[TableInfo]:
        """Build model for a single table"""
        table_id = safe_int_convert(table_data.get('id'))
        table_name = safe_str_convert(table_data.get('TableName'))
        table_cn_name = safe_str_convert(table_data.get('TableCnName'))
        app_id = safe_str_convert(table_data.get('APPID'))
        
        if not all([table_id, table_name, table_cn_name, app_id]):
            LOG_ERROR(f"Invalid table data: {table_data}")
            return None
        
        # Get column definitions
        column_fields = self._get_column_definitions(table_id)
        
        # Get Elasticsearch mapping (simulated - in real implementation would call ES)
        mapping_response = self._get_es_mapping(table_name)
        
        # Extract fields
        fields = extract_fields_from_mapping(mapping_response, column_fields)
        
        # Generate model code
        model_code = self._generate_model_code(
            app_id=normalize_path(app_id),
            class_name=table_name.upper(),
            table_cn_name=table_cn_name,
            fields=fields
        )
        
        # Write to file
        self._write_model_file(app_id, table_name, model_code)
        
        return TableInfo(
            id=table_id,
            table_name=table_name,
            table_cn_name=table_cn_name,
            app_id=app_id,
            fields=fields
        )
    
    def _get_column_definitions(self, table_id: int) -> List[Dict[str, Any]]:
        """Get column definitions from MySQL"""
        query = """
            SELECT ColName, ColCnName 
            FROM security_tablecolumn_define 
            WHERE tableid = %s
        """
        return self.mysql_dao.execute_query(query, [table_id])
    
    def _get_es_mapping(self, index_name: str) -> MappingResponse:
        """Get Elasticsearch mapping (simulated)"""
        # In real implementation, this would call Elasticsearch
        # For now, return a mock mapping
        mock_mapping = {
            index_name: {
                "mappings": {
                    "properties": {
                        "id": {"type": "long"},
                        "name": {"type": "keyword"},
                        "create_time": {"type": "date"},
                        "update_time": {"type": "date"}
                    }
                }
            }
        }
        return parse_mapping_response(mock_mapping)
    
    def _generate_model_code(
        self,
        app_id: str,
        class_name: str,
        table_cn_name: str,
        fields: List[Field]
    ) -> str:
        """Generate model code"""
        # Prepare field data for template
        template_fields = []
        for field in fields:
            template_field = {
                'name': snake_to_camel(field.name),
                'type': field.type,
                'description': field.description,
                'alias': field.name,
                'default': None
            }
            template_fields.append(template_field)
        
        # Render template
        return self.model_template.render(
            module_name=app_id.lower(),
            description=table_cn_name,
            class_name=class_name,
            fields=template_fields
        )
    
    def _write_model_file(self, app_id: str, table_name: str, code: str):
        """Write model code to file"""
        # Create directory if it doesn't exist
        module_dir = os.path.join(self.output_dir, app_id.lower())
        os.makedirs(module_dir, exist_ok=True)
        
        # Write file
        file_path = os.path.join(module_dir, f"{table_name}.py")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(code)
        
        LOG_DEBUG(f"Model file written: {file_path}")


def main():
    """Main function for testing"""
    # This would be used for testing the model generation
    pass


if __name__ == "__main__":
    main()