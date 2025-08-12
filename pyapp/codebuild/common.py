"""
CodeBuild Module - Common Types and Utilities
Provides common types and utility functions for code generation
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
import re


class ESType(str, Enum):
    """Elasticsearch field types"""
    TEXT = "text"
    KEYWORD = "keyword"
    LONG = "long"
    INTEGER = "integer"
    DATE = "date"
    BOOLEAN = "boolean"
    FLOAT = "float"
    DOUBLE = "double"
    NESTED = "nested"
    OBJECT = "object"


@dataclass
class Field:
    """Represents a struct/class field"""
    name: str
    type: str
    es_type: str
    description: str = ""
    required: bool = False
    default: Any = None


@dataclass
class TableInfo:
    """Represents table information"""
    id: int
    table_name: str
    table_cn_name: str
    app_id: str
    fields: List[Field]


@dataclass
class MappingResponse:
    """Represents Elasticsearch mapping response"""
    properties: Dict[str, Dict[str, Any]]


def map_es_type_to_python(es_type: str) -> str:
    """Map Elasticsearch field types to Python types"""
    type_mapping = {
        ESType.TEXT: "str",
        ESType.KEYWORD: "str",
        ESType.LONG: "int",
        ESType.INTEGER: "int",
        ESType.DATE: "datetime.datetime",
        ESType.BOOLEAN: "bool",
        ESType.FLOAT: "float",
        ESType.DOUBLE: "float",
        ESType.NESTED: "Dict[str, Any]",
        ESType.OBJECT: "Dict[str, Any]",
    }
    return type_mapping.get(es_type, "Any")


def map_es_type_to_pydantic(es_type: str) -> str:
    """Map Elasticsearch field types to Pydantic types"""
    type_mapping = {
        ESType.TEXT: "Optional[str]",
        ESType.KEYWORD: "Optional[str]",
        ESType.LONG: "Optional[int]",
        ESType.INTEGER: "Optional[int]",
        ESType.DATE: "Optional[datetime.datetime]",
        ESType.BOOLEAN: "Optional[bool]",
        ESType.FLOAT: "Optional[float]",
        ESType.DOUBLE: "Optional[float]",
        ESType.NESTED: "Optional[Dict[str, Any]]",
        ESType.OBJECT: "Optional[Dict[str, Any]]",
    }
    return type_mapping.get(es_type, "Any")


def capitalize_first(s: str) -> str:
    """Capitalize first letter of string"""
    if not s:
        return s
    return s[0].upper() + s[1:]


def snake_to_camel(s: str) -> str:
    """Convert snake_case to camelCase"""
    components = s.split('_')
    return components[0] + ''.join(x.capitalize() for x in components[1:])


def camel_to_snake(s: str) -> str:
    """Convert camelCase to snake_case"""
    s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()


def normalize_path(app_id: str) -> str:
    """Normalize path for app ID"""
    return app_id.upper()


def safe_str_convert(value: Any) -> Optional[str]:
    """Safely convert value to string"""
    if value is None:
        return None
    try:
        return str(value)
    except (ValueError, TypeError):
        return None


def safe_int_convert(value: Any) -> Optional[int]:
    """Safely convert value to int"""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_float_convert(value: Any) -> Optional[float]:
    """Safely convert value to float"""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_mapping_response(mapping_data: Dict[str, Any]) -> MappingResponse:
    """Parse Elasticsearch mapping response"""
    properties = {}
    
    for index_name, index_data in mapping_data.items():
        if 'mappings' in index_data and 'properties' in index_data['mappings']:
            properties = index_data['mappings']['properties']
            break
    
    return MappingResponse(properties=properties)


def extract_fields_from_mapping(
    mapping_response: MappingResponse,
    column_fields: List[Dict[str, Any]]
) -> List[Field]:
    """Extract fields from mapping response"""
    fields = []
    
    for field_name, field_props in mapping_response.properties.items():
        es_type = field_props.get('type', 'text')
        
        # Find Chinese description
        description = ""
        for col_field in column_fields:
            col_name = safe_str_convert(col_field.get('ColName'))
            if col_name and col_name.lower() == field_name.lower():
                description = safe_str_convert(col_field.get('ColCnName')) or ""
                break
        
        # Create field
        field = Field(
            name=snake_to_camel(field_name),
            type=map_es_type_to_python(es_type),
            es_type=es_type,
            description=description
        )
        fields.append(field)
    
    return fields