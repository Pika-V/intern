"""
Configuration Management Module
Handles loading and accessing configuration settings from YAML files
"""

import os
import yaml
from typing import Optional, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field


class DatabaseConfig(BaseSettings):
    """Database configuration settings"""
    enable: bool = True
    username: str = ""
    password: str = ""
    address: str = ""
    port: int = 0
    dbname: str = ""
    charset: str = "utf8"


class PlatformConfig(BaseSettings):
    """Platform configuration settings"""
    llm_baseurl: str = "https://api.openai.com/v1"
    llm_apikey: str = ""
    llm_deepseekid: str = "DeepSeek-R1"
    llm_qwenid: str = "Qwen3-14B"
    log_mode: str = "file"
    log_level: str = "debug"
    web_port: int = 8089


class McpToolConfig(BaseSettings):
    """MCP tool configuration"""
    mtype: str = "sse"
    sse_url: str = ""
    stdio_command: str = "uvx"
    stdio_env: str = ""
    stdio_args: str = ""


class GatewayConfig(BaseSettings):
    """Gateway configuration"""
    username: str = ""
    password: str = ""
    enable: bool = True
    gatwayurl: str = ""
    gatwayurl_1400: str = ""
    gatwayurl_everything: str = ""


class Settings(BaseSettings):
    """Main settings class"""
    platform: PlatformConfig = Field(default_factory=PlatformConfig)
    mcp_tool: McpToolConfig = Field(default_factory=McpToolConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    es: DatabaseConfig = Field(default_factory=DatabaseConfig)
    mysql: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: DatabaseConfig = Field(default_factory=DatabaseConfig)
    nebula: DatabaseConfig = Field(default_factory=DatabaseConfig)
    milvus: DatabaseConfig = Field(default_factory=DatabaseConfig)

    class Config:
        env_file = ".env"
        env_prefix = "ZX_"
        case_sensitive = False


def load_config(config_path: str = "conf/config.yaml") -> Settings:
    """Load configuration from YAML file"""
    if not os.path.exists(config_path):
        # Create default config
        config_data = {
            "Platform": {
                "llm_baseurl": "https://www.sophnet.com/api/open-apis",
                "llm_apikey": "",
                "llm_deepseekid": "DeepSeek-R1",
                "llm_qwenid": "Qwen3-14B",
                "LogMode": "file",
                "LogLevel": "debug",
                "webPort": 8089
            },
            "McpTool": {
                "mtype": "sse",
                "sse_url": "",
                "stdio_command": "uvx",
                "stdio_env": "",
                "stdio_args": ""
            },
            "Gateway": {
                "username": "admin",
                "password": "Aa123456",
                "enable": True,
                "gatwayurl": "",
                "gatwayurl_1400": "",
                "gatwayurl_everything": ""
            },
            "ES": {
                "enable": True,
                "uri": "",
                "username": "elastic",
                "password": ""
            },
            "MySQL": {
                "enable": True,
                "username": "root",
                "password": "",
                "address": "",
                "port": 3306,
                "dbName": "uec",
                "charset": "utf8"
            },
            "Redis": {
                "enable": True,
                "password": "",
                "address": "",
                "port": 6379,
                "dbName": "0"
            },
            "Nebula": {
                "enable": True,
                "username": "root",
                "password": "",
                "address": "",
                "port": 9669,
                "spacename": "relations"
            },
            "Milvus": {
                "enable": False,
                "username": "root",
                "password": "",
                "address": "",
                "port": 19530,
                "dbname": "default"
            }
        }
        
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)
    
    return Settings(**config_data)


# Global settings instance
settings = load_config()