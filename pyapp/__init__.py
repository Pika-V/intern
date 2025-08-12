"""
ZX AI Framework - Python Implementation
===================================================

A Python + Langchain implementation of the original Go + Eino framework
for AI-powered data querying and analysis across multiple data sources.

Architecture:
- conf: Configuration management
- logger: Logging utilities
- dao: Data Access Objects for ES, MySQL, Nebula, Milvus
- service: Business logic and MCP services
- agent: AI agents with Langchain integration
- controller: FastAPI controllers
- utils: Utility functions
- modules: Data models and schemas
- templates: Code generation templates
- tests: Unit and integration tests

Main Features:
- Multi-database query support (ES, MySQL, Nebula, Milvus)
- MCP (Model Context Protocol) tool integration
- AI-powered natural language query processing
- Automatic code generation for data models
- RESTful API with FastAPI
"""

__version__ = "1.0.0"
__author__ = "ZX AI Framework Team"