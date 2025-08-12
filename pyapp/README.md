# ZX AI Framework - Python Implementation

A Python + Langchain implementation of the original Go + Eino framework for AI-powered data querying and analysis across multiple data sources.

## Overview

This project has been refactored from Go + Eino to Python + Langchain, providing the same functionality with improved maintainability and access to Python's rich AI ecosystem.

## Architecture

### Core Components

1. **Configuration Management (`conf/`)**
   - YAML-based configuration
   - Environment variable support
   - Database and service settings

2. **Logging (`logger/`)**
   - Structured logging with Loguru
   - File and console output
   - Configurable log levels

3. **Data Access Layer (`dao/`)**
   - MySQL support with aiomysql
   - Elasticsearch integration
   - Base DAO classes for extensibility
   - Connection pooling and management

4. **Service Layer (`service/`)**
   - Business logic implementation
   - MCP (Model Context Protocol) integration
   - Tool registration and execution
   - Service registry for management

5. **Agent Layer (`agent/`)**
   - Langchain-based AI agents
   - Tool integration and execution
   - Memory management
   - Multi-agent support

6. **Code Generation (`codebuild/`)**
   - Automatic model generation
   - Controller generation
   - MCP tool generation
   - Template-based code generation

### Key Features

- **Multi-Database Support**: MySQL, Elasticsearch, Nebula, Milvus
- **AI-Powered Querying**: Natural language to database queries
- **MCP Integration**: Model Context Protocol for tool usage
- **RESTful API**: FastAPI-based HTTP interface
- **Code Generation**: Automatic generation of models and controllers
- **Agent Framework**: Multiple specialized AI agents

## Installation

### Prerequisites

- Python 3.8+
- MySQL database
- Elasticsearch (optional)
- Redis (optional)

### Setup

1. **Clone the repository**
   ```bash
   cd pyapp
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the application**
   - Edit `conf/config.yaml` with your database credentials
   - Set up LLM API keys in the configuration

4. **Run tests**
   ```bash
   python test_implementation.py
   ```

5. **Start the application**
   ```bash
   python main.py
   ```

The application will be available at `http://localhost:8089`

## Configuration

### Main Configuration File (`conf/config.yaml`)

```yaml
Platform:
  llm_baseurl: https://api.openai.com/v1
  llm_apikey: your-api-key
  llm_deepseekid: DeepSeek-R1
  llm_qwenid: Qwen3-14B
  web_port: 8089

MySQL:
  enable: true
  username: root
  password: your-password
  address: localhost
  port: 3306
  dbName: uec

Elasticsearch:
  enable: true
  uri: http://localhost:9200
  username: elastic
  password: your-password
```

### Environment Variables

You can also configure the application using environment variables with the `ZX_` prefix:

```bash
export ZX_PLATFORM_LLM_BASEURL=https://api.openai.com/v1
export ZX_PLATFORM_LLM_APIKEY=your-api-key
export ZX_MYSQL_USERNAME=root
export ZX_MYSQL_PASSWORD=your-password
```

## API Usage

### Chat Endpoint

```bash
curl -X POST "http://localhost:8089/api/chat" \
     -H "Content-Type: application/json" \
     -d '{
       "message": "查询张三的酒店入住记录",
       "agent_name": "query_assistant_agent"
     }'
```

### Tool Execution

```bash
curl -X POST "http://localhost:8089/api/tool" \
     -H "Content-Type: application/json" \
     -d '{
       "tool_name": "query_security_hotel_info",
       "parameters": {
         "name": "张三",
         "limit": 10
       }
     }'
```

### Data Query

```bash
curl -X POST "http://localhost:8089/api/query" \
     -H "Content-Type: application/json" \
     -d '{
       "table_name": "security_hotel_info",
       "filters": {
         "name": "张三"
       },
       "limit": 10
     }'
```

## Available Agents

### Query Assistant Agent
- **Purpose**: General query assistance
- **Model**: Qwen3-14B
- **Temperature**: 0.7
- **Features**: Friendly conversation style, accurate information retrieval

### Data Analysis Agent
- **Purpose**: Data analysis and insights
- **Model**: DeepSeek-R1
- **Temperature**: 0.3
- **Features**: Analytical responses, trend analysis

## Available Tools

### ZXYC Tools
- `query_security_hotel_info`: Hotel stay information
- `query_security_person_info`: Person basic information
- `query_security_vehicle_info`: Vehicle information
- `query_security_subway_ride_info`: Subway ride records
- `query_security_ticket_info`: Scenic area tickets
- `query_security_internet_access_info`: Internet access records

### Database Tools
- MySQL query tools
- Elasticsearch search tools
- Data aggregation tools

## Code Generation

### Generate Models

```python
from codebuild import ModelBuilder
from dao import MySQLDAO

mysql_dao = MySQLDAO()
builder = ModelBuilder(mysql_dao)
await builder.build_models(app_filter="ZXJS_BCP")
```

### Generate Controllers

```python
from codebuild import ControllerBuilder
from dao import MySQLDAO

mysql_dao = MySQLDAO()
builder = ControllerBuilder(mysql_dao)
controllers = await builder.build_controllers(app_filter="ZXJS_BCP")
```

### Generate MCP Tools

```python
from codebuild import MCPToolBuilder
from dao import MySQLDAO

mysql_dao = MySQLDAO()
builder = MCPToolBuilder(mysql_dao)
tools = await builder.build_tools(app_filter="ZXJS_BCP")
```

## Development

### Project Structure

```
pyapp/
├── __init__.py              # Main package
├── main.py                  # FastAPI application
├── requirements.txt         # Dependencies
├── test_implementation.py  # Test script
├── conf/                    # Configuration
│   └── __init__.py
├── logger/                  # Logging utilities
│   └── __init__.py
├── dao/                     # Data access layer
│   ├── __init__.py
│   ├── base.py
│   ├── mysql_dao.py
│   └── elasticsearch_dao.py
├── service/                 # Business logic
│   ├── __init__.py
│   ├── base.py
│   ├── mcp_service.py
│   └── modules/
├── agent/                   # AI agents
│   ├── __init__.py
│   ├── base.py
│   └── langchain_agent.py
├── codebuild/               # Code generation
│   ├── __init__.py
│   ├── common.py
│   ├── model_builder.py
│   ├── controller_builder.py
│   └── mcp_tool_builder.py
└── logs/                    # Log files
```

### Adding New Tools

1. **Create tool function in service module**
   ```python
   @mcp_tool("query_new_data", "Query new data source")
   async def query_new_data(param1: str, param2: int = 100):
       # Tool implementation
       return results
   ```

2. **Register tool in service**
   ```python
   async def register_new_tools(mcp_service):
       mcp_service.register_tool_handler("query_new_data", query_new_data)
   ```

3. **Update service initialization**
   ```python
   # In service/__init__.py
   from .modules.new_tools import register_new_tools
   await register_new_tools(mcp_service)
   ```

### Adding New Agents

1. **Create agent class**
   ```python
   class CustomAgent(LangchainAgent):
       def __init__(self):
           config = AgentConfig(
               agent_name="custom_agent",
               model_name="your-model",
               system_prompt="Your custom prompt"
           )
           super().__init__(config)
   ```

2. **Register agent**
   ```python
   custom_agent = CustomAgent()
   agent_registry.register_agent(custom_agent)
   ```

## Testing

Run the comprehensive test suite:

```bash
python test_implementation.py
```

This will test:
- Configuration loading
- Database connections
- Service initialization
- Agent initialization
- Basic chat functionality
- Tool execution

## Deployment

### Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8089

CMD ["python", "main.py"]
```

### Production Configuration

1. **Environment Variables**: Set all sensitive data via environment variables
2. **Database**: Use production database instances
3. **Logging**: Configure appropriate log levels and retention
4. **Security**: Enable HTTPS, authentication, and authorization
5. **Monitoring**: Set up health checks and monitoring

## Migration from Go to Python

### Key Changes

1. **Language**: Go → Python
2. **AI Framework**: Eino → Langchain
3. **Web Framework**: Gin → FastAPI
4. **Database**: Native Go drivers → aiomysql, elasticsearch-py
5. **Configuration**: Viper → Pydantic Settings
6. **Logging**: Custom → Loguru
7. **Code Generation**: Go templates → Jinja2

### Benefits of Python Implementation

- **Easier Maintenance**: Python's simplicity and readability
- **Rich AI Ecosystem**: Access to Langchain, transformers, and other AI libraries
- **Faster Development**: Rapid prototyping and iteration
- **Better Tooling**: Extensive Python libraries for data science and AI
- **Community Support**: Large Python community for AI and data processing

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Check database credentials in configuration
   - Ensure database is running and accessible
   - Verify network connectivity

2. **LLM API Errors**
   - Check API key and base URL
   - Verify API rate limits
   - Ensure proper model selection

3. **Tool Execution Failures**
   - Check tool registration
   - Verify tool parameters
   - Review tool implementation

4. **Memory Issues**
   - Monitor agent memory usage
   - Clear conversation history periodically
   - Adjust memory limits as needed

### Debug Mode

Enable debug logging:

```python
# In conf/config.yaml
Platform:
  LogLevel: debug
```

Or set environment variable:

```bash
export ZX_PLATFORM_LOG_LEVEL=debug
```

## License

This project is licensed under the same terms as the original Go implementation.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## Support

For issues and questions:
- Check the troubleshooting section
- Review the test examples
- Examine the Go implementation for reference behavior
- Contact the development team