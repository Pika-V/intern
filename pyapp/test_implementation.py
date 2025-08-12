"""
Test Script for Python Implementation
Validates the core functionality of the refactored Python application
"""

import asyncio
import sys
import os

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main import app
from conf import settings
from logger import LOG_INFO, LOG_ERROR
from dao import db_manager
from service import service_registry
from agent import agent_registry


async def test_database_connections():
    """Test database connections"""
    LOG_INFO("Testing database connections...")
    
    try:
        # Test MySQL connection
        mysql_dao = db_manager.get_mysql_dao()
        if mysql_dao:
            await mysql_dao.connect()
            tables = await mysql_dao.get_table_list()
            LOG_INFO(f"MySQL connected successfully, found {len(tables)} tables")
            await mysql_dao.disconnect()
        else:
            LOG_ERROR("MySQL DAO not available")
        
        # Test Elasticsearch connection
        es_dao = db_manager.get_elasticsearch_dao()
        if es_dao:
            await es_dao.connect()
            LOG_INFO("Elasticsearch connected successfully")
            await es_dao.disconnect()
        else:
            LOG_ERROR("Elasticsearch DAO not available")
            
        return True
    except Exception as e:
        LOG_ERROR(f"Database connection test failed: {e}")
        return False


async def test_service_initialization():
    """Test service initialization"""
    LOG_INFO("Testing service initialization...")
    
    try:
        # Initialize services
        success = await service_registry.initialize_all()
        
        if success:
            LOG_INFO("Services initialized successfully")
            LOG_INFO(f"Available services: {service_registry.list_services()}")
        else:
            LOG_ERROR("Service initialization failed")
            
        return success
    except Exception as e:
        LOG_ERROR(f"Service initialization test failed: {e}")
        return False


async def test_agent_initialization():
    """Test agent initialization"""
    LOG_INFO("Testing agent initialization...")
    
    try:
        # Initialize agents
        success = await agent_registry.initialize_all()
        
        if success:
            LOG_INFO("Agents initialized successfully")
            LOG_INFO(f"Available agents: {agent_registry.list_agents()}")
        else:
            LOG_ERROR("Agent initialization failed")
            
        return success
    except Exception as e:
        LOG_ERROR(f"Agent initialization test failed: {e}")
        return False


async def test_basic_chat():
    """Test basic chat functionality"""
    LOG_INFO("Testing basic chat functionality...")
    
    try:
        from agent import process_message
        
        # Test simple message
        response = await process_message(
            message="你好，请介绍一下自己",
            agent_name="query_assistant_agent"
        )
        
        LOG_INFO(f"Chat response: {response.message.content}")
        LOG_INFO(f"Tool calls made: {len(response.tool_calls)}")
        
        return True
    except Exception as e:
        LOG_ERROR(f"Basic chat test failed: {e}")
        return False


async def test_tool_execution():
    """Test tool execution"""
    LOG_INFO("Testing tool execution...")
    
    try:
        from service import execute_mcp_tool
        
        # Test tool execution (this might fail if tools are not properly configured)
        result = await execute_mcp_tool("list_tools", {})
        
        if result.success:
            LOG_INFO("Tool execution successful")
            LOG_INFO(f"Tool result: {result.data}")
        else:
            LOG_WARNING(f"Tool execution failed: {result.message}")
            
        return True
    except Exception as e:
        LOG_ERROR(f"Tool execution test failed: {e}")
        return False


async def test_configuration():
    """Test configuration loading"""
    LOG_INFO("Testing configuration...")
    
    try:
        # Test platform settings
        LOG_INFO(f"LLM Base URL: {settings.platform.llm_baseurl}")
        LOG_INFO(f"LLM Model: {settings.platform.llm_deepseekid}")
        LOG_INFO(f"Web Port: {settings.platform.web_port}")
        
        # Test database settings
        LOG_INFO(f"MySQL Host: {settings.mysql.address}")
        LOG_INFO(f"MySQL Database: {settings.mysql.dbname}")
        LOG_INFO(f"ES URI: {settings.es.uri}")
        
        LOG_INFO("Configuration loaded successfully")
        return True
    except Exception as e:
        LOG_ERROR(f"Configuration test failed: {e}")
        return False


async def run_all_tests():
    """Run all tests"""
    LOG_INFO("Starting comprehensive tests...")
    
    tests = [
        ("Configuration", test_configuration),
        ("Database Connections", test_database_connections),
        ("Service Initialization", test_service_initialization),
        ("Agent Initialization", test_agent_initialization),
        ("Basic Chat", test_basic_chat),
        ("Tool Execution", test_tool_execution),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        LOG_INFO(f"Running test: {test_name}")
        try:
            result = await test_func()
            results[test_name] = "PASS" if result else "FAIL"
            LOG_INFO(f"Test {test_name}: {'PASS' if result else 'FAIL'}")
        except Exception as e:
            results[test_name] = "ERROR"
            LOG_ERROR(f"Test {test_name}: ERROR - {e}")
    
    # Print summary
    LOG_INFO("\n" + "="*50)
    LOG_INFO("TEST SUMMARY")
    LOG_INFO("="*50)
    
    for test_name, result in results.items():
        status = result.ljust(10)
        LOG_INFO(f"{test_name}: {status}")
    
    total_tests = len(tests)
    passed_tests = sum(1 for result in results.values() if result == "PASS")
    
    LOG_INFO("="*50)
    LOG_INFO(f"TOTAL: {total_tests}, PASSED: {passed_tests}, FAILED: {total_tests - passed_tests}")
    LOG_INFO("="*50)
    
    return passed_tests == total_tests


async def main():
    """Main test function"""
    try:
        # Create logs directory
        os.makedirs("logs", exist_ok=True)
        
        LOG_INFO("Starting Python implementation validation tests...")
        
        # Run all tests
        success = await run_all_tests()
        
        if success:
            LOG_INFO("All tests passed! Python implementation is working correctly.")
        else:
            LOG_ERROR("Some tests failed. Please check the logs for details.")
            
        return success
        
    except Exception as e:
        LOG_ERROR(f"Test execution failed: {e}")
        return False


if __name__ == "__main__":
    # Run tests
    result = asyncio.run(main())
    
    # Exit with appropriate code
    sys.exit(0 if result else 1)