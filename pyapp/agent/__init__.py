"""
Agent Layer - Main Module
Provides AI agent implementations with Langchain integration
"""

from .base import (
    BaseAgent,
    ToolAgent,
    MemoryAgent,
    AgentConfig,
    AgentMessage,
    AgentResponse,
    ToolCall,
    AgentRegistry,
    agent_registry
)
from .langchain_agent import (
    LangchainAgent,
    DataAnalysisAgent,
    QueryAssistantAgent,
    data_analysis_agent,
    query_assistant_agent
)

# Export main classes and functions
__all__ = [
    # Base classes
    'BaseAgent',
    'ToolAgent',
    'MemoryAgent',
    'AgentConfig',
    'AgentMessage',
    'AgentResponse',
    'ToolCall',
    
    # Registry
    'AgentRegistry',
    'agent_registry',
    
    # Langchain implementations
    'LangchainAgent',
    'DataAnalysisAgent',
    'QueryAssistantAgent',
    'data_analysis_agent',
    'query_assistant_agent'
]


async def initialize_agents() -> bool:
    """Initialize all agents"""
    try:
        success = await agent_registry.initialize_all()
        return success
    except Exception as e:
        from ..logger import LOG_ERROR
        LOG_ERROR(f"Failed to initialize agents: {e}")
        return False


async def shutdown_agents() -> bool:
    """Shutdown all agents"""
    try:
        return await agent_registry.shutdown_all()
    except Exception as e:
        from ..logger import LOG_ERROR
        LOG_ERROR(f"Failed to shutdown agents: {e}")
        return False


def get_agent(agent_name: str) -> BaseAgent:
    """Get agent by name"""
    return agent_registry.get_agent(agent_name)


async def process_message(
    message: str,
    agent_name: str = "query_assistant_agent",
    context: Optional[Dict[str, Any]] = None
) -> AgentResponse:
    """Process message with specified agent"""
    agent = get_agent(agent_name)
    if not agent:
        raise ValueError(f"Agent not found: {agent_name}")
    
    return await agent.process_message(message, context)