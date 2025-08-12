"""
Agent Layer - Base Classes and Interfaces
Provides base agent classes for AI-powered functionality
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, AsyncIterator
from dataclasses import dataclass
from datetime import datetime
import json
import asyncio

from ..service.base import ServiceResult
from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG


@dataclass
class AgentMessage:
    """Agent message structure"""
    role: str  # system, user, assistant, tool
    content: str
    metadata: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None


@dataclass
class AgentConfig:
    """Agent configuration"""
    agent_name: str
    model_name: str
    temperature: float = 0.7
    max_tokens: int = 2000
    enable_tools: bool = True
    enable_memory: bool = True
    system_prompt: Optional[str] = None


@dataclass
class ToolCall:
    """Tool call structure"""
    tool_name: str
    parameters: Dict[str, Any]
    call_id: Optional[str] = None


@dataclass
class AgentResponse:
    """Agent response structure"""
    message: AgentMessage
    tool_calls: List[ToolCall]
    thinking_process: Optional[str] = None
    usage_stats: Optional[Dict[str, Any]] = None


class BaseAgent(ABC):
    """Base agent class"""
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self.memory: List[AgentMessage] = []
        self.tools: Dict[str, Any] = {}
        self._initialized = False
    
    async def initialize(self) -> bool:
        """Initialize agent"""
        try:
            await self._on_initialize()
            self._initialized = True
            LOG_INFO(f"Agent {self.config.agent_name} initialized successfully")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to initialize agent {self.config.agent_name}: {e}")
            return False
    
    async def shutdown(self) -> bool:
        """Shutdown agent"""
        try:
            await self._on_shutdown()
            self._initialized = False
            LOG_INFO(f"Agent {self.config.agent_name} shutdown successfully")
            return True
        except Exception as e:
            LOG_ERROR(f"Failed to shutdown agent {self.config.agent_name}: {e}")
            return False
    
    @abstractmethod
    async def _on_initialize(self) -> None:
        """Agent-specific initialization logic"""
        pass
    
    @abstractmethod
    async def _on_shutdown(self) -> None:
        """Agent-specific shutdown logic"""
        pass
    
    def is_initialized(self) -> bool:
        """Check if agent is initialized"""
        return self._initialized
    
    def add_to_memory(self, message: AgentMessage) -> None:
        """Add message to agent memory"""
        if not message.timestamp:
            message.timestamp = datetime.now()
        self.memory.append(message)
        
        # Keep only last N messages to prevent memory bloat
        max_memory = 50
        if len(self.memory) > max_memory:
            self.memory = self.memory[-max_memory:]
    
    def get_memory_context(self, max_messages: int = 10) -> List[AgentMessage]:
        """Get recent memory context"""
        return self.memory[-max_messages:] if self.memory else []
    
    def clear_memory(self) -> None:
        """Clear agent memory"""
        self.memory.clear()
    
    def register_tool(self, tool_name: str, tool: Any) -> None:
        """Register a tool for the agent"""
        self.tools[tool_name] = tool
        LOG_DEBUG(f"Agent {self.config.agent_name} registered tool: {tool_name}")
    
    def unregister_tool(self, tool_name: str) -> None:
        """Unregister a tool"""
        if tool_name in self.tools:
            del self.tools[tool_name]
            LOG_DEBUG(f"Agent {self.config.agent_name} unregistered tool: {tool_name}")
    
    @abstractmethod
    async def process_message(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> AgentResponse:
        """Process a user message and return response"""
        pass
    
    @abstractmethod
    async def process_message_stream(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[AgentResponse]:
        """Process a user message and return streaming response"""
        pass


class ToolAgent(BaseAgent):
    """Agent with tool capabilities"""
    
    def __init__(self, config: AgentConfig):
        super().__init__(config)
        self.tool_history: List[ToolCall] = []
    
    async def _on_initialize(self) -> None:
        """Initialize tool agent"""
        # Load tools if enabled
        if self.config.enable_tools:
            await self._load_tools()
    
    async def _on_shutdown(self) -> None:
        """Shutdown tool agent"""
        self.tool_history.clear()
    
    async def _load_tools(self) -> None:
        """Load available tools"""
        # This would be implemented by subclasses
        pass
    
    async def execute_tool(
        self,
        tool_name: str,
        parameters: Dict[str, Any]
    ) -> ServiceResult:
        """Execute a tool with parameters"""
        if tool_name not in self.tools:
            return ServiceResult(
                success=False,
                message=f"Tool not found: {tool_name}"
            )
        
        try:
            tool = self.tools[tool_name]
            if hasattr(tool, '__call__'):
                result = await tool(**parameters)
                return ServiceResult(
                    success=True,
                    data=result,
                    message="Tool executed successfully"
                )
            else:
                return ServiceResult(
                    success=False,
                    message=f"Tool {tool_name} is not callable"
                )
        except Exception as e:
            LOG_ERROR(f"Tool execution failed: {e}")
            return ServiceResult(
                success=False,
                message=f"Tool execution failed: {str(e)}"
            )
    
    def add_tool_call(self, tool_call: ToolCall) -> None:
        """Add tool call to history"""
        self.tool_history.append(tool_call)
        
        # Keep only recent tool calls
        max_history = 20
        if len(self.tool_history) > max_history:
            self.tool_history = self.tool_history[-max_history:]
    
    def get_tool_history(self, max_calls: int = 10) -> List[ToolCall]:
        """Get recent tool call history"""
        return self.tool_history[-max_calls:] if self.tool_history else []


class MemoryAgent(BaseAgent):
    """Agent with enhanced memory capabilities"""
    
    def __init__(self, config: AgentConfig):
        super().__init__(config)
        self.long_term_memory: Dict[str, Any] = {}
        self.conversation_summaries: List[str] = []
    
    async def _on_initialize(self) -> None:
        """Initialize memory agent"""
        # Load long-term memory if enabled
        if self.config.enable_memory:
            await self._load_long_term_memory()
    
    async def _on_shutdown(self) -> None:
        """Shutdown memory agent"""
        await self._save_long_term_memory()
    
    async def _load_long_term_memory(self) -> None:
        """Load long-term memory from storage"""
        # This would be implemented by subclasses
        pass
    
    async def _save_long_term_memory(self) -> None:
        """Save long-term memory to storage"""
        # This would be implemented by subclasses
        pass
    
    def store_in_long_term_memory(self, key: str, value: Any) -> None:
        """Store information in long-term memory"""
        self.long_term_memory[key] = value
        LOG_DEBUG(f"Stored in long-term memory: {key}")
    
    def retrieve_from_long_term_memory(self, key: str) -> Optional[Any]:
        """Retrieve information from long-term memory"""
        return self.long_term_memory.get(key)
    
    async def summarize_conversation(self) -> str:
        """Summarize recent conversation"""
        if len(self.memory) < 3:
            return ""
        
        # Simple summarization - can be enhanced with LLM
        recent_messages = self.get_memory_context(max_messages=10)
        summary_points = []
        
        for msg in recent_messages[-5:]:  # Last 5 messages
            if msg.role == "user":
                summary_points.append(f"User asked about: {msg.content[:100]}...")
            elif msg.role == "assistant":
                summary_points.append(f"Assistant responded: {msg.content[:100]}...")
        
        summary = " | ".join(summary_points)
        self.conversation_summaries.append(summary)
        
        # Keep only recent summaries
        if len(self.conversation_summaries) > 10:
            self.conversation_summaries = self.conversation_summaries[-10:]
        
        return summary


class AgentRegistry:
    """Registry for managing agents"""
    
    def __init__(self):
        self._agents: Dict[str, BaseAgent] = {}
        self._initialized = False
    
    def register_agent(self, agent: BaseAgent) -> None:
        """Register an agent"""
        self._agents[agent.config.agent_name] = agent
        LOG_INFO(f"Registered agent: {agent.config.agent_name}")
    
    def unregister_agent(self, agent_name: str) -> None:
        """Unregister an agent"""
        if agent_name in self._agents:
            del self._agents[agent_name]
            LOG_INFO(f"Unregistered agent: {agent_name}")
    
    def get_agent(self, agent_name: str) -> Optional[BaseAgent]:
        """Get agent by name"""
        return self._agents.get(agent_name)
    
    async def initialize_all(self) -> bool:
        """Initialize all registered agents"""
        success = True
        
        for agent in self._agents.values():
            if not agent.is_initialized():
                result = await agent.initialize()
                if not result:
                    success = False
        
        if success:
            self._initialized = True
            LOG_INFO("All agents initialized successfully")
        else:
            LOG_ERROR("Some agents failed to initialize")
        
        return success
    
    async def shutdown_all(self) -> bool:
        """Shutdown all registered agents"""
        success = True
        
        for agent in self._agents.values():
            if agent.is_initialized():
                result = await agent.shutdown()
                if not result:
                    success = False
        
        if success:
            self._initialized = False
            LOG_INFO("All agents shutdown successfully")
        else:
            LOG_ERROR("Some agents failed to shutdown")
        
        return success
    
    def list_agents(self) -> List[str]:
        """List all registered agents"""
        return list(self._agents.keys())
    
    def is_initialized(self) -> bool:
        """Check if all agents are initialized"""
        return self._initialized


# Global agent registry
agent_registry = AgentRegistry()