"""
Agent Layer - Langchain Integration
Provides Langchain-based agent implementations
"""

import asyncio
from typing import Dict, List, Any, Optional, Union, AsyncIterator
from datetime import datetime
import json

from langchain_core.messages import (
    HumanMessage,
    AIMessage,
    SystemMessage,
    ToolMessage,
    BaseMessage,
    ChatMessage
)
from langchain_core.tools import BaseTool
from langchain_core.agents import AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
from langchain.agents import create_openai_tools_agent
from langchain_community.callbacks import get_openai_callback

from .base import (
    BaseAgent, ToolAgent, AgentConfig, AgentMessage, AgentResponse, 
    ToolCall, agent_registry
)
from ..service import execute_mcp_tool
from ..logger import LOG_INFO, LOG_ERROR, LOG_DEBUG
from ..conf import settings


class LangchainTool(BaseTool):
    """Langchain tool wrapper for MCP tools"""
    
    def __init__(self, tool_name: str, description: str, mcp_service):
        self._tool_name = tool_name
        self._description = description
        self._mcp_service = mcp_service
        super().__init__()
    
    @property
    def name(self) -> str:
        return self._tool_name
    
    @property
    def description(self) -> str:
        return self._description
    
    def _run(self, **kwargs) -> str:
        """Run the tool synchronously"""
        return asyncio.run(self._arun(**kwargs))
    
    async def _arun(self, **kwargs) -> str:
        """Run the tool asynchronously"""
        try:
            result = await self._mcp_service.execute_tool(self._tool_name, kwargs)
            if result.success:
                return json.dumps(result.data, ensure_ascii=False, indent=2)
            else:
                return f"Error: {result.message}"
        except Exception as e:
            return f"Tool execution failed: {str(e)}"


class LangchainAgent(ToolAgent):
    """Langchain-based agent implementation"""
    
    def __init__(self, config: AgentConfig):
        super().__init__(config)
        self.llm: Optional[ChatOpenAI] = None
        self.agent_executor: Optional[AgentExecutor] = None
        self.prompt_template: Optional[ChatPromptTemplate] = None
    
    async def _on_initialize(self) -> None:
        """Initialize Langchain agent"""
        await self._initialize_llm()
        await self._initialize_prompt()
        await self._initialize_agent_executor()
        await self._load_tools()
    
    async def _on_shutdown(self) -> None:
        """Shutdown Langchain agent"""
        self.llm = None
        self.agent_executor = None
        self.prompt_template = None
    
    async def _initialize_llm(self) -> None:
        """Initialize LLM"""
        try:
            self.llm = ChatOpenAI(
                model_name=self.config.model_name,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                openai_api_base=settings.platform.llm_baseurl,
                openai_api_key=settings.platform.llm_apikey
            )
            LOG_INFO(f"LLM initialized: {self.config.model_name}")
        except Exception as e:
            LOG_ERROR(f"Failed to initialize LLM: {e}")
            raise
    
    async def _initialize_prompt(self) -> None:
        """Initialize prompt template"""
        try:
            # Default system prompt if not provided
            if not self.config.system_prompt:
                self.config.system_prompt = self._get_default_system_prompt()
            
            self.prompt_template = ChatPromptTemplate.from_messages([
                ("system", self.config.system_prompt),
                MessagesPlaceholder(variable_name="chat_history", optional=True),
                ("human", "{input}"),
                MessagesPlaceholder(variable_name="agent_scratchpad"),
            ])
            LOG_DEBUG("Prompt template initialized")
        except Exception as e:
            LOG_ERROR(f"Failed to initialize prompt template: {e}")
            raise
    
    async def _initialize_agent_executor(self) -> None:
        """Initialize agent executor"""
        try:
            if not self.llm or not self.prompt_template:
                raise RuntimeError("LLM and prompt template must be initialized first")
            
            # Tools will be added later
            tools = []
            
            # Create agent
            agent = create_openai_tools_agent(self.llm, tools, self.prompt_template)
            
            # Create agent executor
            self.agent_executor = AgentExecutor(
                agent=agent,
                tools=tools,
                verbose=True,
                return_intermediate_steps=True,
                max_iterations=10,
                early_stopping_method="generate"
            )
            
            LOG_DEBUG("Agent executor initialized")
        except Exception as e:
            LOG_ERROR(f"Failed to initialize agent executor: {e}")
            raise
    
    async def _load_tools(self) -> None:
        """Load MCP tools as Langchain tools"""
        try:
            from ..service import mcp_service
            
            # Get available tools from MCP service
            available_tools = await mcp_service.list_available_tools()
            
            langchain_tools = []
            for tool_name in available_tools:
                # Get tool info
                tool_info = await mcp_service.get_tool_info(tool_name)
                if tool_info:
                    description = tool_info.get('description', f'MCP tool: {tool_name}')
                    
                    # Create Langchain tool wrapper
                    langchain_tool = LangchainTool(
                        tool_name=tool_name,
                        description=description,
                        mcp_service=mcp_service
                    )
                    langchain_tools.append(langchain_tool)
                    
                    # Register tool with agent
                    self.register_tool(tool_name, langchain_tool)
            
            # Update agent executor with tools
            if self.agent_executor and langchain_tools:
                # Recreate agent with tools
                agent = create_openai_tools_agent(self.llm, langchain_tools, self.prompt_template)
                self.agent_executor = AgentExecutor(
                    agent=agent,
                    tools=langchain_tools,
                    verbose=True,
                    return_intermediate_steps=True,
                    max_iterations=10,
                    early_stopping_method="generate"
                )
                
                LOG_INFO(f"Loaded {len(langchain_tools)} tools into agent")
            
        except Exception as e:
            LOG_ERROR(f"Failed to load tools: {e}")
    
    def _get_default_system_prompt(self) -> str:
        """Get default system prompt"""
        return """你是一个超级助理，回答问题的时候，你可以先查询MCP工具，再根据用户提问，尽可能的使用工具来回答问题。

注意：不要思考过程<no_think></no_think>

# 原则：
1.回答的内容，全部基于MCP工具返回的数据，不要无中生有的创造答案。
2.遇到"无响应数据或返回数据为NULL"，直接按照没有数据处理
3.如果从数据来源中可以直接查询的数据，则直接去查询，不要再走ZXYC_开头的接口

# 思维链：
1.先根据指令，分析数据源在哪里，确定查询mysql,es,nebula,milvuse,或者调用api
2.如果是调用api的，则直接走zxyc_开头的mcp工具即可
3.如果是需要查库，则先确定表名，和字段名称
4.再根据字段名称匹配，去表中查询数据

# 数据来源
以下数据源，数据均来自Elasticsearch，数据表结构在mysql数据库的security_table_define表和security_tablecolumn_define表中。

查询方法：
1.可以查询security_table_define得到索引名称和描述，其中ID就是表ID，TableName就是索引名称，TableCnName就是表名的中文描述
2.可以根据表ID查询security_tablecolumn_define得到字段名和描述，其中tableid就是表ID，ColName就是列名称，ColCnName就是列的中文描述
3.security_table_define.id=security_tablecolumn_define.table_id

# 规则：
1.对于一个人的描述有姓名，身份证，视频身份VID。一般身份证是18位数字或者17位数字加最后一位X，全部汉字的是姓名，其他描述一般都是视频身份VID编号。
2.查询人物轨迹的时候，仔细分析语句中的参数部分，先获取时间，地点，再按照头部特征，上身穿着和颜色，下身穿着和颜色，鞋子穿着和颜色，性别，年龄段分析指令中的参数。
3.对于车辆，注意车辆颜色和车牌颜色的区分"""
    
    def _convert_to_langchain_messages(self, messages: List[AgentMessage]) -> List[BaseMessage]:
        """Convert agent messages to Langchain messages"""
        langchain_messages = []
        
        for msg in messages:
            if msg.role == "system":
                langchain_messages.append(SystemMessage(content=msg.content))
            elif msg.role == "user":
                langchain_messages.append(HumanMessage(content=msg.content))
            elif msg.role == "assistant":
                langchain_messages.append(AIMessage(content=msg.content))
            elif msg.role == "tool":
                langchain_messages.append(ToolMessage(content=msg.content))
            else:
                langchain_messages.append(ChatMessage(role=msg.role, content=msg.content))
        
        return langchain_messages
    
    def _convert_to_agent_message(self, message: BaseMessage) -> AgentMessage:
        """Convert Langchain message to agent message"""
        role = "assistant" if isinstance(message, AIMessage) else message.type
        return AgentMessage(
            role=role,
            content=message.content,
            timestamp=datetime.now()
        )
    
    async def process_message(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> AgentResponse:
        """Process a user message and return response"""
        if not self.agent_executor:
            raise RuntimeError("Agent not initialized")
        
        try:
            # Get memory context
            memory_context = self.get_memory_context()
            langchain_history = self._convert_to_langchain_messages(memory_context)
            
            # Add current message to memory
            user_message = AgentMessage(
                role="user",
                content=message,
                timestamp=datetime.now()
            )
            self.add_to_memory(user_message)
            
            # Execute agent
            with get_openai_callback() as cb:
                result = await self.agent_executor.ainvoke({
                    "input": message,
                    "chat_history": langchain_history
                })
                
                # Extract response
                response_content = result.get("output", "")
                intermediate_steps = result.get("intermediate_steps", [])
                
                # Process tool calls
                tool_calls = []
                for step in intermediate_steps:
                    if len(step) >= 2:
                        action, observation = step
                        if hasattr(action, 'tool') and hasattr(action, 'tool_input'):
                            tool_call = ToolCall(
                                tool_name=action.tool,
                                parameters=action.tool_input
                            )
                            tool_calls.append(tool_call)
                            self.add_tool_call(tool_call)
                
                # Create assistant message
                assistant_message = AgentMessage(
                    role="assistant",
                    content=response_content,
                    timestamp=datetime.now()
                )
                self.add_to_memory(assistant_message)
                
                return AgentResponse(
                    message=assistant_message,
                    tool_calls=tool_calls,
                    usage_stats={
                        "total_tokens": cb.total_tokens,
                        "prompt_tokens": cb.prompt_tokens,
                        "completion_tokens": cb.completion_tokens,
                        "total_cost": cb.total_cost
                    }
                )
                
        except Exception as e:
            LOG_ERROR(f"Message processing failed: {e}")
            raise
    
    async def process_message_stream(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[AgentResponse]:
        """Process a user message and return streaming response"""
        # For now, return non-streaming response
        # In a real implementation, this would use streaming capabilities
        response = await self.process_message(message, context)
        yield response


class DataAnalysisAgent(LangchainAgent):
    """Specialized agent for data analysis tasks"""
    
    def __init__(self):
        config = AgentConfig(
            agent_name="data_analysis_agent",
            model_name=settings.platform.llm_deepseekid,
            temperature=0.3,  # Lower temperature for more analytical responses
            max_tokens=3000,
            enable_tools=True,
            system_prompt=self._get_data_analysis_prompt()
        )
        super().__init__(config)
    
    def _get_data_analysis_prompt(self) -> str:
        """Get data analysis specific system prompt"""
        return """你是一个数据分析专家，专门处理各种数据查询和分析任务。

你的主要职责：
1. 理解用户的数据分析需求
2. 使用合适的工具查询相关数据
3. 对查询结果进行分析和总结
4. 提供清晰的数据洞察

工作流程：
1. 分析用户问题，确定需要查询的数据类型
2. 选择合适的工具进行数据查询
3. 对查询结果进行整理和分析
4. 生成包含数据洞察的回复

请确保：
- 数据查询准确无误
- 分析结果客观公正
- 回答内容清晰易懂
- 提供具体的数值和趋势分析"""


class QueryAssistantAgent(LangchainAgent):
    """Specialized agent for general query assistance"""
    
    def __init__(self):
        config = AgentConfig(
            agent_name="query_assistant_agent",
            model_name=settings.platform.llm_qwenid,
            temperature=0.7,
            max_tokens=2000,
            enable_tools=True,
            system_prompt=self._get_query_assistant_prompt()
        )
        super().__init__(config)
    
    def _get_query_assistant_prompt(self) -> str:
        """Get query assistant specific system prompt"""
        return """你是一个智能查询助手，可以帮助用户查询各种信息。

你的特点：
- 友好的对话风格
- 准确的信息查询
- 清晰的结果展示
- 主动提供相关建议

请：
1. 耐心理解用户需求
2. 使用合适的工具查询信息
3. 用通俗易懂的方式回答
4. 主动提供帮助和建议"""


# Create global agent instances
data_analysis_agent = DataAnalysisAgent()
query_assistant_agent = QueryAssistantAgent()

# Register agents
agent_registry.register_agent(data_analysis_agent)
agent_registry.register_agent(query_assistant_agent)