import logging
from typing import Dict, List, Any, Optional, Callable
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseAgent(ABC):
    """Base class for all agents."""
    
    def __init__(self, name: str, description: str, verbose: bool = True):
        """
        Initialize an agent.
        
        Args:
            name: The name of the agent
            description: A description of what the agent does
            verbose: Whether to display the agent's thought process
        """
        self.name = name
        self.description = description
        self.tools = []
        self.verbose = verbose
    
    def add_tool(self, tool):
        """
        Add a tool to the agent.
        
        Args:
            tool: The tool to add
        """
        self.tools.append(tool)
    
    def think(self, message: str):
        """
        Display the agent's thought process.
        
        Args:
            message: The thought message
        """
        if self.verbose:
            logger.info(f"[{self.name} thinking] {message}")
    
    @abstractmethod
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the agent with the given state.
        
        Args:
            state: The current state
            
        Returns:
            The updated state
        """
        pass
