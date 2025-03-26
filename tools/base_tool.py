from typing import Dict, List, Any, Optional, Callable, TypeVar, Generic
from abc import ABC, abstractmethod

T = TypeVar('T')
R = TypeVar('R')

class BaseTool(Generic[T, R], ABC):
    """Base class for all tools."""
    
    def __init__(self, name: str, description: str):
        """
        Initialize a tool.
        
        Args:
            name: The name of the tool
            description: A description of what the tool does
        """
        self.name = name
        self.description = description
    
    @abstractmethod
    def run(self, input_data: T) -> R:
        """
        Run the tool with the given input.
        
        Args:
            input_data: The input data for the tool
            
        Returns:
            The result of running the tool
        """
        pass
    
    def __call__(self, input_data: T) -> R:
        """
        Make the tool callable.
        
        Args:
            input_data: The input data for the tool
            
        Returns:
            The result of running the tool
        """
        return self.run(input_data)
