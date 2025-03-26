import logging
from typing import Dict, List, Any, Optional

from agents.base_agent import BaseAgent
from tools.column_description_tool import ColumnDescriptionTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ColumnDescriptionAgent(BaseAgent):
    """Agent for describing target columns using LLM."""
    
    def __init__(self, verbose: bool = True):
        """Initialize the column description agent."""
        super().__init__(
            name="column_description_agent",
            description="Describes target columns using LLM",
            verbose=verbose
        )
        self.add_tool(ColumnDescriptionTool())
    
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Describe target columns using LLM.
        
        Args:
            state: The current state, must contain 'target_columns'
            
        Returns:
            The updated state with 'column_descriptions' added
        """
        self.think("Starting to describe target columns using LLM")
        
        target_columns = state.get('target_columns')
        
        if not target_columns:
            self.think("No target columns provided, returning empty descriptions")
            logger.error("No target columns provided to ColumnDescriptionAgent")
            state['column_descriptions'] = {}
            return state
        
        self.think(f"Analyzing {len(target_columns)} target columns")
        if target_columns:
            self.think(f"Sample target columns: {', '.join(target_columns[:5])}")
            if len(target_columns) > 5:
                self.think(f"And {len(target_columns) - 5} more...")
        
        logger.info(f"Describing {len(target_columns)} target columns")
        
        # Use the column description tool
        column_description_tool = self.tools[0]
        self.think("Using ColumnDescriptionTool to generate descriptions")
        column_descriptions = column_description_tool.run(target_columns)
        
        self.think(f"Generated descriptions for {len(column_descriptions)} target columns")
        if column_descriptions:
            sample_column = next(iter(column_descriptions))
            self.think(f"Sample description for '{sample_column}': {column_descriptions[sample_column]}")
        
        logger.info(f"Generated descriptions for {len(column_descriptions)} target columns")
        
        # Update the state
        state['column_descriptions'] = column_descriptions
        
        self.think("Finished describing target columns")
        return state
