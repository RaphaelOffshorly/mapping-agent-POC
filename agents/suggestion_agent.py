import logging
from typing import Dict, List, Any, Optional, Tuple

from agents.base_agent import BaseAgent
from tools.header_suggestion_tool import HeaderSuggestionTool
from tools.data_suggestion_tool import DataSuggestionTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SuggestionAgent(BaseAgent):
    """Agent for suggesting headers and column ranges for target columns."""
    
    def __init__(self, verbose: bool = True):
        """Initialize the suggestion agent."""
        super().__init__(
            name="suggestion_agent",
            description="Suggests headers and column ranges for target columns",
            verbose=verbose
        )
        self.add_tool(HeaderSuggestionTool())
        self.add_tool(DataSuggestionTool())
    
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Suggest headers and column ranges for target columns.
        
        Args:
            state: The current state, must contain 'file_path', 'target_columns', 'matches', and 'column_descriptions'
            
        Returns:
            The updated state with 'suggested_headers' and 'column_ranges' added
        """
        self.think("Starting to suggest headers and column ranges for target columns")
        
        file_path = state.get('file_path')
        target_columns = state.get('target_columns')
        matches = state.get('matches', {})
        column_descriptions = state.get('column_descriptions', {})
        
        if not file_path or not target_columns:
            self.think("Missing required inputs, returning empty suggestions")
            logger.error("Missing required inputs for SuggestionAgent")
            state['suggested_headers'] = {}
            state['column_ranges'] = {}
            return state
        
        self.think(f"Analyzing {len(target_columns)} target columns")
        if target_columns:
            self.think(f"Sample target columns: {', '.join(target_columns[:5])}")
            if len(target_columns) > 5:
                self.think(f"And {len(target_columns) - 5} more...")
        
        logger.info(f"Suggesting headers and column ranges for {len(target_columns)} target columns")
        
        # Get the tools
        header_suggestion_tool = self.tools[0]
        data_suggestion_tool = self.tools[1]
        
        # Suggest headers and column ranges for each target column
        suggested_headers = {}
        column_ranges = {}
        
        for target_column in target_columns:
            self.think(f"Processing target column: '{target_column}'")
            
            # Get the column description if available
            column_description = column_descriptions.get(target_column)
            if column_description:
                self.think(f"Found column description for '{target_column}'")
            else:
                self.think(f"No column description available for '{target_column}'")
            
            # Suggest a header for this target column
            self.think(f"Using HeaderSuggestionTool to suggest header for '{target_column}'")
            suggested_header = header_suggestion_tool.run((file_path, target_column, column_description))
            suggested_headers[target_column] = suggested_header
            self.think(f"Suggested header for '{target_column}': '{suggested_header}'")
            
            # Get the matched header for this target column
            matched_header = "No match found"
            if target_column in matches and matches[target_column].get('match') != "No match found":
                matched_header = matches[target_column].get('match')
                self.think(f"Found matched header for '{target_column}': '{matched_header}'")
            else:
                self.think(f"No matched header found for '{target_column}'")
            
            # Generate column ranges for this target column
            self.think(f"Using DataSuggestionTool to generate column ranges for '{target_column}'")
            ranges = data_suggestion_tool.run((file_path, target_column, matched_header, column_description))
            column_ranges[target_column] = ranges
            
            self.think(f"Generated {len(ranges)} column ranges for '{target_column}'")
            if ranges:
                self.think(f"Sample ranges for '{target_column}': {ranges[:3]}")
                if len(ranges) > 3:
                    self.think(f"And {len(ranges) - 3} more ranges...")
        
        logger.info(f"Generated suggestions for {len(suggested_headers)} target columns")
        
        # Update the state
        state['suggested_headers'] = suggested_headers
        state['column_ranges'] = column_ranges
        
        self.think("Finished suggesting headers and column ranges")
        return state
