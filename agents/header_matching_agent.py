import logging
from typing import Dict, List, Any, Optional, Tuple

from agents.base_agent import BaseAgent
from tools.header_matching_tool import HeaderMatchingTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HeaderMatchingAgent(BaseAgent):
    """Agent for matching headers to target columns using LLM."""
    
    def __init__(self, verbose: bool = True):
        """Initialize the header matching agent."""
        super().__init__(
            name="header_matching_agent",
            description="Matches headers to target columns using LLM",
            verbose=verbose
        )
        self.add_tool(HeaderMatchingTool())
    
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Match headers to target columns using LLM.
        
        Args:
            state: The current state, must contain 'potential_headers' and 'target_columns'
            
        Returns:
            The updated state with 'matches' added
        """
        self.think("Starting to match headers to target columns using LLM")
        
        potential_headers = state.get('potential_headers')
        target_columns = state.get('target_columns')
        
        if not potential_headers or not target_columns:
            self.think("Missing required inputs, returning empty matches")
            logger.error("Missing required inputs for HeaderMatchingAgent")
            state['matches'] = {}
            return state
        
        self.think(f"Analyzing {len(potential_headers)} potential headers and {len(target_columns)} target columns")
        if potential_headers:
            self.think(f"Sample potential headers: {', '.join(potential_headers[:5])}")
            if len(potential_headers) > 5:
                self.think(f"And {len(potential_headers) - 5} more...")
        
        if target_columns:
            self.think(f"Sample target columns: {', '.join(target_columns[:5])}")
            if len(target_columns) > 5:
                self.think(f"And {len(target_columns) - 5} more...")
        
        logger.info(f"Matching {len(potential_headers)} headers to {len(target_columns)} target columns")
        
        # Use the header matching tool
        header_matching_tool = self.tools[0]
        self.think("Using HeaderMatchingTool to match headers to target columns")
        matches = header_matching_tool.run((potential_headers, target_columns))
        
        self.think(f"Generated matches for {len(matches)} target columns")
        if matches:
            # Display a few sample matches
            sample_matches = list(matches.items())[:3]
            for target, match_info in sample_matches:
                self.think(f"Match for '{target}': '{match_info['match']}' (confidence: {match_info['confidence']})")
            if len(matches) > 3:
                self.think(f"And {len(matches) - 3} more matches...")
        
        logger.info(f"Generated matches for {len(matches)} target columns")
        
        # Update the state
        state['matches'] = matches
        
        self.think("Finished matching headers to target columns")
        return state
