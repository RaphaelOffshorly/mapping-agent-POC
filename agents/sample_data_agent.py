import logging
from typing import Dict, List, Any, Optional, Tuple

from agents.base_agent import BaseAgent
from tools.sample_data_tool import SampleDataTool
from tools.data_suggestion_tool import DataSuggestionTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SampleDataAgent(BaseAgent):
    """Agent for extracting sample data for matched headers using column ranges."""
    
    def __init__(self, verbose: bool = True):
        """Initialize the sample data agent."""
        super().__init__(
            name="sample_data_agent",
            description="Extracts sample data for matched headers using column ranges",
            verbose=verbose
        )
        self.add_tool(SampleDataTool())
    
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract sample data for matched headers using column ranges.
        
        Args:
            state: The current state, must contain 'file_path' and 'matches'
                  Can optionally contain 'column_ranges' with pre-generated column ranges
            
        Returns:
            The updated state with 'sample_data' added
        """
        self.think("Starting to extract sample data for matched headers using column ranges")
        
        file_path = state.get('file_path')
        matches = state.get('matches')
        column_ranges = state.get('column_ranges')
        
        if not file_path or not matches:
            self.think("Missing required inputs, returning empty sample data")
            logger.error("Missing required inputs for SampleDataAgent")
            state['sample_data'] = {}
            return state
        
        self.think(f"Analyzing {len(matches)} matches from file {file_path}")
        
        # Count how many matches are valid (not "No match found")
        valid_matches = {target: info for target, info in matches.items() 
                         if info.get('match') != "No match found"}
        
        self.think(f"Found {len(valid_matches)} valid matches out of {len(matches)} total matches")
        
        if valid_matches:
            # Display a few sample valid matches
            sample_matches = list(valid_matches.items())[:3]
            for target, match_info in sample_matches:
                self.think(f"Valid match for '{target}': '{match_info['match']}' (confidence: {match_info['confidence']})")
            if len(valid_matches) > 3:
                self.think(f"And {len(valid_matches) - 3} more valid matches...")
        
        # Check if we have pre-generated column ranges
        if column_ranges:
            self.think(f"Using {len(column_ranges)} pre-generated column range sets")
            # Log some examples of the column ranges
            if column_ranges:
                sample_ranges = list(column_ranges.items())[:2]
                for target, ranges in sample_ranges:
                    if ranges:
                        self.think(f"Sample column ranges for '{target}': {ranges[:2]}")
                        if len(ranges) > 2:
                            self.think(f"And {len(ranges) - 2} more column ranges...")
        else:
            self.think("No pre-generated column ranges found, will generate them as needed")
        
        logger.info(f"Extracting sample data for {len(matches)} matches")
        
        # Use the sample data tool
        sample_data_tool = self.tools[0]
        self.think("Using SampleDataTool to extract sample data")
        
        # Get column descriptions from state
        column_descriptions = state.get('column_descriptions', {})
        
        # Pass column ranges if available
        if column_ranges:
            self.think("Passing pre-generated column ranges to the tool")
            sample_data = sample_data_tool.run((file_path, matches, column_ranges, column_descriptions))
        else:
            self.think("Letting the tool generate column ranges as needed")
            sample_data = sample_data_tool.run((file_path, matches, None, column_descriptions))
        
        self.think(f"Extracted sample data for {len(sample_data)} target columns")
        if sample_data:
            # Display a few sample data entries
            sample_entries = list(sample_data.items())[:3]
            for target, data in sample_entries:
                self.think(f"Sample data for '{target}': {data[:3]}")
                if len(data) > 3:
                    self.think(f"And {len(data) - 3} more sample values...")
            if len(sample_data) > 3:
                self.think(f"And sample data for {len(sample_data) - 3} more target columns...")
        
        logger.info(f"Extracted sample data for {len(sample_data)} target columns")
        
        # Update the state
        state['sample_data'] = sample_data
        
        self.think("Finished extracting sample data")
        return state
