import logging
from typing import Dict, List, Any, Optional

from agents.base_agent import BaseAgent
from tools.header_extraction_tool import HeaderExtractionTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HeaderExtractorAgent(BaseAgent):
    """Agent for extracting potential headers from Excel files."""
    
    def __init__(self, verbose: bool = True):
        """Initialize the header extractor agent."""
        super().__init__(
            name="header_extractor_agent",
            description="Extracts potential headers from Excel files",
            verbose=verbose
        )
        self.add_tool(HeaderExtractionTool())
    
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract potential headers from an Excel file.
        
        Args:
            state: The current state, must contain 'file_path'
            
        Returns:
            The updated state with 'potential_headers' added
        """
        self.think("Starting to extract potential headers from the Excel file")
        
        file_path = state.get('file_path')
        
        if not file_path:
            self.think("No file path provided, returning empty headers list")
            logger.error("No file path provided to HeaderExtractorAgent")
            state['potential_headers'] = []
            return state
        
        self.think(f"Analyzing Excel file at {file_path}")
        logger.info(f"Extracting headers from {file_path}")
        
        # Use the header extraction tool
        header_extraction_tool = self.tools[0]
        self.think("Using HeaderExtractionTool to extract potential headers")
        potential_headers = header_extraction_tool.run(file_path)
        
        self.think(f"Found {len(potential_headers)} potential headers")
        if potential_headers:
            self.think(f"Sample headers: {', '.join(potential_headers[:5])}")
            if len(potential_headers) > 5:
                self.think(f"And {len(potential_headers) - 5} more...")
        
        logger.info(f"Extracted {len(potential_headers)} potential headers")
        
        # Update the state
        state['potential_headers'] = potential_headers
        
        self.think("Finished extracting potential headers")
        return state
