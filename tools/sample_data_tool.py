import logging
from typing import Dict, List, Any, Optional, Tuple

from tools.base_tool import BaseTool
from utils.excel import extract_sample_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SampleDataTool(BaseTool[Tuple[str, Dict[str, Dict[str, str]]], Dict[str, List[str]]]):
    """Tool for extracting sample data for matched headers."""
    
    def __init__(self, max_rows: int = 5):
        """
        Initialize the sample data tool.
        
        Args:
            max_rows: The maximum number of rows to extract
        """
        super().__init__(
            name="sample_data_extractor",
            description="Extracts sample data for matched headers"
        )
        self.max_rows = max_rows
    
    def run(self, input_data: Tuple[str, Dict[str, Dict[str, str]]]) -> Dict[str, List[str]]:
        """
        Extract sample data for matched headers.
        
        Args:
            input_data: A tuple of (file_path, matches)
            
        Returns:
            A dictionary with sample data for each target column
        """
        file_path, matches = input_data
        
        if not file_path or not matches:
            return {}
        
        sample_data = {}
        
        for target, info in matches.items():
            if info["match"] != "No match found":
                # Extract actual sample data from the file
                samples = extract_sample_data(file_path, info["match"], self.max_rows)
                sample_data[target] = samples
        
        return sample_data
