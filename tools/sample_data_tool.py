import logging
from typing import Dict, List, Any, Optional, Tuple, Union

from tools.base_tool import BaseTool
from tools.data_suggestion_tool import DataSuggestionTool
from utils.excel import retrieve_data_from_column_ranges

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SampleDataTool(BaseTool[Tuple[str, Dict[str, Dict[str, str]], Optional[Dict[str, List[Tuple[str, str, str, str]]]]], Dict[str, List[str]]]):
    """Tool for retrieving sample data using column ranges."""
    
    def __init__(self, max_samples: int = 20):
        """
        Initialize the sample data tool.
        
        Args:
            max_samples: The maximum number of samples to return
        """
        super().__init__(
            name="sample_data_extractor",
            description="Retrieves sample data using column ranges"
        )
        self.max_samples = max_samples
        self.data_suggestion_tool = DataSuggestionTool()
    
    def run(self, input_data: Tuple[str, Dict[str, Dict[str, str]], Optional[Dict[str, List[Tuple[str, str, str, str]]]]]) -> Dict[str, List[str]]:
        """
        Retrieve sample data using column ranges.
        
        Args:
            input_data: A tuple of (file_path, matches, pre_generated_column_ranges)
                - file_path: Path to the Excel file
                - matches: Dict of target columns to matched headers
                - pre_generated_column_ranges: Optional dict of pre-generated column ranges
            
        Returns:
            A dictionary with sample data for each target column
        """
        if len(input_data) == 2:
            file_path, matches = input_data
            pre_generated_column_ranges = None
        else:
            file_path, matches, pre_generated_column_ranges = input_data
        
        if not file_path or not matches:
            return {}
        
        sample_data = {}
        
        for target, info in matches.items():
            if info["match"] != "No match found":
                # Get column ranges either from pre-generated ones or generate new ones
                if pre_generated_column_ranges and target in pre_generated_column_ranges:
                    column_ranges = pre_generated_column_ranges[target]
                else:
                    # Get column description if available
                    column_description = None
                    if "description" in info:
                        column_description = {
                            "description": info.get("description", ""),
                            "data_type": info.get("data_type", ""),
                            "sample_values": info.get("sample_values", [])
                        }
                    
                    # Generate column ranges using the data suggestion tool
                    column_ranges = self.data_suggestion_tool.run((file_path, target, info["match"], column_description))
                
                if column_ranges:
                    # Retrieve the actual data using the column ranges
                    data = retrieve_data_from_column_ranges(file_path, column_ranges)
                    # Filter out empty values
                    data = [val for val in data if val]
                    
                    # Limit to max_samples
                    data = data[:self.max_samples] if len(data) > self.max_samples else data
                    
                    if data:
                        sample_data[target] = data
                    else:
                        sample_data[target] = ["No sample data found"]
                else:
                    sample_data[target] = ["No sample data found"]
        
        return sample_data
