import logging
import pandas as pd
import json
from typing import Dict, List, Any, Optional, Tuple, Union

from tools.base_tool import BaseTool
from utils.excel import read_excel_file

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CellCoordinateTool(BaseTool[Tuple[str, Dict[str, Dict[str, str]], Dict[str, List[str]], Dict[str, List[str]], Dict[str, str], Dict[str, List[Tuple[str, str]]]], Dict[str, Any]]):
    """Tool for finding exact cell coordinates of selected data."""
    
    def __init__(self):
        """Initialize the cell coordinate tool."""
        super().__init__(
            name="cell_coordinate_finder",
            description="Finds exact cell coordinates of selected data"
        )
    
    def run(self, input_data: Tuple[str, Dict[str, Dict[str, str]], Dict[str, List[str]], Dict[str, List[str]], Dict[str, str], Dict[str, List[Tuple[str, str]]]]) -> Dict[str, Any]:
        """
        Find exact cell coordinates of selected data.
        
        Args:
            input_data: A tuple of (file_path, matches, sample_data, suggested_data, export_selections, selected_cells)
            
        Returns:
            A dictionary mapping target columns to cell coordinates in JSON format
        """
        file_path, matches, sample_data, suggested_data, export_selections, selected_cells = input_data
        
        if not file_path:
            return {}
        
        # Create a mapping dictionary
        mapping = {}
        
        try:
            # Initialize all target columns with empty arrays
            for target in matches.keys():
                mapping[target] = []
            
            # Process each target column
            for target, match_info in matches.items():
                # Use the selected cells from the UI if available
                if target in selected_cells and selected_cells[target]:
                    # Format the coordinates
                    coords = []
                    for sheet_name, cell_coord in selected_cells[target]:
                        coords.append(f"{sheet_name}!{cell_coord}")
                    
                    # Add to mapping
                    mapping[target] = coords
            
            # Convert to JSON format
            result = {
                "mapping": mapping
            }
            
            return result
        
        except Exception as e:
            logger.error(f"Error finding cell coordinates: {e}")
            return {"error": str(e)}
