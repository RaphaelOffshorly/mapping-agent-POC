import logging
import pandas as pd
import json
from typing import Dict, List, Any, Optional, Tuple, Union

from tools.base_tool import BaseTool
from utils.excel import read_excel_file

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CellCoordinateTool(BaseTool[Tuple[str, Dict[str, Dict[str, str]], Dict[str, List[str]], Dict[str, List[str]], Dict[str, str], Dict[str, List[Tuple[str, str]]], Optional[Dict[str, List[Tuple[str, str, str, str]]]]], Dict[str, Any]]):
    """Tool for finding exact cell coordinates of selected data."""
    
    def __init__(self):
        """Initialize the cell coordinate tool."""
        super().__init__(
            name="cell_coordinate_finder",
            description="Finds exact cell coordinates of selected data"
        )
    
    def run(self, input_data: Tuple[str, Dict[str, Dict[str, str]], Dict[str, List[str]], Dict[str, List[str]], Dict[str, str], Dict[str, List[Tuple[str, str]]], Optional[Dict[str, List[Tuple[str, str, str, str]]]]]) -> Dict[str, Any]:
        """
        Find exact cell coordinates of selected data.
        
        Args:
            input_data: A tuple of (file_path, matches, sample_data, suggested_data, export_selections, selected_cells, column_ranges)
                - file_path: Path to the Excel file
                - matches: Dictionary of target columns to matched headers
                - sample_data: Dictionary of sample data for each target column
                - suggested_data: Dictionary of suggested data for each target column
                - export_selections: Dictionary of export selections
                - selected_cells: Dictionary of manually selected cells from the UI
                - column_ranges: Optional dictionary of AI-generated column ranges from the suggestion agent
            
        Returns:
            A dictionary mapping target columns to cell coordinates in JSON format
        """
        # Parse input data, handling the case of both 6 and 7 arguments
        if len(input_data) == 7:
            file_path, matches, sample_data, suggested_data, export_selections, selected_cells, column_ranges = input_data
        else:
            file_path, matches, sample_data, suggested_data, export_selections, selected_cells = input_data
            column_ranges = {}
        
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
                # First priority: Use manually selected cells from the UI if available
                if target in selected_cells and selected_cells[target]:
                    # Format the coordinates
                    coords = []
                    for sheet_name, cell_coord in selected_cells[target]:
                        coords.append(f"{sheet_name}!{cell_coord}")
                    
                    # Add to mapping
                    mapping[target] = coords
                
                # Second priority: Use AI-suggested column ranges if available
                elif target in column_ranges and column_ranges[target]:
                    # Format the column ranges as a set of coordinates
                    coords = []
                    for sheet_name, column_letter, start_row, end_row in column_ranges[target]:
                        # Convert start_row and end_row to integers
                        try:
                            start = int(start_row)
                            end = int(end_row)
                        except ValueError:
                            # Skip this range if the row indices are invalid
                            logger.error(f"Invalid row indices: {start_row}-{end_row}")
                            continue
                            
                        # Create a coordinate for each row in the range
                        for row in range(start, end + 1):
                            coords.append(f"{sheet_name}!{column_letter}{row}")
                    
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
