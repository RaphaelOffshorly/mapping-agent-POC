import logging
import pandas as pd
import json
from typing import Dict, List, Any, Optional, Tuple, Union

from tools.base_tool import BaseTool
from utils.excel import read_excel_file

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutoCellMappingTool(BaseTool[Tuple[str, Dict[str, Dict[str, str]], Dict[str, List[str]], Dict[str, List[str]], Dict[str, str]], Dict[str, Any]]):
    """Tool for automatically finding cell coordinates of selected data."""
    
    def __init__(self):
        """Initialize the auto cell mapping tool."""
        super().__init__(
            name="auto_cell_mapping_finder",
            description="Automatically finds exact cell coordinates of selected data"
        )
    
    def run(self, input_data: Tuple[str, Dict[str, Dict[str, str]], Dict[str, List[str]], Dict[str, List[str]], Dict[str, str]]) -> Dict[str, Any]:
        """
        Automatically find exact cell coordinates of selected data.
        
        Args:
            input_data: A tuple of (file_path, matches, sample_data, suggested_data, export_selections)
            
        Returns:
            A dictionary mapping target columns to cell coordinates in JSON format
        """
        file_path, matches, sample_data, suggested_data, export_selections = input_data
        
        if not file_path:
            return {"error": "No file path provided"}
        
        # Create a mapping dictionary
        mapping = {}
        
        try:
            # Read the Excel file
            excel_data = read_excel_file(file_path)
            
            # Process each target column
            for target, match_info in matches.items():
                # Skip if no match found
                if match_info.get("match") == "No match found":
                    continue
                
                # Determine which data source to use based on export_selections
                data_source = export_selections.get(target, "sample")
                
                if data_source == "ai" and target in suggested_data and suggested_data[target]:
                    # Use AI-suggested data
                    data_values = suggested_data[target]
                elif target in sample_data and sample_data[target]:
                    # Use sample data
                    data_values = sample_data[target]
                else:
                    # No data available
                    continue
                
                # Find cell coordinates for this data
                coords = self._find_cell_coordinates(excel_data, match_info["match"], data_values)
                
                if coords:
                    mapping[target] = coords
            
            # Return the mapping
            return {"mapping": mapping}
        
        except Exception as e:
            logger.error(f"Error finding cell coordinates: {e}")
            return {"error": str(e)}
    
    def _find_cell_coordinates(self, excel_data: Dict[str, pd.DataFrame], header_name: str, data_values: List[str]) -> List[str]:
        """
        Find the Excel cell coordinates for the given header and data values.
        
        Args:
            excel_data: Dictionary of DataFrames with sheet names as keys
            header_name: The header name to look for
            data_values: The data values to find
            
        Returns:
            A list of cell coordinates (e.g., ["Sheet1!A2", "Sheet1!A5", "Sheet1!A8"])
        """
        all_coords = []
        
        for sheet_name, df in excel_data.items():
            # First try to find the header in the first few rows
            header_row = -1
            header_col = -1
            
            # Search for the header in the first 20 rows
            for i in range(min(20, len(df))):
                for j in range(df.shape[1]):
                    cell_value = df.iloc[i, j]
                    if pd.notna(cell_value) and str(cell_value).strip() == header_name:
                        header_row = i
                        header_col = j
                        break
                if header_row != -1:
                    break
            
            if header_row != -1:
                # Found the header, now find the data values
                data_coords = []
                
                # Look for data values below the header (most common case)
                for i in range(header_row + 1, len(df)):
                    cell_value = df.iloc[i, header_col]
                    if pd.notna(cell_value) and str(cell_value).strip() in data_values:
                        # Convert to Excel-style coordinates (A1 notation)
                        col_letter = self._get_column_letter(header_col)
                        data_coords.append(f"{sheet_name}!{col_letter}{i+1}")
                
                # If no data found below, look for data to the right of the header
                if not data_coords:
                    for j in range(header_col + 1, df.shape[1]):
                        cell_value = df.iloc[header_row, j]
                        if pd.notna(cell_value) and str(cell_value).strip() in data_values:
                            # Convert to Excel-style coordinates (A1 notation)
                            col_letter = self._get_column_letter(j)
                            data_coords.append(f"{sheet_name}!{col_letter}{header_row+1}")
                
                # Add all found coordinates to the result
                all_coords.extend(data_coords)
        
        return all_coords
    
    def _get_column_letter(self, col_idx: int) -> str:
        """
        Convert a zero-based column index to an Excel-style column letter (A, B, C, ..., Z, AA, AB, ...).
        
        Args:
            col_idx: The zero-based column index
            
        Returns:
            The Excel-style column letter
        """
        result = ""
        while True:
            col_idx, remainder = divmod(col_idx, 26)
            result = chr(65 + remainder) + result
            if col_idx == 0:
                break
            col_idx -= 1
        return result
