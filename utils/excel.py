import os
import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple

from utils.common import infer_header_row, extract_from_inferred_header
from config.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_excel_file(file_path: str, sheet_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """
    Read an Excel file and return a dictionary of DataFrames.
    
    Args:
        file_path: The path to the Excel file
        sheet_name: The name of the sheet to read, if None, read all sheets
        
    Returns:
        A dictionary of DataFrames, with sheet names as keys
    """
    try:
        if sheet_name:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            return {sheet_name: df}
        else:
            excel_file = pd.ExcelFile(file_path)
            dfs = {}
            for sheet in excel_file.sheet_names:
                dfs[sheet] = pd.read_excel(file_path, sheet_name=sheet, header=None)
            return dfs
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        raise

def get_excel_preview(file_path: str) -> Dict[str, Any]:
    """
    Get a preview of the Excel file for display in the UI.
    
    Args:
        file_path: The path to the Excel file
        
    Returns:
        A dictionary with sheet data
    """
    try:
        excel_file = pd.ExcelFile(file_path)
        preview = {}
        
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            
            # Get dimensions
            rows, cols = df.shape
            
            # Extract all data without limiting rows/columns
            preview_data = []
            for i in range(rows):
                row_data = []
                for j in range(cols):
                    val = df.iloc[i, j]
                    # Preserve formatting better
                    if pd.isna(val):
                        row_data.append("")
                    elif isinstance(val, (int, float)) and val == int(val):
                        # Format integers without decimal points
                        row_data.append(str(int(val)))
                    else:
                        row_data.append(str(val))
                preview_data.append(row_data)
            
            # Add row numbers for all rows
            row_numbers = [str(i+1) for i in range(rows)]
            
            preview[sheet_name] = {
                "data": preview_data,
                "row_numbers": row_numbers,
                "total_rows": rows,
                "total_cols": cols
            }
        
        return preview
    except Exception as e:
        logger.error(f"Error generating Excel preview: {e}")
        return {"error": str(e)}

def extract_sample_data(file_path: str, header_name: str, max_rows: int = 5) -> List[str]:
    """
    Extract sample data for a given header from the Excel file.
    
    Args:
        file_path: The path to the Excel file
        header_name: The name of the header to extract data for
        max_rows: The maximum number of rows to extract
        
    Returns:
        A list of sample values
    """
    try:
        excel_file = pd.ExcelFile(file_path)
        samples = []
        
        # Try to find the header in each sheet
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            
            # First try to find the header in the inferred header row
            inferred_index = infer_header_row(df, Config.HEADER_SCAN_ROWS)
            if inferred_index is not None:
                header_row = df.iloc[inferred_index]
                for col_idx, col_name in enumerate(header_row):
                    if str(col_name).strip() == header_name:
                        # Extract sample data from this column
                        data_start_row = inferred_index + 1
                        if data_start_row < len(df):
                            column_data = df.iloc[data_start_row:data_start_row+max_rows, col_idx].tolist()
                            samples = [str(val) for val in column_data if pd.notna(val)]
                            if samples:
                                return samples
            
            # If not found in header row, search the entire sheet
            for i in range(min(20, len(df))):
                for j in range(min(20, df.shape[1])):
                    if str(df.iloc[i, j]).strip() == header_name:
                        # Found the header, extract data below or to the right
                        # Try below first (more common)
                        if i + 1 < len(df):
                            column_data = df.iloc[i+1:i+1+max_rows, j].tolist()
                            samples = [str(val) for val in column_data if pd.notna(val)]
                            if samples:
                                return samples
                        
                        # Try to the right if no data found below
                        if not samples and j + 1 < df.shape[1]:
                            row_data = df.iloc[i, j+1:j+1+max_rows].tolist()
                            samples = [str(val) for val in row_data if pd.notna(val)]
                            if samples:
                                return samples
        
        return samples or ["No sample data found"]
    except Exception as e:
        logger.error(f"Error extracting sample data: {e}")
        return ["Error extracting sample data"]

def find_cell_coordinates_for_data(file_path: str, data_values: List[str], max_rows: int = None) -> List[Tuple[str, str]]:
    """
    Find the cell coordinates for the given data values in the Excel file.
    
    Args:
        file_path: The path to the Excel file
        data_values: The list of data values to find
        max_rows: Maximum number of rows to search (None for all rows)
        
    Returns:
        A list of tuples (sheet_name, cell_coordinate) for each found value
    """
    try:
        excel_file = pd.ExcelFile(file_path)
        coordinates = []
        
        # Convert all data values to strings for comparison
        data_values_str = [str(val).strip() for val in data_values]
        
        # Create a dictionary to track all occurrences of each value
        all_occurrences = {}
        
        # First pass: collect all occurrences of each value
        for sheet_name in excel_file.sheet_names:
            # Get the selected sheet name from session if available
            selected_sheet = None
            try:
                from flask import session
                selected_sheet = session.get('selected_sheet')
            except:
                pass
            
            # Skip sheets that are not the selected sheet if a sheet is selected
            if selected_sheet and sheet_name != selected_sheet:
                logger.info(f"Skipping sheet {sheet_name} as it's not the selected sheet {selected_sheet}")
                continue
                
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            
            # Get the number of rows to search
            rows_to_search = len(df) if max_rows is None else min(len(df), max_rows)
            
            # Search the entire sheet for each data value
            for data_value in data_values_str:
                # Skip empty values
                if not data_value:
                    continue
                
                # Initialize the list of occurrences for this value if not already done
                if data_value not in all_occurrences:
                    all_occurrences[data_value] = []
                
                # Search the sheet for the data value
                for i in range(rows_to_search):
                    for j in range(df.shape[1]):
                        cell_value = str(df.iloc[i, j]).strip() if pd.notna(df.iloc[i, j]) else ""
                        if cell_value == data_value:
                            # Convert column index to Excel column letter (A, B, C, ...)
                            col_letter = get_column_letter(j)
                            # Excel rows are 1-indexed
                            cell_coord = f"{col_letter}{i+1}"
                            # Add this occurrence to the list
                            all_occurrences[data_value].append((sheet_name, cell_coord))
        
        # Second pass: include all occurrences of each value to improve auto mapping
        for data_value in data_values_str:
            if not data_value or data_value not in all_occurrences:
                continue
                
            occurrences = all_occurrences[data_value]
            
            # If there are no occurrences, skip this value
            if not occurrences:
                continue
                
            # Add all occurrences to the coordinates list
            coordinates.extend(occurrences)
        
        return coordinates
    except Exception as e:
        logger.error(f"Error finding cell coordinates: {e}")
        return []

def retrieve_data_from_column_ranges(file_path: str, column_ranges: List[Tuple[str, str, str, str]]) -> List[str]:
    """
    Retrieve data from an Excel file using column ranges.
    
    Args:
        file_path: The path to the Excel file
        column_ranges: A list of tuples (sheet_name, column_letter, start_row, end_row)
        
    Returns:
        A list of retrieved values from the specified column ranges
    """
    try:
        if not column_ranges:
            return []
            
        result = []
        # Group ranges by sheet for efficiency
        by_sheet = {}
        for sheet_name, column_letter, start_row, end_row in column_ranges:
            if sheet_name not in by_sheet:
                by_sheet[sheet_name] = []
            by_sheet[sheet_name].append((column_letter, start_row, end_row))
        
        # Read each sheet only once
        for sheet_name, ranges in by_sheet.items():
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                for column_letter, start_row, end_row in ranges:
                    # Convert column letter to index
                    col_idx = 0
                    for i, letter in enumerate(reversed(column_letter)):
                        col_idx += (ord(letter.upper()) - 64) * (26 ** i)
                    col_idx -= 1  # Convert to 0-based index
                    
                    # Convert row numbers to 0-based indices
                    try:
                        start_row_idx = int(start_row) - 1
                        end_row_idx = int(end_row) - 1
                    except ValueError:
                        logger.error(f"Invalid row indices: start_row={start_row}, end_row={end_row}")
                        continue
                    
                    # Ensure indices are within bounds
                    start_row_idx = max(0, start_row_idx)
                    end_row_idx = min(len(df) - 1, end_row_idx)
                    
                    if start_row_idx > end_row_idx or col_idx < 0 or col_idx >= df.shape[1]:
                        logger.warning(f"Invalid range: sheet={sheet_name}, column={column_letter}, " 
                                       f"start_row={start_row}, end_row={end_row}")
                        continue
                    
                    # Extract column data for the specified row range
                    column_data = df.iloc[start_row_idx:end_row_idx+1, col_idx].tolist()
                    
                    # Convert values to strings and handle NaN
                    for val in column_data:
                        if pd.isna(val):
                            result.append("")
                        else:
                            result.append(str(val))
                    
            except Exception as e:
                logger.error(f"Error reading sheet {sheet_name}: {e}")
                # Don't add empty values as we don't know how many should be added
                
        return result
    except Exception as e:
        logger.error(f"Error retrieving data from column ranges: {e}")
        return []

def retrieve_data_from_coordinates(file_path: str, coordinates: List[Tuple[str, str]]) -> List[str]:
    """
    Retrieve data from an Excel file using a list of coordinates.
    
    Args:
        file_path: The path to the Excel file
        coordinates: A list of tuples (sheet_name, cell_coordinate) to retrieve data from
        
    Returns:
        A list of retrieved values from the specified coordinates
    """
    try:
        if not coordinates:
            return []
            
        result = []
        # Group coordinates by sheet for efficiency
        by_sheet = {}
        for sheet_name, cell_coord in coordinates:
            if sheet_name not in by_sheet:
                by_sheet[sheet_name] = []
            by_sheet[sheet_name].append(cell_coord)
        
        # Read each sheet only once
        for sheet_name, cell_coords in by_sheet.items():
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                for cell_coord in cell_coords:
                    # Parse the cell coordinate (e.g., "A1")
                    col_letter = ""
                    row_idx = ""
                    for char in cell_coord:
                        if char.isalpha():
                            col_letter += char
                        else:
                            row_idx += char
                    
                    # Convert Excel coordinate to DataFrame indices
                    col_idx = 0
                    for i, letter in enumerate(reversed(col_letter)):
                        col_idx += (ord(letter.upper()) - 64) * (26 ** i)
                    col_idx -= 1  # Convert to 0-based index
                    
                    row_idx = int(row_idx) - 1  # Convert to 0-based index
                    
                    # Get the value if within dataframe bounds
                    if 0 <= row_idx < len(df) and 0 <= col_idx < df.shape[1]:
                        val = df.iloc[row_idx, col_idx]
                        if pd.isna(val):
                            result.append("")
                        else:
                            result.append(str(val))
                    else:
                        result.append("")
            except Exception as e:
                logger.error(f"Error reading sheet {sheet_name}: {e}")
                # Add empty values for all coordinates in this sheet
                result.extend([""] * len(cell_coords))
                
        return result
    except Exception as e:
        logger.error(f"Error retrieving data from coordinates: {e}")
        return []

def get_column_letter(col_idx: int) -> str:
    """
    Convert a zero-based column index to an Excel column letter (A, B, C, ..., Z, AA, AB, ...).
    
    Args:
        col_idx: The zero-based column index
        
    Returns:
        The Excel column letter
    """
    result = ""
    while True:
        quotient = col_idx // 26
        remainder = col_idx % 26
        result = chr(65 + remainder) + result
        if quotient == 0:
            break
        col_idx = quotient - 1
    return result
