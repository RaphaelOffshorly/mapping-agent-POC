import logging
from typing import Dict, List, Any, Optional, Union, Annotated
import pandas as pd
import re
import os
import tempfile
from langchain_core.tools import tool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def load_dataframe_from_csv(file_path):
    """Load a DataFrame from a CSV file."""
    if not os.path.exists(file_path):
        logger.error(f"CSV file not found: {file_path}")
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    logger.info(f"Loaded DataFrame from CSV file: {file_path}, shape={df.shape}")
    return df

def save_dataframe_to_csv(df, file_path):
    """Save a DataFrame to a CSV file."""
    df.to_csv(file_path, index=False)
    logger.info(f"Saved DataFrame to CSV file: {file_path}")
    return file_path

@tool
def csv_info(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get information about the current state of the CSV file, including headers, number of rows, and a preview of the data.
    
    Args:
        state: The current state containing CSV file path
        
    Returns:
        The updated state with information about the CSV file
    """
    logger.info("csv_info tool called")
    logger.info(f"State keys: {list(state.keys())}")
    
    try:
        csv_file_path = state.get('csv_file_path')
        
        if not csv_file_path or not os.path.exists(csv_file_path):
            error_msg = f"CSV file not found: {csv_file_path}"
            logger.error(error_msg)
            state["error"] = error_msg
            return state
        
        # Load the CSV file
        csv_data = load_dataframe_from_csv(csv_file_path)
        
        logger.info(f"CSV data found: shape={csv_data.shape}, dtypes={csv_data.dtypes}")
    
        # Get basic information about the CSV file
        info = {
            "headers": list(csv_data.columns),
            "num_rows": len(csv_data),
            "num_cols": len(csv_data.columns),
            "preview": csv_data.head(5).to_string()
        }
        
        logger.info(f"CSV info: {len(info['headers'])} columns, {info['num_rows']} rows")
        logger.debug(f"CSV headers: {info['headers']}")
        
        # Add the response to the state
        state["tool_response"] = f"CSV Information:\n\nHeaders: {info['headers']}\nNumber of rows: {info['num_rows']}\nNumber of columns: {info['num_cols']}\n\nPreview:\n{info['preview']}"
        
        return state
    except Exception as e:
        error_msg = f"Error in csv_info: {str(e)}"
        logger.error(error_msg, exc_info=True)
        state["error"] = error_msg
        return state

@tool
def csv_bulk_insert(
    state: Dict[str, Any], 
    start_row_index: Annotated[int, "The starting row index to insert at (0-based)"],
    rows_data: Annotated[List[Dict], "List of dictionaries with column name to value pairs for each row"]
) -> Dict[str, Any]:
    """
    Insert multiple new rows into the CSV file starting at the specified row index.
    
    Args:
        state: The current state containing CSV file path
        start_row_index: The starting row index to insert at (0-based)
        rows_data: List of dictionaries with column name to value pairs for each row
        
    Returns:
        The updated state with the modified CSV data
    """
    logger.info(f"csv_bulk_insert tool called with start_row_index={start_row_index}, rows_count={len(rows_data)}")
    logger.debug(f"First row data sample: {rows_data[0] if rows_data else 'No rows provided'}")
    
    csv_file_path = state.get('csv_file_path')
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        error_msg = f"CSV file not found: {csv_file_path}"
        logger.error(error_msg)
        state["error"] = error_msg
        return state
    
    try:
        # Load the CSV file
        csv_data = load_dataframe_from_csv(csv_file_path)
        
        # Log initial shape for debugging
        initial_shape = csv_data.shape
        logger.info(f"Before bulk insert: DataFrame shape is {initial_shape}, columns={list(csv_data.columns)}")
        
        # Check that all columns in values exist in the dataframe
        all_cols = set()
        for row_data in rows_data:
            all_cols.update(row_data.keys())
        
        logger.info(f"Columns in rows_data: {all_cols}")
        logger.info(f"Columns in DataFrame: {set(csv_data.columns)}")
        
        missing_cols = [col for col in all_cols if col not in csv_data.columns]
        if missing_cols:
            error_msg = f"Columns not found in CSV: {missing_cols}"
            logger.error(error_msg)
            state["error"] = error_msg
            return state
        
        # Ensure start_row_index is valid
        original_index = start_row_index
        if start_row_index < 0:
            start_row_index = 0
            logger.warning(f"Adjusted negative start_row_index {original_index} to 0")
        elif start_row_index > len(csv_data):
            start_row_index = len(csv_data)
            logger.warning(f"Adjusted too large start_row_index {original_index} to {len(csv_data)}")
        
        # Insert the rows
        csv_data_before = csv_data.iloc[:start_row_index]
        csv_data_after = csv_data.iloc[start_row_index:]
        
        logger.info(f"Split DataFrame: before={len(csv_data_before)} rows, after={len(csv_data_after)} rows")
        
        # Create DataFrame from rows_data
        new_rows_df = pd.DataFrame(rows_data)
        logger.info(f"Created new rows DataFrame with shape: {new_rows_df.shape}, columns={list(new_rows_df.columns)}")
        
        # Ensure new_rows_df has the same columns as csv_data (in the same order)
        missing_in_new = [col for col in csv_data.columns if col not in new_rows_df.columns]
        if missing_in_new:
            logger.info(f"Adding missing columns to new rows: {missing_in_new}")
            for col in csv_data.columns:
                if col not in new_rows_df.columns:
                    new_rows_df[col] = ""
        
        # Reorder columns to match original
        new_rows_df = new_rows_df[csv_data.columns]
        logger.info(f"Reordered new rows columns to match original: {list(new_rows_df.columns)}")
        
        # Log how many rows we're inserting for debugging
        logger.info(f"Inserting {len(new_rows_df)} rows at index {start_row_index}")
        
        # Concat with the new rows in the middle
        new_csv_data = pd.concat([csv_data_before, new_rows_df, csv_data_after]).reset_index(drop=True)
        logger.info(f"Created concatenated DataFrame with shape: {new_csv_data.shape}")
        
        # Save the updated DataFrame back to the CSV file
        save_dataframe_to_csv(new_csv_data, csv_file_path)
        
        # Log final shape for debugging
        final_shape = new_csv_data.shape
        logger.info(f"After bulk insert: DataFrame shape is now {final_shape}")
        logger.info(f"Row count changed from {initial_shape[0]} to {final_shape[0]}")
        
        state["tool_response"] = f"Successfully inserted {len(rows_data)} new rows starting at index {start_row_index}"
        logger.info("Bulk insert completed successfully")
        
    except Exception as e:
        error_msg = f"Error inserting rows: {str(e)}"
        logger.error(error_msg, exc_info=True)
        state["error"] = error_msg
    
    return state

@tool
def csv_bulk_delete(
    state: Dict[str, Any], 
    row_indices: Annotated[List[int], "List of row indices to delete (0-based)"]
) -> Dict[str, Any]:
    """
    Delete multiple rows from the CSV file based on the specified row indices.
    
    Args:
        state: The current state containing CSV file path
        row_indices: List of row indices to delete (0-based)
        
    Returns:
        The updated state with the modified CSV data
    """
    csv_file_path = state.get('csv_file_path')
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        state["error"] = f"CSV file not found: {csv_file_path}"
        return state
    
    try:
        # Load the CSV file
        csv_data = load_dataframe_from_csv(csv_file_path)
        
        # Log original shape
        logger.info(f"CSV data shape before delete: {csv_data.shape}")
        
        # Ensure all indices are valid
        row_indices = [idx for idx in row_indices if 0 <= idx < len(csv_data)]
        
        if not row_indices:
            state["tool_response"] = "No valid row indices provided, no rows deleted"
            return state
        
        # Delete the rows
        new_csv_data = csv_data.drop(row_indices).reset_index(drop=True)
        
        # Save the updated DataFrame back to the CSV file
        save_dataframe_to_csv(new_csv_data, csv_file_path)
        
        state["tool_response"] = f"Successfully deleted {len(row_indices)} rows"
        
        # Log the new shape
        logger.info(f"CSV data shape after delete: {new_csv_data.shape}")
        
    except Exception as e:
        state["error"] = f"Error deleting rows: {str(e)}"
        logger.error(f"Error in row delete: {e}", exc_info=True)
    
    return state

@tool
def csv_bulk_update(
    state: Dict[str, Any], 
    updates: Annotated[List[Dict[str, Any]], "List of updates, each with row_index, column, and value keys"]
) -> Dict[str, Any]:
    """
    Update multiple cells in the CSV file with the provided values.
    
    Args:
        state: The current state containing CSV file path
        updates: List of updates, each with row_index, column, and value keys
        
    Returns:
        The updated state with the modified CSV data
    """
    logger.info(f"csv_bulk_update tool called with {len(updates)} updates")
    if updates:
        logger.debug(f"First update: row_index={updates[0].get('row_index')}, column={updates[0].get('column')}, value={updates[0].get('value')}")
    
    csv_file_path = state.get('csv_file_path')
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        error_msg = f"CSV file not found: {csv_file_path}"
        logger.error(error_msg)
        state["error"] = error_msg
        return state
    
    try:
        # Load the CSV file
        csv_data = load_dataframe_from_csv(csv_file_path)
        
        logger.info(f"CSV data shape before bulk update: {csv_data.shape}, columns={list(csv_data.columns)}")
        
        # Make a copy of the DataFrame to ensure we don't modify the original
        new_csv_data = csv_data.copy()
        
        successful_updates = 0
        failed_updates = 0
        failed_details = []
        
        for i, update in enumerate(updates):
            row_index = update.get('row_index')
            column = update.get('column')
            value = update.get('value')
            
            # Check that all required fields are present
            if row_index is None or column is None or 'value' not in update:
                error = f"Update {i}: Missing required fields"
                logger.warning(error)
                failed_details.append(error)
                failed_updates += 1
                continue
            
            # Check that the column exists
            if column not in new_csv_data.columns:
                error = f"Update {i}: Column '{column}' not found in DataFrame"
                logger.warning(error)
                failed_details.append(error)
                failed_updates += 1
                continue
            
            # Check that the row index is valid
            if row_index < 0 or row_index >= len(new_csv_data):
                error = f"Update {i}: Row index {row_index} out of bounds (0-{len(new_csv_data)-1})"
                logger.warning(error)
                failed_details.append(error)
                failed_updates += 1
                continue
            
            # Update the cell
            logger.debug(f"Updating cell [{row_index}, {column}] to {value}")
            new_csv_data.at[row_index, column] = value
            successful_updates += 1
        
        # Save the updated DataFrame back to the CSV file
        save_dataframe_to_csv(new_csv_data, csv_file_path)
        
        state["tool_response"] = f"Successfully updated {successful_updates} cells, {failed_updates} updates failed"
        
        logger.info(f"Completed bulk update with {successful_updates} successful updates, {failed_updates} failed")
        if failed_details:
            logger.info(f"Failed update details: {failed_details}")
        
    except Exception as e:
        error_msg = f"Error updating cells: {str(e)}"
        logger.error(error_msg, exc_info=True)
        state["error"] = error_msg
    
    return state

@tool
def csv_bulk_add_column(
    state: Dict[str, Any], 
    columns_data: Annotated[List[Dict[str, Any]], "List of column definitions, each with name and optional values"]
) -> Dict[str, Any]:
    """
    Add multiple new columns to the CSV file with the provided names and optional values.
    
    Args:
        state: The current state containing CSV file path
        columns_data: List of column definitions, each with name and optional values
        
    Returns:
        The updated state with the modified CSV data
    """
    csv_file_path = state.get('csv_file_path')
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        state["error"] = f"CSV file not found: {csv_file_path}"
        return state
    
    try:
        # Load the CSV file
        csv_data = load_dataframe_from_csv(csv_file_path)
        
        logger.info(f"CSV data shape before bulk add columns: {csv_data.shape}")
        
        # Make a copy of the DataFrame to ensure we don't modify the original
        new_csv_data = csv_data.copy()
        
        successful_adds = 0
        failed_adds = 0
        
        for column_def in columns_data:
            column_name = column_def.get('name')
            values = column_def.get('values')
            
            # Check that column name is provided
            if not column_name:
                failed_adds += 1
                continue
            
            # Check if the column already exists
            if column_name in new_csv_data.columns:
                failed_adds += 1
                continue
            
            # Create values list if not provided
            if values is None:
                values = ["" for _ in range(len(new_csv_data))]
            
            # Ensure values list is the right length
            if len(values) < len(new_csv_data):
                values.extend(["" for _ in range(len(new_csv_data) - len(values))])
            elif len(values) > len(new_csv_data):
                values = values[:len(new_csv_data)]
            
            # Add the column
            new_csv_data[column_name] = values
            successful_adds += 1
        
        # Save the updated DataFrame back to the CSV file
        save_dataframe_to_csv(new_csv_data, csv_file_path)
        
        state["tool_response"] = f"Successfully added {successful_adds} columns, {failed_adds} additions failed"
        
        logger.info(f"CSV data shape after bulk add columns: {new_csv_data.shape}, added {successful_adds} columns")
        
    except Exception as e:
        state["error"] = f"Error adding columns: {str(e)}"
        logger.error(f"Error in bulk add columns: {e}", exc_info=True)
    
    return state

@tool
def excel_coordinate_extractor(
    state: Dict[str, Any], 
    coordinates: Annotated[str, "Excel coordinates (e.g., 'B7-B10' or 'A1:C5')"]
) -> Dict[str, Any]:
    """
    Extract data from the source file at the specified Excel coordinates.
    
    Args:
        state: The current state containing source data
        coordinates: Excel coordinates (e.g., 'B7-B10' or 'A1:C5')
        
    Returns:
        The updated state with the extracted data
    """
    source_data = state.get('source_data')
    
    if source_data is None or not isinstance(source_data, Dict):
        state["error"] = "No valid source data found in state"
        return state
    
    try:
        # Parse coordinates
        # Support formats like "B7-B10", "B7:B10", "A1:C5", etc.
        match = re.search(r'([A-Z]+)(\d+)[-:]([A-Z]+)(\d+)', coordinates.upper())
        
        if not match:
            state["error"] = f"Invalid coordinate format: {coordinates}"
            return state
        
        start_col, start_row, end_col, end_row = match.groups()
        start_row, end_row = int(start_row), int(end_row)
        
        # Convert column letters to indices (A=0, B=1, etc.)
        def col_to_idx(col_str):
            result = 0
            for char in col_str:
                result = result * 26 + (ord(char) - ord('A') + 1)
            return result - 1
        
        start_col_idx = col_to_idx(start_col)
        end_col_idx = col_to_idx(end_col)
        
        # Find the right sheet in source data
        sheet_data = None
        sheet_name = None
        
        for name, data in source_data.items():
            if isinstance(data, pd.DataFrame):
                sheet_data = data
                sheet_name = name
                break
        
        if sheet_data is None:
            state["error"] = "No valid sheet data found in source data"
            return state
        
        # Adjust for 1-based indexing in Excel vs 0-based in pandas
        start_row -= 1
        end_row -= 1
        
        # Extract data
        extracted_data = []
        
        # Handle range extraction
        for row in range(start_row, end_row + 1):
            row_data = []
            for col in range(start_col_idx, end_col_idx + 1):
                if row < len(sheet_data) and col < len(sheet_data.columns):
                    value = sheet_data.iloc[row, col]
                    row_data.append(value)
                else:
                    row_data.append("")
            extracted_data.append(row_data)
        
        # Flatten if it's a single column
        if start_col_idx == end_col_idx:
            extracted_data = [row[0] for row in extracted_data]
        
        # Update the state
        state["extracted_data"] = extracted_data
        state["tool_response"] = f"Successfully extracted data from {coordinates} in sheet '{sheet_name}'"
        
    except Exception as e:
        state["error"] = f"Error extracting data from coordinates: {str(e)}"
    
    return state
