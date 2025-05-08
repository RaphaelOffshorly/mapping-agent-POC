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
    
    # Use keep_default_na=False to prevent pandas from converting empty strings to NaN
    df = pd.read_csv(file_path, keep_default_na=False)
    logger.info(f"Loaded DataFrame from CSV file: {file_path}, shape={df.shape}")
    
    # Log the first row for debugging
    if not df.empty:
        logger.info(f"First row after loading: {df.iloc[0].to_dict()}")
    
    return df

def sanitize_edit_function(func_code):
    """
    Sanitize the function code to ensure it correctly defines an edit_dataframe function.
    
    Args:
        func_code: String containing the function definition
        
    Returns:
        Sanitized function code
    """
    # Check if the code defines a function
    import re
    
    logger.info("Sanitizing edit function code")
    
    # If code doesn't have the 'def edit_dataframe(' declaration, try to fix it
    if not re.search(r'def\s+edit_dataframe\s*\(\s*df\s*\)\s*:', func_code):
        # Check if there's any function defined
        function_match = re.search(r'def\s+(\w+)\s*\(\s*df\s*\)\s*:', func_code)
        if function_match:
            # If a function is defined but with a different name, rename it
            wrong_name = function_match.group(1)
            logger.info(f"Found function with name '{wrong_name}' instead of 'edit_dataframe', renaming")
            func_code = re.sub(r'def\s+' + wrong_name + r'\s*\(', 'def edit_dataframe(', func_code)
        else:
            # If no function is defined, wrap the code in a function
            logger.info("No function definition found, wrapping code in edit_dataframe function")
            # Indent all lines
            indented_lines = [
                "    " + line if line.strip() else line 
                for line in func_code.split('\n')
            ]
            
            # Return statement if not already present
            if not any(re.search(r'\s*return\s+df', line) for line in indented_lines):
                indented_lines.append("    return df")
            
            func_code = "def edit_dataframe(df):\n" + "\n".join(indented_lines)
    
    # Ensure the function returns something (preferably the dataframe)
    if not re.search(r'\s*return\s+', func_code):
        logger.info("No return statement found, adding return df at the end")
        # Find the last line with content to determine indentation
        lines = func_code.split('\n')
        indentation = ""
        for line in reversed(lines):
            if line.strip():
                # Extract indentation
                indentation_match = re.match(r'^(\s*)', line)
                if indentation_match:
                    indentation = indentation_match.group(1)
                break
        
        # Add return statement at the end
        func_code += f"\n{indentation}return df"
    
    logger.info(f"Sanitized function code: {func_code[:100]}...")
    return func_code

def save_dataframe_to_csv(df, file_path):
    """Save a DataFrame to a CSV file."""
    # Use na_rep='' to ensure empty strings are preserved
    df.to_csv(file_path, index=False, na_rep='')
    logger.info(f"Saved DataFrame to CSV file: {file_path}")
    
    # Log the first row for debugging
    if not df.empty:
        logger.info(f"First row before saving: {df.iloc[0].to_dict()}")
    
    # Explicitly flush file buffers to ensure changes are written to disk
    import os
    os.fsync(os.open(file_path, os.O_RDONLY))
    
    # Verify the file was written correctly
    verification_df = pd.read_csv(file_path)
    logger.info(f"Verification after save: DataFrame has shape {verification_df.shape}")
    
    return file_path

@tool
def csv_info(csv_file_path: str) -> str:
    """
    Get information about the current state of the CSV file, including headers, number of rows, and a preview of the data.
    
    Args:
        csv_file_path: Path to the CSV file
        
    Returns:
        Information about the CSV file
    """
    logger.info("csv_info tool called")
    
    try:
        if not csv_file_path or not os.path.exists(csv_file_path):
            error_msg = f"CSV file not found: {csv_file_path}"
            logger.error(error_msg)
            return error_msg
        
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
        
        # Format the response
        response = f"CSV Information:\n\nHeaders: {info['headers']}\nNumber of rows: {info['num_rows']}\nNumber of columns: {info['num_cols']}\n\nPreview:\n{info['preview']}"
        
        return response
    except Exception as e:
        error_msg = f"Error in csv_info: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return error_msg

@tool
def csv_bulk_insert(
    csv_file_path: str, 
    start_row_index: Annotated[int, "The starting row index to insert at (0-based)"],
    rows_data: Annotated[List[Dict], "List of dictionaries with column name to value pairs for each row"]
) -> str:
    """
    Insert multiple new rows into the CSV file starting at the specified row index.
    
    Args:
        csv_file_path: Path to the CSV file
        start_row_index: The starting row index to insert at (0-based)
        rows_data: List of dictionaries with column name to value pairs for each row
        
    Returns:
        Result of the operation
    """
    logger.info(f"csv_bulk_insert tool called with start_row_index={start_row_index}, rows_count={len(rows_data)}")
    logger.info(f"Rows data: {rows_data}")
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        error_msg = f"CSV file not found: {csv_file_path}"
        logger.error(error_msg)
        return error_msg
    
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
            return error_msg
        
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
        
        response = f"Successfully inserted {len(rows_data)} new rows starting at index {start_row_index}. The CSV now has {final_shape[0]} rows and {final_shape[1]} columns."
        logger.info("Bulk insert completed successfully")
        return response
        
    except Exception as e:
        error_msg = f"Error inserting rows: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return error_msg

@tool
def csv_bulk_delete(
    csv_file_path: str, 
    row_indices: Annotated[List[int], "List of row indices to delete (0-based)"]
) -> str:
    """
    Delete multiple rows from the CSV file based on the specified row indices.
    
    Args:
        csv_file_path: Path to the CSV file
        row_indices: List of row indices to delete (0-based)
        
    Returns:
        Result of the operation
    """
    if not csv_file_path or not os.path.exists(csv_file_path):
        error_msg = f"CSV file not found: {csv_file_path}"
        return error_msg
    
    try:
        # Load the CSV file
        csv_data = load_dataframe_from_csv(csv_file_path)
        
        # Log original shape
        logger.info(f"CSV data shape before delete: {csv_data.shape}")
        
        # Ensure all indices are valid
        row_indices = [idx for idx in row_indices if 0 <= idx < len(csv_data)]
        
        if not row_indices:
            return "No valid row indices provided, no rows deleted"
        
        # Delete the rows
        new_csv_data = csv_data.drop(row_indices).reset_index(drop=True)
        
        # Save the updated DataFrame back to the CSV file
        save_dataframe_to_csv(new_csv_data, csv_file_path)
        
        response = f"Successfully deleted {len(row_indices)} rows. The CSV now has {new_csv_data.shape[0]} rows and {new_csv_data.shape[1]} columns."
        
        # Log the new shape
        logger.info(f"CSV data shape after delete: {new_csv_data.shape}")
        return response
        
    except Exception as e:
        error_msg = f"Error deleting rows: {str(e)}"
        logger.error(f"Error in row delete: {e}", exc_info=True)
        return error_msg

@tool
def csv_bulk_update(
    csv_file_path: str, 
    updates: Annotated[List[Dict[str, Any]], "List of updates, each with row_index, column, and value keys"]
) -> str:
    """
    Update multiple cells in the CSV file with the provided values.
    
    Args:
        csv_file_path: Path to the CSV file
        updates: List of updates, each with row_index, column, and value keys
        
    Returns:
        Result of the operation
    """
    logger.info(f"csv_bulk_update tool called with {len(updates)} updates")
    if updates:
        logger.debug(f"First update: row_index={updates[0].get('row_index')}, column={updates[0].get('column')}, value={updates[0].get('value')}")
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        error_msg = f"CSV file not found: {csv_file_path}"
        logger.error(error_msg)
        return error_msg
    
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
        
        response = f"Successfully updated {successful_updates} cells, {failed_updates} updates failed. The CSV now has {new_csv_data.shape[0]} rows and {new_csv_data.shape[1]} columns."
        
        logger.info(f"Completed bulk update with {successful_updates} successful updates, {failed_updates} failed")
        if failed_details:
            logger.info(f"Failed update details: {failed_details}")
        return response
        
    except Exception as e:
        error_msg = f"Error updating cells: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return error_msg

@tool
def csv_bulk_add_column(
    csv_file_path: str, 
    columns_data: Annotated[List[Dict[str, Any]], "List of column definitions, each with name and optional values"]
) -> str:
    """
    Add multiple new columns to the CSV file with the provided names and optional values.
    
    Args:
        csv_file_path: Path to the CSV file
        columns_data: List of column definitions, each with name and optional values
        
    Returns:
        Result of the operation
    """
    if not csv_file_path or not os.path.exists(csv_file_path):
        error_msg = f"CSV file not found: {csv_file_path}"
        return error_msg
    
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
        
        response = f"Successfully added {successful_adds} columns, {failed_adds} additions failed. The CSV now has {new_csv_data.shape[0]} rows and {new_csv_data.shape[1]} columns."
        
        logger.info(f"CSV data shape after bulk add columns: {new_csv_data.shape}, added {successful_adds} columns")
        return response
        
    except Exception as e:
        error_msg = f"Error adding columns: {str(e)}"
        logger.error(f"Error in bulk add columns: {e}", exc_info=True)
        return error_msg

@tool
def excel_coordinate_extractor(
    source_data: Dict[str, Any], 
    coordinates: Annotated[str, "Excel coordinates (e.g., 'B7-B10' or 'A1:C5')"]
) -> Dict[str, Any]:
    """
    Extract data from the source file at the specified Excel coordinates.
    
    Args:
        source_data: Dictionary containing source data
        coordinates: Excel coordinates (e.g., 'B7-B10' or 'A1:C5')
        
    Returns:
        Dictionary containing the extracted data
    """
    if source_data is None or not isinstance(source_data, Dict):
        return {"error": "No valid source data provided", "extracted_data": None}
    
    try:
        # Parse coordinates
        # Support formats like "B7-B10", "B7:B10", "A1:C5", etc.
        match = re.search(r'([A-Z]+)(\d+)[-:]([A-Z]+)(\d+)', coordinates.upper())
        
        if not match:
            return {"error": f"Invalid coordinate format: {coordinates}", "extracted_data": None}
        
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
            return {"error": "No valid sheet data found in source data", "extracted_data": None}
        
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
        
        return {
            "extracted_data": extracted_data,
            "message": f"Successfully extracted data from {coordinates} in sheet '{sheet_name}'"
        }
        
    except Exception as e:
        return {"error": f"Error extracting data from coordinates: {str(e)}", "extracted_data": None}

@tool
def csv_pandas_eval(
    csv_file_path: str,
    verification_code: str
) -> str:
    """
    Execute a verification code block on the CSV file to evaluate data without modifying it.
    The function should define 'verify_dataframe(df)' that returns verification results.
    The DataFrame is available as 'df' inside your function.
    
    Args:
        csv_file_path: Path to the CSV file
        verification_code: A code block that defines a function named 'verify_dataframe' that takes a DataFrame parameter and returns verification results
    
    Returns:
        The result of the verification or an error message.
    """
    import pandas as pd
    import numpy as np
    import builtins
    import traceback
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        return f"CSV file not found: {csv_file_path}"
        
    try:
        # Load the CSV file into a pandas DataFrame
        df = load_dataframe_from_csv(csv_file_path)
        logger.info(f"Loaded DataFrame for verification: {csv_file_path}, shape={df.shape}")
        
        # Define a safe import function that only allows importing specific modules
        def safe_import(name, *args, **kwargs):
            # Only allow importing specific modules
            allowed_modules = {'pandas', 'numpy', 'math', 're', 'datetime', 'builtins'}
            if name in allowed_modules:
                return __import__(name, *args, **kwargs)
            else:
                raise ImportError(f"Import of module '{name}' is not allowed for security reasons")
                
        # Create a safe execution environment with all necessary globals
        safe_globals = {
            "__builtins__": {
                "len": len,
                "range": range,
                "min": min,
                "max": max,
                "sum": sum,
                "list": list,
                "dict": dict,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "set": set,
                "tuple": tuple,
                "zip": zip,
                "enumerate": enumerate,
                "map": map,
                "filter": filter,
                "sorted": sorted,
                "reversed": reversed,
                "math": __import__('math'),  # Allow math module
                "re": __import__('re'),      # Allow regex module
                "datetime": __import__('datetime'),  # Allow datetime module
                "json": __import__('json'),  # Allow JSON module
                "print": print,  # Allow print for debugging
                "all": all,      # Add all() function
                "any": any,      # Add any() function
                "abs": abs,      # Add abs() function
                "round": round,  # Add round() function
                "__import__": safe_import,  # Add restricted import capability
                "isinstance": isinstance,   # Add isinstance function
                "issubclass": issubclass,   # Add issubclass function
                "type": type                # Add type function
            },
            "pd": pd,
            "np": np,
            "DataFrame": pd.DataFrame,
            "Series": pd.Series,
            "concat": pd.concat,
            "len": len,
            "range": range,
            "df": df  # Make df available in the global scope
        }
        
        # Similar to sanitize_edit_function but for verify_dataframe
        def sanitize_verify_function(code):
            import re
            import ast
            
            logger.info("Sanitizing verify function code")
            
            # Check for unmatched braces
            def check_balanced_braces(code_str):
                stack = []
                braces = {'}': '{', ']': '[', ')': '('}
                for i, char in enumerate(code_str):
                    if char in '{[(':
                        stack.append(char)
                    elif char in '}])':
                        if not stack or stack.pop() != braces[char]:
                            # Found unmatched closing brace
                            return i, char
                if stack:
                    # Found unmatched opening brace
                    return -1, stack[-1]
                return -1, None
                
            # First check for unmatched braces
            pos, brace = check_balanced_braces(code)
            if pos >= 0:
                # Found unmatched closing brace, remove it
                logger.warning(f"Found unmatched closing brace '{brace}' at position {pos}, removing it")
                code = code[:pos] + code[pos+1:]
            
            # If code doesn't have the 'def verify_dataframe(' declaration, try to fix it
            if not re.search(r'def\s+verify_dataframe\s*\(\s*df\s*\)\s*:', code):
                # Check if there's any function defined
                function_match = re.search(r'def\s+(\w+)\s*\(\s*df\s*\)\s*:', code)
                if function_match:
                    # If a function is defined but with a different name, rename it
                    wrong_name = function_match.group(1)
                    logger.info(f"Found function with name '{wrong_name}' instead of 'verify_dataframe', renaming")
                    code = re.sub(r'def\s+' + wrong_name + r'\s*\(', 'def verify_dataframe(', code)
                else:
                    # If no function is defined, wrap the code in a function
                    logger.info("No function definition found, wrapping code in verify_dataframe function")
                    # Indent all lines
                    indented_lines = [
                        "    " + line if line.strip() else line 
                        for line in code.split('\n')
                    ]
                    
                    # Return statement if not already present
                    if not any(re.search(r'\s*return\s+', line) for line in indented_lines):
                        indented_lines.append("    return result")
                    
                    code = "def verify_dataframe(df):\n    result = None\n" + "\n".join(indented_lines)
            
            # Ensure the function returns something
            if not re.search(r'\s*return\s+', code):
                logger.info("No return statement found, adding return result at the end")
                # Find the last line with content to determine indentation
                lines = code.split('\n')
                indentation = ""
                for line in reversed(lines):
                    if line.strip():
                        # Extract indentation
                        indentation_match = re.match(r'^(\s*)', line)
                        if indentation_match:
                            indentation = indentation_match.group(1)
                        break
                
                # Add return statement at the end
                code += f"\n{indentation}return result"
            
            # Check for return statements with unmatched braces in dictionaries
            lines = code.split('\n')
            for i, line in enumerate(lines):
                if re.search(r'\s*return\s+', line) and '{' in line:
                    # Count opening and closing braces in this line
                    open_count = line.count('{')
                    close_count = line.count('}')
                    if close_count > open_count:
                        # Too many closing braces, remove extras
                        excess = close_count - open_count
                        logger.warning(f"Found {excess} extra closing braces in return statement, fixing")
                        # Remove last 'excess' closing braces
                        new_line = line
                        for _ in range(excess):
                            last_brace = new_line.rindex('}')
                            new_line = new_line[:last_brace] + new_line[last_brace+1:]
                        lines[i] = new_line
            
            # Rejoin the lines
            code = '\n'.join(lines)
            
            # Try to parse the code to check for syntax errors
            try:
                ast.parse(code)
            except SyntaxError as e:
                logger.warning(f"Syntax error in verification code: {e}")
                # If we have a syntax error about unmatched braces, try to fix it
                if "unmatched '}'" in str(e):
                    # If it's about a specific line, try to fix that line
                    if hasattr(e, 'lineno') and e.lineno > 0 and e.lineno <= len(lines):
                        line = lines[e.lineno - 1]
                        if '}' in line:
                            # Remove the last closing brace
                            last_brace = line.rindex('}')
                            lines[e.lineno - 1] = line[:last_brace] + line[last_brace+1:]
                            code = '\n'.join(lines)
                            logger.info(f"Fixed unmatched brace on line {e.lineno}")
            
            logger.info(f"Sanitized verify function code: {code[:100]}...")
            return code
        
        # Sanitize the verification code
        sanitized_code = sanitize_verify_function(verification_code)
        logger.info(f"Verification code sanitized: {sanitized_code[:100]}...")
        
        try:
            # Execute the function definition
            exec(sanitized_code, safe_globals)
            
            # Get the function from globals
            if 'verify_dataframe' not in safe_globals:
                logger.error("Function 'verify_dataframe' not found after executing code")
                return "Error: The verification_code must define a function called 'verify_dataframe'"
                
            verify_function = safe_globals['verify_dataframe']
        except Exception as e:
            logger.error(f"Error executing verification function definition: {e}", exc_info=True)
            return f"Error executing verification function definition: {str(e)}\nTraceback:\n{traceback.format_exc()}"
        
        # Call the verification function with the DataFrame
        verification_result = verify_function(df)
        logger.info(f"Verification function executed, result type: {type(verification_result).__name__}")
        
        # Convert result to string for return
        if isinstance(verification_result, pd.DataFrame):
            return verification_result.to_string()
        elif isinstance(verification_result, pd.Series):
            return verification_result.to_string()
        else:
            return str(verification_result)
            
    except Exception as e:
        logger.error(f"Error in csv_pandas_eval: {str(e)}", exc_info=True)
        return f"Error verifying CSV: {str(e)}\nTraceback:\n{traceback.format_exc()}"

@tool
def csv_pandas_edit(
    csv_file_path: str,
    pandas_edit_command: str
) -> str:
    """
    Execute a pandas command to edit the CSV file. The function should receive a DataFrame and return a modified DataFrame.
    The function signature should be: def edit_dataframe(df): ... return modified_df
    
    Args:
        csv_file_path: Path to the CSV file
        pandas_edit_command: The pandas command to execute (as a string). Should define a function that takes a DataFrame and returns a modified DataFrame.
    
    Returns:
        Success message or error.
    """
    import pandas as pd
    import numpy as np
    import builtins
    import traceback
    
    if not csv_file_path or not os.path.exists(csv_file_path):
        return f"CSV file not found: {csv_file_path}"
        
    try:
        # Load the CSV file into a pandas DataFrame
        df_original = pd.read_csv(csv_file_path)
        logger.info(f"Loaded DataFrame from CSV file: {csv_file_path}, shape={df_original.shape}")
        if len(df_original) > 0:
            logger.info(f"First row after loading: {df_original.iloc[0].to_dict()}")
        
        # Define a safe import function that only allows importing specific modules
        def safe_import(name, *args, **kwargs):
            # Only allow importing specific modules
            allowed_modules = {'pandas', 'numpy', 'math', 're', 'datetime', 'builtins'}
            if name in allowed_modules:
                return __import__(name, *args, **kwargs)
            else:
                raise ImportError(f"Import of module '{name}' is not allowed for security reasons")
                
        # Create a safe execution environment with all necessary globals
        safe_globals = {
            "__builtins__": {
                "len": len,
                "range": range,
                "min": min,
                "max": max,
                "sum": sum,
                "list": list,
                "dict": dict,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "set": set,
                "tuple": tuple,
                "zip": zip,
                "enumerate": enumerate,
                "map": map,
                "filter": filter,
                "sorted": sorted,
                "reversed": reversed,
                "math": __import__('math'),  # Allow math module
                "re": __import__('re'),      # Allow regex module
                "datetime": __import__('datetime'),  # Allow datetime module
                "json": __import__('json'),  # Allow JSON module
                "print": print,  # Allow print for debugging
                "all": all,      # Add all() function
                "any": any,      # Add any() function
                "abs": abs,      # Add abs() function
                "round": round,  # Add round() function
                "__import__": safe_import,  # Add restricted import capability
                "isinstance": isinstance,   # Add isinstance function
                "issubclass": issubclass,   # Add issubclass function
                "type": type                # Add type function
            },
            "pd": pd,
            "np": np,
            "DataFrame": pd.DataFrame,
            "Series": pd.Series,
            "concat": pd.concat,
            "len": len,
            "range": range
        }
        
        # Sanitize the pandas_edit_command to ensure it defines edit_dataframe correctly
        sanitized_command = sanitize_edit_function(pandas_edit_command)
        logger.info(f"Sanitized edit function: {sanitized_command[:100]}...")
        
        try:
            # Execute the function definition
            exec(sanitized_command, safe_globals)
            
            # Get the function from globals
            if 'edit_dataframe' not in safe_globals:
                logger.error("Function 'edit_dataframe' not found after executing command")
                return "Error: The pandas_edit_command must define a function called 'edit_dataframe'"
                
            edit_function = safe_globals['edit_dataframe']
        except Exception as e:
            logger.error(f"Error executing function definition: {e}", exc_info=True)
            return f"Error executing function definition: {str(e)}"
        
        # Make a copy to work with
        df = df_original.copy()
        logger.info(f"Before executing edit function: DataFrame has shape {df.shape}")
        
        # Call the function with the DataFrame
        modified_df = edit_function(df)
        if modified_df is None:
            return "Error: The edit_dataframe function must return a DataFrame"
        
        # Debug the DataFrame contents before and after command execution
        logger.info(f"Original DataFrame: {df.shape}, has {len(df)} rows")
        logger.info(f"Modified DataFrame: {modified_df.shape}, has {len(modified_df)} rows")
        if 'Last Name' in df.columns and 'Last Name' in modified_df.columns:
            logger.info(f"Original Last Name values: {df['Last Name'].tolist()}")
            logger.info(f"Modified Last Name values: {modified_df['Last Name'].tolist()}")
        
        # Log the resulting dataframe structure and values for debugging
        if 'Last Name' in modified_df.columns:
            logger.info(f"After command execution - Last Name values: {modified_df['Last Name'].head(5).tolist()}")

        logger.info(f"After executing command: DataFrame has shape {modified_df.shape}")

        # Log the DataFrame content after modification for debugging
        if not modified_df.empty:
            logger.info(f"DataFrame after modification - first row: {modified_df.iloc[0].to_dict()}")
            if 'Last Name' in modified_df.columns:
                logger.info(f"DataFrame after modification - first 5 rows of 'Last Name' column: {modified_df['Last Name'].head(5).tolist()}")

        # Save the modified DataFrame back to the CSV
        logger.info(f"Saving DataFrame to CSV file: {csv_file_path}")
        modified_df.to_csv(csv_file_path, index=False)
        logger.info(f"DataFrame saved to CSV")
        
        # Force flush the file to disk
        with open(csv_file_path, 'a') as f:
            f.flush()
            os.fsync(f.fileno())
            
        # Double-check by reading the file back
        verification_df = pd.read_csv(csv_file_path)
        logger.info(f"Final verification: DataFrame read back from file has shape {verification_df.shape}")
        if not verification_df.empty and 'Last Name' in verification_df.columns:
            logger.info(f"Final verification: 'Last Name' column values: {verification_df['Last Name'].head(10).tolist()}")

        # Log the final state of the DataFrame without modifying it
        if 'Last Name' in verification_df.columns and len(verification_df) > 0:
            last_name_values = verification_df['Last Name'].tolist()
            logger.info(f"Final verification - Last Name column values: {last_name_values[:10]}")

        return "Edit successful. DataFrame saved to CSV."
    except Exception as e:
        logger.error(f"Error in csv_pandas_edit: {str(e)}", exc_info=True)
        return f"Error editing CSV: {str(e)}\nTraceback:\n{traceback.format_exc()}"
