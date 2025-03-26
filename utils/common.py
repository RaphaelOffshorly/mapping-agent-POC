import re
import json
import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_json_string(json_str: str) -> str:
    """
    Clean a JSON string to make it valid for parsing.
    
    Args:
        json_str: The JSON string to clean
        
    Returns:
        A cleaned JSON string
    """
    # Remove trailing commas
    json_str = re.sub(r',\s*([\]}])', r'\1', json_str).strip()
    
    # Fix unbalanced braces if necessary
    missing_braces = json_str.count("{") - json_str.count("}")
    if missing_braces > 0:
        json_str += "}" * missing_braces
    
    # Fix unbalanced quotes if necessary
    if len(re.findall(r'(?<!\\)"', json_str)) % 2 != 0:
        json_str += '"'
    
    return json_str

def extract_json_from_text(text: str) -> Optional[str]:
    """
    Extract a JSON object from text.
    
    Args:
        text: The text containing a JSON object
        
    Returns:
        The extracted JSON string or None if not found
    """
    json_pattern = r'\{[\s\S]*\}'
    json_match = re.search(json_pattern, text)
    
    if json_match:
        json_str = json_match.group()
        return clean_json_string(json_str)
    
    # Try to find a JSON array
    json_array_pattern = r'\[[\s\S]*\]'
    json_array_match = re.search(json_array_pattern, text)
    
    if json_array_match:
        json_str = json_array_match.group()
        return clean_json_string(json_str)
    
    return None

def parse_json_response(response_text: str) -> Optional[Union[Dict, List]]:
    """
    Parse a JSON response from text.
    
    Args:
        response_text: The text containing a JSON object
        
    Returns:
        The parsed JSON object or None if parsing fails
    """
    try:
        json_str = extract_json_from_text(response_text)
        if json_str:
            return json.loads(json_str)
        return None
    except Exception as e:
        logger.error(f"Error parsing JSON response: {e}")
        logger.error(f"Raw response: {response_text}")
        return None

def infer_header_row(df: pd.DataFrame, header_scan_rows: int = 10) -> Optional[int]:
    """
    Infer the most likely header row index in the DataFrame by computing the ratio of non-numeric,
    non-empty cells for the first few rows.
    
    Args:
        df: The DataFrame to analyze
        header_scan_rows: Number of rows to consider when inferring header
        
    Returns:
        The index of the most likely header row or None if not found
    """
    best_row = None
    best_score = 0
    
    for i in range(min(header_scan_rows, len(df))):
        row = df.iloc[i, :]
        # Count cells that are strings and not purely numeric or empty
        valid_cells = [val for val in row if isinstance(val, str) and val.strip() and not val.strip().isdigit()]
        score = len(valid_cells) / len(row) if len(row) > 0 else 0
        
        if score > best_score:
            best_score = score
            best_row = i
    
    return best_row

def extract_from_inferred_header(df: pd.DataFrame, header_index: Optional[int]) -> List[str]:
    """
    Extract headers from the inferred header row.
    
    Args:
        df: The DataFrame to extract headers from
        header_index: The index of the header row
        
    Returns:
        A list of header values
    """
    if header_index is not None:
        header_values = df.iloc[header_index].astype(str).tolist()
        return [val.strip() for val in header_values if val.strip()]
    return []
