import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple

from tools.base_tool import BaseTool
from utils.common import infer_header_row, extract_from_inferred_header
from utils.excel import read_excel_file
from config.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HeaderExtractionTool(BaseTool[str, List[str]]):
    """Tool for extracting potential headers from Excel files."""
    
    # Define format types
    FORMAT_CONVENTIONAL = "conventional_table"
    FORMAT_KEY_VALUE = "key_value_pairs"
    FORMAT_MIXED = "mixed_format"
    
    def __init__(self):
        """Initialize the header extraction tool."""
        super().__init__(
            name="header_extractor",
            description="Extracts potential headers from Excel files"
        )
        self.header_scan_rows = Config.HEADER_SCAN_ROWS
        self.cell_scan_rows = Config.CELL_SCAN_ROWS
        self.cell_scan_cols = Config.CELL_SCAN_COLS
    
    def detect_excel_format(self, df: pd.DataFrame) -> Tuple[str, Optional[int], List[Tuple[str, str]]]:
        """
        Detect if an Excel sheet is in conventional table format, key-value format, or mixed format.
        
        Args:
            df: The DataFrame representing the Excel sheet
            
        Returns:
            A tuple of (format_type, header_row_index, key_value_pairs)
            - format_type: One of FORMAT_CONVENTIONAL, FORMAT_KEY_VALUE, FORMAT_MIXED
            - header_row_index: Index of the header row for conventional table (None if not applicable)
            - key_value_pairs: List of (key, value) tuples for key-value pairs format (empty if not applicable)
        """
        # First, try to detect conventional table format
        header_row_index = infer_header_row(df, self.header_scan_rows)
        
        # Check for key-value pairs pattern
        key_value_pairs = []
        key_value_pattern_count = 0
        
        # The typical pattern is label in column A, value in column B
        for i in range(min(20, len(df))):  # Check first 20 rows
            if df.shape[1] >= 2:  # Ensure at least 2 columns
                key = str(df.iloc[i, 0]).strip() if pd.notna(df.iloc[i, 0]) else ""
                value = str(df.iloc[i, 1]).strip() if pd.notna(df.iloc[i, 1]) else ""
                
                # Check if this looks like a key-value pair (non-empty key with some value)
                if key and len(key) > 1:
                    key_value_pairs.append((key, value))
                    key_value_pattern_count += 1
        
        # Detect zigzag key-value patterns (like in the shown image)
        zigzag_pattern_count = 0
        for i in range(min(20, len(df))):
            if df.shape[1] >= 4:  # Need at least 4 columns for zigzag pattern
                # Look for pattern where there are keys/values in columns A/B and C/D
                left_key = str(df.iloc[i, 0]).strip() if pd.notna(df.iloc[i, 0]) else ""
                right_key = str(df.iloc[i, 2]).strip() if pd.notna(df.iloc[i, 2]) else ""
                
                if left_key and right_key and len(left_key) > 1 and len(right_key) > 1:
                    left_value = str(df.iloc[i, 1]).strip() if pd.notna(df.iloc[i, 1]) else ""
                    right_value = str(df.iloc[i, 3]).strip() if pd.notna(df.iloc[i, 3]) else ""
                    
                    key_value_pairs.append((left_key, left_value))
                    key_value_pairs.append((right_key, right_value))
                    zigzag_pattern_count += 1
        
        # Determine the format based on the patterns found
        has_conventional = header_row_index is not None
        has_key_value = key_value_pattern_count >= 3 or zigzag_pattern_count >= 2
        
        if has_conventional and has_key_value:
            return self.FORMAT_MIXED, header_row_index, key_value_pairs
        elif has_conventional:
            return self.FORMAT_CONVENTIONAL, header_row_index, []
        elif has_key_value:
            return self.FORMAT_KEY_VALUE, None, key_value_pairs
        else:
            # Default to conventional if nothing specific is detected
            return self.FORMAT_CONVENTIONAL, header_row_index, []
    
    def run(self, file_path: str) -> List[str]:
        """
        Extract potential headers from an Excel file.
        
        Args:
            file_path: The path to the Excel file
            
        Returns:
            A list of potential headers
        """
        try:
            all_texts = []
            
            # Read all sheets in the Excel file
            sheet_dfs = read_excel_file(file_path)
            
            for sheet_name, df in sheet_dfs.items():
                logger.info(f"Processing sheet: {sheet_name}")
                
                # Detect the format of this sheet
                format_type, header_row_index, key_value_pairs = self.detect_excel_format(df)
                logger.info(f"Detected format for sheet '{sheet_name}': {format_type}")
                
                # Extract headers based on the detected format
                if format_type == self.FORMAT_CONVENTIONAL or format_type == self.FORMAT_MIXED:
                    # Extract headers from the conventional table part
                    if header_row_index is not None:
                        inferred_headers = extract_from_inferred_header(df, header_row_index)
                        logger.info(f"Extracted {len(inferred_headers)} headers from conventional table format")
                        all_texts.extend(inferred_headers)
                    
                    # Consider the first few rows as potential header rows (sometimes there are multiple header rows)
                    for i in range(min(5, len(df))):
                        if i != header_row_index:  # Skip the already processed header row
                            row_texts = [str(val).strip() for val in df.iloc[i, :] if pd.notna(val)]
                            all_texts.extend(row_texts)
                
                if format_type == self.FORMAT_KEY_VALUE or format_type == self.FORMAT_MIXED:
                    # Extract keys from key-value pairs
                    key_headers = [key for key, _ in key_value_pairs]
                    logger.info(f"Extracted {len(key_headers)} keys from key-value pairs format")
                    all_texts.extend(key_headers)
                    
                    # For zigzag pattern, we already extracted both left and right keys in detect_excel_format
                
                # For all format types, check for additional headers:
                
                # Check the first column for labels (might have more than what we already captured)
                first_col_texts = [str(df.iloc[i, 0]).strip() for i in range(len(df)) 
                                if pd.notna(df.iloc[i, 0])]
                all_texts.extend(first_col_texts)
                
                # Look for cells that use a colon or equals sign as label indicators
                for i in range(min(self.cell_scan_rows, len(df))):
                    for j in range(min(self.cell_scan_cols, df.shape[1])):
                        cell_val = df.iloc[i, j]
                        if pd.notna(cell_val) and isinstance(cell_val, str):
                            text = cell_val.strip()
                            if ':' in text or '=' in text:
                                # Extract text before the colon or equals sign
                                delimiter = ':' if ':' in text else '='
                                label_part = text.split(delimiter)[0].strip()
                                if label_part:
                                    all_texts.append(label_part)
            
            # Clean and filter extracted texts
            cleaned_texts = []
            for text in all_texts:
                text = str(text).strip()
                if (text and 2 <= len(text) <= 50 and not text.isdigit() and text.lower() != "nan"):
                    cleaned_texts.append(text)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_texts = [x for x in cleaned_texts if x not in seen and not seen.add(x)]
            
            return unique_texts
        
        except Exception as e:
            logger.error(f"Error extracting headers: {e}")
            return []
