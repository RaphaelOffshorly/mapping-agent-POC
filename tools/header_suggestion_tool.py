import json
import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union, Literal

from tools.base_tool import BaseTool
from utils.llm import get_llm
from utils.common import parse_json_response, infer_header_row
from utils.excel import get_excel_preview, read_excel_file

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HeaderSuggestionTool(BaseTool[Tuple[str, str, Optional[Dict[str, Any]]], str]):
    """Tool for suggesting headers for target columns using LLM."""
    
    # Define format types
    FORMAT_CONVENTIONAL = "conventional_table"
    FORMAT_KEY_VALUE = "key_value_pairs"
    FORMAT_MIXED = "mixed_format"
    
    def __init__(self):
        """Initialize the header suggestion tool."""
        super().__init__(
            name="header_suggester",
            description="Suggests headers for target columns using LLM"
        )
        self.llm = get_llm()
    
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
        header_row_index = infer_header_row(df, 10)
        
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
    
    def extract_potential_headers(self, file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Extract potential headers based on the detected format of each sheet in the Excel file.
        
        Args:
            file_path: The path to the Excel file
            
        Returns:
            A dictionary mapping sheet names to their format information, including detected headers
        """
        try:
            # Read all sheets in the Excel file
            dfs = read_excel_file(file_path)
            
            # Store format information and headers for each sheet
            sheet_formats = {}
            
            for sheet_name, df in dfs.items():
                # Detect the format of this sheet
                format_type, header_row_index, key_value_pairs = self.detect_excel_format(df)
                
                # Extract headers based on format
                conventional_headers = []
                if header_row_index is not None:
                    conventional_headers = [
                        str(df.iloc[header_row_index, col]).strip() 
                        for col in range(df.shape[1]) 
                        if pd.notna(df.iloc[header_row_index, col])
                    ]
                
                key_value_headers = [key for key, _ in key_value_pairs] if key_value_pairs else []
                
                # Store the format information for this sheet
                sheet_formats[sheet_name] = {
                    "format_type": format_type,
                    "header_row_index": header_row_index,
                    "conventional_headers": conventional_headers,
                    "key_value_headers": key_value_headers,
                }
            
            return sheet_formats
        except Exception as e:
            logger.error(f"Error extracting potential headers: {e}")
            return {}
    
    def run(self, input_data: Tuple[str, str, Optional[Dict[str, Any]]]) -> str:
        """
        Suggest a header for a target column using LLM.
        
        Args:
            input_data: A tuple of (file_path, target_column, column_description)
            
        Returns:
            A suggested header string
        """
        file_path, target_column, column_description = input_data
        
        if not file_path or not target_column:
            return f"Error suggesting header for {target_column}"
        
        # Get all potential headers from the Excel file
        try:
            # Get a preview of the Excel file to analyze
            excel_preview = get_excel_preview(file_path)
            
            # Extract format information and potential headers
            sheet_formats = self.extract_potential_headers(file_path)
            
            # Prepare data for the prompt
            prompt_data = []
            for sheet_name, sheet_data in excel_preview.items():
                # Only include the first sheet or limit to 2 sheets to avoid context length issues
                if len(prompt_data) >= 2:
                    break
                    
                # Get the data from this sheet
                rows = sheet_data.get('data', [])
                
                # Limit to first 30 rows to avoid context length issues
                sample_rows = rows[:30]
                
                # Get the format information for this sheet
                format_info = sheet_formats.get(sheet_name, {})
                
                # Add this sheet's data to the prompt
                prompt_data.append({
                    'sheet_name': sheet_name,
                    'rows': sample_rows,
                    'format_type': format_info.get('format_type', self.FORMAT_CONVENTIONAL),
                    'conventional_headers': format_info.get('conventional_headers', []),
                    'key_value_headers': format_info.get('key_value_headers', []),
                    'header_row_index': format_info.get('header_row_index')
                })
            
            # Create the prompt for the LLM
            prompt = self._create_suggestion_prompt(target_column, column_description, prompt_data)
            
            # Call the LLM
            response = self.llm.invoke(prompt)
            
            # Extract just the suggested header name from the response
            suggested_header = response.content.strip()
            
            # Remove any quotes or formatting that might be present
            suggested_header = suggested_header.strip('"\'')
            
            return suggested_header
        
        except Exception as e:
            logger.error(f"Error suggesting header: {e}")
            return f"Error suggesting header for {target_column}"
    
    def _create_suggestion_prompt(
        self, 
        target_column: str, 
        column_description: Optional[Dict[str, Any]], 
        excel_data: List[Dict[str, Any]]
    ) -> str:
        """
        Create a prompt for suggesting a header for a target column.
        
        Args:
            target_column: The target column name
            column_description: The description of the target column
            excel_data: The Excel data to analyze
            
        Returns:
            A prompt string
        """
        prompt = f"""
I need you to analyze this Excel data and suggest an appropriate header for a target column named "{target_column}".

"""
        # Add column description if available
        if column_description:
            prompt += f"""
Target column description:
- Description: {column_description.get('description', 'Not available')}
- Data type: {column_description.get('data_type', 'Not available')}
- Expected sample values: {json.dumps(column_description.get('sample_values', []), indent=2)}

"""
        
        prompt += """
Here's the Excel data:
"""
        
        # Add the Excel data to the prompt with format context
        for sheet_data in excel_data:
            sheet_name = sheet_data['sheet_name']
            format_type = sheet_data.get('format_type', self.FORMAT_CONVENTIONAL)
            conventional_headers = sheet_data.get('conventional_headers', [])
            key_value_headers = sheet_data.get('key_value_headers', [])
            header_row_index = sheet_data.get('header_row_index')
            
            prompt += f"\nSheet: {sheet_name}\n"
            
            # Add format information
            if format_type == self.FORMAT_CONVENTIONAL:
                prompt += "Format: Conventional table (headers in first row)\n"
                if conventional_headers:
                    prompt += f"Detected Headers: {', '.join(conventional_headers)}\n"
                if header_row_index is not None:
                    prompt += f"Header Row: {header_row_index + 1}\n"
            elif format_type == self.FORMAT_KEY_VALUE:
                prompt += "Format: Key-Value pairs\n"
                if key_value_headers:
                    prompt += f"Detected Keys: {', '.join(key_value_headers[:10])}"
                    if len(key_value_headers) > 10:
                        prompt += f" and {len(key_value_headers) - 10} more..."
                    prompt += "\n"
            elif format_type == self.FORMAT_MIXED:
                prompt += "Format: Mixed (contains both conventional table and key-value pairs)\n"
                if conventional_headers:
                    prompt += f"Detected Table Headers: {', '.join(conventional_headers)}\n"
                if key_value_headers:
                    prompt += f"Detected Keys: {', '.join(key_value_headers[:10])}"
                    if len(key_value_headers) > 10:
                        prompt += f" and {len(key_value_headers) - 10} more..."
                    prompt += "\n"
                if header_row_index is not None:
                    prompt += f"Header Row: {header_row_index + 1}\n"
            
            # Add row data
            prompt += "\nSample Data:\n"
            for i, row in enumerate(sheet_data['rows']):
                row_str = ", ".join(row)
                if len(row_str) > 1000:  # Truncate very long rows
                    row_str = row_str[:1000] + "..."
                prompt += f"Row {i+1}: {row_str}\n"
        
        prompt += f"""
Based on the Excel data shown above and the description of the target column "{target_column}", please:

1. Analyze the data patterns and content
2. Identify the most appropriate header that would match this target column
3. Consider the Excel format type:
   - For conventional tables, look at the detected headers and their column values
   - For key-value pairs, consider both keys and values as potential header matches
   - For mixed formats, check both conventional headers and key-value pairs

Important considerations:
- If the file contains key-value pairs, the keys (labels in the first column) could be excellent header candidates
- If the file has a conventional table structure, check the headers in the detected header row
- In mixed format files, determine if the target column aligns better with the table section or the key-value section
- Match semantically with the target column description and sample values if provided

Return ONLY a single string with your suggested header name. Do not include any explanations or additional text.
"""
        
        return prompt
