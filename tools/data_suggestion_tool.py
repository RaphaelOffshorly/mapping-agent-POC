import json
import logging
import re
from typing import Dict, List, Any, Optional, Tuple, Union, Literal

from tools.base_tool import BaseTool
from utils.llm import get_llm
from utils.common import parse_json_response
from utils.excel import get_excel_preview

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSuggestionTool(BaseTool[Tuple[str, str, str, Optional[Dict[str, Any]]], List[Tuple[str, str, str, str]]]):
    """Tool for suggesting column ranges for target columns using LLM."""
    
    def __init__(self):
        """Initialize the data suggestion tool."""
        super().__init__(
            name="data_suggester",
            description="Suggests column ranges for target columns using LLM"
        )
        self.llm = get_llm()
    
    def run(self, input_data: Tuple[str, str, str, Optional[Dict[str, Any]]]) -> List[Tuple[str, str, str, str]]:
        """
        Suggest column ranges for a target column using LLM.
        
        Args:
            input_data: A tuple of (file_path, target_column, matched_header, column_description)
            
        Returns:
            A list of tuples with column ranges (sheet_name, column_letter, start_row, end_row)
        """
        file_path, target_column, matched_header, column_description = input_data
        
        if not file_path or not target_column:
            return []
        
        try:
            # Get a preview of the Excel file to analyze
            excel_preview = get_excel_preview(file_path)
            
            # Prepare data for the prompt
            excel_data = {}
            
            # Read all sheets and all data
            for sheet_name, sheet_data in excel_preview.items():
                # Get the data from this sheet
                rows = sheet_data.get('data', [])
                
                # Limit to first 100 rows to avoid context length issues
                sample_rows = rows[:100]
                
                excel_data[sheet_name] = {
                    'data': sample_rows,
                    'total_rows': sheet_data.get('total_rows', 0),
                    'total_cols': sheet_data.get('total_cols', 0)
                }
            
            # First detect the Excel format using LLM
            format_type = self._detect_excel_format(excel_data, matched_header)
            logger.info(f"Detected Excel format type for '{matched_header}': {format_type}")
            
            # Use the appropriate prompt based on the detected format
            if format_type == "key_value":
                prompt = self._create_key_value_prompt(target_column, matched_header, column_description, excel_data)
            else:  # conventional format
                prompt = self._create_conventional_prompt(target_column, matched_header, column_description, excel_data)
            
            # Call the LLM
            response = self.llm.invoke(prompt)
            
            # Extract the JSON object from the response
            column_ranges = parse_json_response(response.content)
            
            if not column_ranges or not isinstance(column_ranges, dict) or "ranges" not in column_ranges:
                logger.warning(f"Invalid LLM response for column ranges: {response.content}")
                return []
            
            # Parse the column ranges from the LLM response
            range_tuples = []
            for range_obj in column_ranges["ranges"]:
                if isinstance(range_obj, dict) and "sheet" in range_obj and "column" in range_obj:
                    sheet = range_obj.get("sheet", "")
                    column = range_obj.get("column", "")
                    start_row = str(range_obj.get("start_row", 1))
                    end_row = str(range_obj.get("end_row", 100))
                    
                    # Validate the column format (should be a letter)
                    if not re.match(r'^[A-Z]+$', column):
                        continue
                        
                    # Add the range tuple
                    range_tuples.append((sheet, column, start_row, end_row))
            
            return range_tuples
        
        except Exception as e:
            logger.error(f"Error generating column ranges: {e}")
            return []
    
    def _detect_excel_format(
        self, 
        excel_data: Dict[str, Dict[str, Any]], 
        matched_header: str
    ) -> Literal["conventional", "key_value"]:
        """
        Detect whether the Excel file is structured as a conventional table or a key-value pair format.
        
        Args:
            excel_data: The Excel data to analyze
            matched_header: The matched header name to look for
            
        Returns:
            "conventional" or "key_value" indicating the detected format
        """
        try:
            # First check if this is clearly a conventional table format
            for sheet_name, sheet_data in excel_data.items():
                rows = sheet_data.get('data', [])
                if not rows or len(rows) < 2:  # Need at least header + one data row
                    continue
                    
                # First look for the matched_header in the first row (common for conventional tables)
                if rows and len(rows) > 0:
                    first_row = rows[0]
                    # Check if the first row contains the matched header
                    for cell in first_row:
                        cell_str = str(cell).strip()
                        if cell_str and cell_str.lower() == matched_header.lower():
                            logger.info(f"Heuristic detected conventional format: found exact '{matched_header}' in first row")
                            return "conventional"
                            
                    # Even if exact match not found, check if first row has multiple column headers
                    # Count non-empty cells in first row
                    header_cells = [str(cell).strip() for cell in first_row if str(cell).strip()]
                    # Count consecutive data rows with cells containing data
                    data_row_count = 0
                    for i in range(1, min(6, len(rows))):  # Check first few data rows
                        row_cells = [str(cell).strip() for cell in rows[i] if str(cell).strip()]
                        if row_cells:
                            data_row_count += 1
                    
                    # If there are multiple cells in first row that look like headers
                    # and we have data rows below, it's likely a conventional table
                    if len(header_cells) >= 2 and data_row_count >= 1:
                        logger.info(f"Heuristic detected conventional format: found {len(header_cells)} potential headers in first row")
                        return "conventional"
                
            # If not clearly conventional, check for key-value format indicators
            for sheet_name, sheet_data in excel_data.items():
                rows = sheet_data.get('data', [])
                
                # Look for the matched_header in the first column (common for key-value formats)
                key_value_indicator_found = False
                for row in rows:
                    if row and len(row) > 0:
                        first_cell = str(row[0]).strip()
                        if first_cell and first_cell.lower() == matched_header.lower():
                            logger.info(f"Heuristic detected key-value format: found exact '{matched_header}' in first column")
                            key_value_indicator_found = True
                            break
                
                # Check for indentation pattern (common in key-value format)
                # If many rows start with indented values in first column, likely key-value
                indented_rows = 0
                for row in rows[:20]:  # Check first 20 rows
                    if row and len(row) > 0:
                        first_cell = str(row[0]).strip()
                        if first_cell and first_cell.startswith(" "):
                            indented_rows += 1
                
                if indented_rows >= 3:  # If several indented rows, likely key-value
                    logger.info(f"Heuristic detected key-value format: found {indented_rows} indented rows")
                    key_value_indicator_found = True
                    
                if key_value_indicator_found:
                    return "key_value"
            
            # If heuristics don't provide a conclusive result, use the LLM for more sophisticated detection
            detection_prompt = f"""
I need to analyze this Excel data to determine if it's in a conventional format (with headers as the first row) 
or a key-value format (where labels appear in one column with values in another column).

Looking specifically for the header/label "{matched_header}", analyze the Excel structure.

IMPORTANT: Pay careful attention to whether the first row appears to contain multiple column headers, which would strongly indicate a conventional table format.

The Excel data:
"""
            # Add a small subset of the Excel data to avoid large prompts
            for sheet_name, sheet_data in excel_data.items():
                # Only add first 15 rows max per sheet for format detection
                max_detection_rows = 15
                prompt_data = sheet_data['data'][:max_detection_rows]
                
                detection_prompt += f"\nSheet: {sheet_name}\n"
                # Explicitly highlight the first row to draw attention to headers
                if prompt_data and len(prompt_data) > 0:
                    detection_prompt += f"Row 1 (Potential Header Row): {', '.join(prompt_data[0])}\n"
                    
                    # Then add remaining rows
                    for i, row in enumerate(prompt_data[1:], start=2):
                        row_str = ", ".join(row)
                        if len(row_str) > 500:  # Smaller truncation for detection
                            row_str = row_str[:500] + "..."
                        detection_prompt += f"Row {i}: {row_str}\n"
            
            # Ask for determination with clearer instructions
            detection_prompt += f"""
Based on this data sample, determine if the format is:

1. "conventional" - A traditional table with headers in the first row(s) and data below
   - Headers appear in the first row, with data rows below
   - Each column represents a different data field
   - Similar data types are arranged in columns
   - The first row often contains multiple different headers
   - Headers like "{matched_header}" will be at the top of a column

2. "key_value" - A form-like layout with labels in one column (usually left) and values in another
   - Labels like "{matched_header}" appear as row labels in a column (often the first column)
   - The data is organized vertically rather than horizontally
   - The sheet often looks like a form with label-value pairs
   - Values are typically in columns to the right of the labels
   - Indentation is often used to organize related information

IMPORTANT: If the first row contains multiple headers and subsequent rows contain data values, this is almost certainly a conventional table.

Specifically, analyze how "{matched_header}" appears in the data:
- If it appears as a column header in the first row - it's likely conventional
- If it appears as a row label or key in a column - it's likely key_value

Return ONLY the word "conventional" or "key_value".
"""
            
            # Call the LLM for format detection
            response = self.llm.invoke(detection_prompt)
            
            # Get the response and determine the format type
            format_text = response.content.strip().lower()
            
            if "key" in format_text and "value" in format_text:
                logger.info(f"LLM detected key-value format with response: '{format_text}'")
                return "key_value"
            elif "conventional" in format_text:
                logger.info(f"LLM detected conventional format with response: '{format_text}'")
                return "conventional"
            else:
                # Default to conventional if unclear - safer assumption for most Excel files
                logger.warning(f"Unclear format detection result: '{format_text}', defaulting to conventional")
                return "conventional"
                
        except Exception as e:
            logger.error(f"Error detecting Excel format: {e}")
            # Default to conventional format if detection fails
            return "conventional"
    
    def _create_conventional_prompt(
        self, 
        target_column: str, 
        matched_header: str, 
        column_description: Optional[Dict[str, Any]], 
        excel_data: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        Create a prompt for suggesting column ranges for a target column in conventional Excel format.
        
        Args:
            target_column: The target column name
            matched_header: The matched header name
            column_description: The description of the target column
            excel_data: The Excel data to analyze
            
        Returns:
            A prompt string
        """
        prompt = f"""
I need to find the appropriate column range in a conventional Excel file for a column named "{target_column}".

The matched header in the Excel file is: "{matched_header}"

"""
        
        # Add column description if available
        if column_description:
            prompt += f"""
Column description:
- Description: {column_description.get('description', 'Not available')}
- Data type: {column_description.get('data_type', 'Not available')}
- Expected sample values: {json.dumps(column_description.get('sample_values', []), indent=2)}

"""
        
        prompt += """
Here's the Excel file data:
"""
        
        # Add Excel data - we need to be careful about the prompt size
        # Include full data for smaller sheets, but limit larger sheets
        for sheet_name, sheet_data in excel_data.items():
            rows = sheet_data['total_rows']
            cols = sheet_data['total_cols']
            
            # For very large sheets, include a subset
            if rows > 100 or cols > 50:
                max_rows = min(100, rows)
                max_cols = min(50, cols)
                
                prompt += f"\nSheet: {sheet_name} (showing {max_rows} of {rows} rows and {max_cols} of {cols} columns)\n"
                
                # Include header rows and some data rows
                for i in range(min(max_rows, len(sheet_data['data']))):
                    row_str = ", ".join(sheet_data['data'][i][:max_cols])
                    if len(row_str) > 1000:  # Truncate very long rows
                        row_str = row_str[:1000] + "..."
                    prompt += f"Row {i+1}: {row_str}\n"
                    
                if rows > max_rows:
                    prompt += f"... ({rows - max_rows} more rows)\n"
            else:
                # For smaller sheets, include all data
                prompt += f"\nSheet: {sheet_name} ({rows} rows, {cols} columns)\n"
                
                for i in range(len(sheet_data['data'])):
                    row_str = ", ".join(sheet_data['data'][i])
                    if len(row_str) > 1000:  # Truncate very long rows
                        row_str = row_str[:1000] + "..."
                    prompt += f"Row {i+1}: {row_str}\n"
        
        prompt += f"""
This Excel file appears to be in a conventional format with headers at the top. Based on the matched header "{matched_header}" and the column description, please:

1. Identify the column letter (A, B, C, etc.) in the Excel file that has the header "{matched_header}" 
   and contains data matching the target column "{target_column}"
2. Determine the row range (start row and end row) that contains relevant data for this column
   - Start row should be the first row after the header row
   - End row should be the last row containing relevant data
3. If multiple sheets contain relevant data, identify all applicable sheet/column/row range combinations

Your task is to provide column range information that can be used to extract data from the Excel file.

Return ONLY a JSON object in the following format:
{{
  "ranges": [
    {{
      "sheet": "Sheet1",
      "column": "B",
      "start_row": 2,
      "end_row": 10
    }},
    // Additional ranges if needed
  ]
}}

The "column" should be a letter (A, B, C, etc.), and the "start_row" and "end_row" should be numbers.
Do not include any explanations or additional text in your response, just the JSON object.
"""
        
        # Check if the prompt is too large and truncate if necessary
        if len(prompt) > 100000:
            logger.warning(f"Prompt for {target_column} is too large ({len(prompt)} chars), truncating")
            prompt = prompt[:100000] + "\n\n[Excel data truncated due to size]\n\nReturn ONLY the JSON object with column ranges."
        
        return prompt
    
    def _create_key_value_prompt(
        self, 
        target_column: str, 
        matched_header: str, 
        column_description: Optional[Dict[str, Any]], 
        excel_data: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        Create a prompt for suggesting column ranges for a target column in key-value Excel format.
        
        Args:
            target_column: The target column name
            matched_header: The matched header name
            column_description: The description of the target column
            excel_data: The Excel data to analyze
            
        Returns:
            A prompt string
        """
        prompt = f"""
I need to find the appropriate column range in a non-conventional Excel file for a column named "{target_column}".

The matched header or label in the Excel file is: "{matched_header}"

"""
        
        # Add column description if available
        if column_description:
            prompt += f"""
Column description:
- Description: {column_description.get('description', 'Not available')}
- Data type: {column_description.get('data_type', 'Not available')}
- Expected sample values: {json.dumps(column_description.get('sample_values', []), indent=2)}

"""
        print (column_description)
        prompt += """
Here's the Excel file data:
"""
        
        # Add Excel data - we need to be careful about the prompt size
        # Include full data for smaller sheets, but limit larger sheets
        for sheet_name, sheet_data in excel_data.items():
            rows = sheet_data['total_rows']
            cols = sheet_data['total_cols']
            
            # For very large sheets, include a subset
            if rows > 100 or cols > 50:
                max_rows = min(100, rows)
                max_cols = min(50, cols)
                
                prompt += f"\nSheet: {sheet_name} (showing {max_rows} of {rows} rows and {max_cols} of {cols} columns)\n"
                
                # Include header rows and some data rows
                for i in range(min(max_rows, len(sheet_data['data']))):
                    row_str = ", ".join(sheet_data['data'][i][:max_cols])
                    if len(row_str) > 1000:  # Truncate very long rows
                        row_str = row_str[:1000] + "..."
                    prompt += f"Row {i+1}: {row_str}\n"
                    
                if rows > max_rows:
                    prompt += f"... ({rows - max_rows} more rows)\n"
            else:
                # For smaller sheets, include all data
                prompt += f"\nSheet: {sheet_name} ({rows} rows, {cols} columns)\n"
                
                for i in range(len(sheet_data['data'])):
                    row_str = ", ".join(sheet_data['data'][i])
                    if len(row_str) > 1000:  # Truncate very long rows
                        row_str = row_str[:1000] + "..."
                    prompt += f"Row {i+1}: {row_str}\n"
        
        prompt += f"""
This Excel file is in a form-like key-value format, where labels like "{matched_header}" appear as row labels in column A (or occasionally other columns) with corresponding values in columns to their right.

Based on the Excel structure I can see, the file has:
1. Labels in column A, with corresponding values in column B
2. Sometimes additional label-value pairs in columns D-E or other columns
3. A form-like structure rather than a traditional table with headers at the top
4. The "{matched_header}" label will likely appear in column A or another label column

Please identify exactly where the value for "{matched_header}" is located:

1. First, find the exact row where the label "{matched_header}" appears (likely in column A, but could be in column D or another label column)
2. Then, determine which column contains the corresponding value:
   - If the label is in column A, the value is likely in column B
   - If the label is in column D, the value is likely in column E
   - The value will be in the same row as its label
3. The start_row and end_row should be the same (the exact row where the value is found)
4. Be precise - we need the exact cell coordinates to extract the correct data

Your task is to provide the exact cell coordinates for the value corresponding to the "{matched_header}" label.

Return ONLY a JSON object in the following format:
{{
  "ranges": [
    {{
      "sheet": "Sheet1",  // The sheet containing the data
      "column": "B",      // The column letter containing the VALUE (not the label)
      "start_row": 12,    // The row number containing the value
      "end_row": 12       // Same as start_row for single-cell values
    }}
  ]
}}

The "column" should be a letter (A, B, C, etc.), and the "start_row" and "end_row" should be numbers.
Do not include any explanations or additional text in your response, just the JSON object.
"""
        
        # Check if the prompt is too large and truncate if necessary
        if len(prompt) > 100000:
            logger.warning(f"Prompt for {target_column} is too large ({len(prompt)} chars), truncating")
            prompt = prompt[:100000] + "\n\n[Excel data truncated due to size]\n\nReturn ONLY the JSON object with column ranges."
        
        return prompt
        
    def _create_column_range_prompt(
        self, 
        target_column: str, 
        matched_header: str, 
        column_description: Optional[Dict[str, Any]], 
        excel_data: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        Legacy method - superseded by format-specific prompts.
        Now calls _detect_excel_format and then the appropriate prompt method.
        
        Args:
            target_column: The target column name
            matched_header: The matched header name
            column_description: The description of the target column
            excel_data: The Excel data to analyze
            
        Returns:
            A prompt string
        """
        # Detect format first
        format_type = self._detect_excel_format(excel_data, matched_header)
        
        # Use appropriate prompt based on detected format
        if format_type == "key_value":
            return self._create_key_value_prompt(target_column, matched_header, column_description, excel_data)
        else:
            return self._create_conventional_prompt(target_column, matched_header, column_description, excel_data)
