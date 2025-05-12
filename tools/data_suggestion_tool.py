import json
import logging
import re
from typing import Dict, List, Any, Optional, Tuple, Union

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
            
            # Use the LLM to find appropriate column ranges based on the matched header
            prompt = self._create_column_range_prompt(target_column, matched_header, column_description, excel_data)
            
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
    
    def _create_column_range_prompt(
        self, 
        target_column: str, 
        matched_header: str, 
        column_description: Optional[Dict[str, Any]], 
        excel_data: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        Create a prompt for suggesting column ranges for a target column.
        
        Args:
            target_column: The target column name
            matched_header: The matched header name
            column_description: The description of the target column
            excel_data: The Excel data to analyze
            
        Returns:
            A prompt string
        """
        prompt = f"""
I need to find the appropriate column range in an Excel file for a column named "{target_column}".

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
Based on the matched header "{matched_header}" and the column description, please:

1. Identify the column letter (A, B, C, etc.) in the Excel file that contains data matching the target column "{target_column}"
2. Determine the row range (start row and end row) that contains relevant data for this column
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
