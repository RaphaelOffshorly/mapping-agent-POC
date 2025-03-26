import json
import logging
from typing import Dict, List, Any, Optional, Tuple, Union

from tools.base_tool import BaseTool
from utils.llm import get_llm
from utils.common import parse_json_response
from utils.excel import get_excel_preview

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSuggestionTool(BaseTool[Tuple[str, str, str, Optional[Dict[str, Any]]], List[str]]):
    """Tool for suggesting sample data for target columns using LLM."""
    
    def __init__(self):
        """Initialize the data suggestion tool."""
        super().__init__(
            name="data_suggester",
            description="Suggests sample data for target columns using LLM"
        )
        self.llm = get_llm()
    
    def run(self, input_data: Tuple[str, str, str, Optional[Dict[str, Any]]]) -> List[str]:
        """
        Suggest sample data for a target column using LLM.
        
        Args:
            input_data: A tuple of (file_path, target_column, matched_header, column_description)
            
        Returns:
            A list of suggested sample data
        """
        file_path, target_column, matched_header, column_description = input_data
        
        if not file_path or not target_column:
            return ["No sample data found"]
        
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
            
            # Use the LLM to find appropriate data based on the column description and matched header
            prompt = self._create_suggestion_prompt(target_column, matched_header, column_description, excel_data)
            
            # Call the LLM
            response = self.llm.invoke(prompt)
            
            # Extract the JSON array from the response
            sample_data = parse_json_response(response.content)
            
            if not sample_data or not isinstance(sample_data, list):
                # Fall back to the column description's sample values
                if column_description and "sample_values" in column_description:
                    return column_description["sample_values"]
                return ["No sample data found"]
            
            # Limit to 20 samples
            sample_data = sample_data[:20]
            
            return sample_data
        
        except Exception as e:
            logger.error(f"Error suggesting sample data: {e}")
            
            # Fall back to the column description's sample values
            if column_description and "sample_values" in column_description:
                return column_description["sample_values"]
            
            return ["Error suggesting sample data"]
    
    def _create_suggestion_prompt(
        self, 
        target_column: str, 
        matched_header: str, 
        column_description: Optional[Dict[str, Any]], 
        excel_data: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        Create a prompt for suggesting sample data for a target column.
        
        Args:
            target_column: The target column name
            matched_header: The matched header name
            column_description: The description of the target column
            excel_data: The Excel data to analyze
            
        Returns:
            A prompt string
        """
        prompt = f"""
I need to find appropriate sample data from an Excel file for a column named "{target_column}".

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

1. Identify the most appropriate data in the Excel file that matches the target column "{target_column}"
2. Extract 5-20 sample values that best represent this data
3. If you can't find exact matches for the header, look for semantically similar columns or data patterns that match the expected data type and description
4. If multiple potential matches exist, prioritize the one that best aligns with the column description

Return ONLY a JSON array of sample values, like this:
["sample1", "sample2", "sample3", ...]

Do not include any explanations or additional text in your response, just the JSON array.
"""
        
        # Check if the prompt is too large and truncate if necessary
        if len(prompt) > 100000:
            logger.warning(f"Prompt for {target_column} is too large ({len(prompt)} chars), truncating")
            prompt = prompt[:100000] + "\n\n[Excel data truncated due to size]\n\nReturn ONLY a JSON array of sample values."
        
        return prompt
