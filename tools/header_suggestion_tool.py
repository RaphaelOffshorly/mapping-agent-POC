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

class HeaderSuggestionTool(BaseTool[Tuple[str, str, Optional[Dict[str, Any]]], str]):
    """Tool for suggesting headers for target columns using LLM."""
    
    def __init__(self):
        """Initialize the header suggestion tool."""
        super().__init__(
            name="header_suggester",
            description="Suggests headers for target columns using LLM"
        )
        self.llm = get_llm()
    
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
                
                # Add this sheet's data to the prompt
                prompt_data.append({
                    'sheet_name': sheet_name,
                    'rows': sample_rows
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
        
        # Add the Excel data to the prompt
        for sheet_data in excel_data:
            prompt += f"\nSheet: {sheet_data['sheet_name']}\n"
            
            # Add all rows
            for i, row in enumerate(sheet_data['rows']):
                row_str = ", ".join(row)
                if len(row_str) > 1000:  # Truncate very long rows
                    row_str = row_str[:1000] + "..."
                prompt += f"Row {i+1}: {row_str}\n"
        
        prompt += f"""
Based on the Excel data shown above and the description of the target column "{target_column}", please:

1. Analyze the data patterns and content
2. Identify the most appropriate header that would match this target column
3. Consider both the semantic meaning and the data structure

Return ONLY a single string with your suggested header name. Do not include any explanations or additional text.
"""
        
        return prompt
