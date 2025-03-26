import json
import logging
from typing import Dict, List, Any, Optional

from tools.base_tool import BaseTool
from utils.llm import get_llm
from utils.common import parse_json_response

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ColumnDescriptionTool(BaseTool[List[str], Dict[str, Dict[str, Any]]]):
    """Tool for describing target columns using LLM."""
    
    def __init__(self):
        """Initialize the column description tool."""
        super().__init__(
            name="column_describer",
            description="Describes target columns using LLM"
        )
        self.llm = get_llm()
    
    def run(self, target_columns: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Describe target columns using LLM.
        
        Args:
            target_columns: A list of target column names
            
        Returns:
            A dictionary with descriptions for each target column
        """
        if not target_columns:
            return {}
        
        prompt = self._create_description_prompt(target_columns)
        
        try:
            response = self.llm.invoke(prompt)
            descriptions = parse_json_response(response.content)
            
            if not descriptions:
                logger.error("Could not parse column descriptions from LLM response")
                return {}
            
            return descriptions
        
        except Exception as e:
            logger.error(f"Error describing columns: {e}")
            return {}
    
    def _create_description_prompt(self, target_columns: List[str]) -> str:
        """
        Create a prompt for describing target columns.
        
        Args:
            target_columns: A list of target column names
            
        Returns:
            A prompt string
        """
        return f"""
I need descriptions for these target column names that might appear in an Excel file:

{json.dumps(target_columns, indent=2)}

For each target column, provide:
1. A brief description of what kind of data this column typically contains
2. The expected data type (text, number, date, etc.)
3. 3-5 realistic sample values that might appear in this column

Return ONLY a JSON object with this structure:
{{
    "target_column_name": {{
        "description": "Brief description of what this column contains",
        "data_type": "text|number|date|boolean|etc",
        "sample_values": ["example1", "example2", "example3"]
    }},
    ...
}}
        """
