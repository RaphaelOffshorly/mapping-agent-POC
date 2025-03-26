import json
import logging
from typing import Dict, List, Any, Optional, Tuple

from tools.base_tool import BaseTool
from utils.llm import get_llm
from utils.common import parse_json_response

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HeaderMatchingTool(BaseTool[Tuple[List[str], List[str]], Dict[str, Dict[str, str]]]):
    """Tool for matching headers to target columns using LLM."""
    
    def __init__(self):
        """Initialize the header matching tool."""
        super().__init__(
            name="header_matcher",
            description="Matches headers to target columns using LLM"
        )
        self.llm = get_llm()
        self.batch_size = 20  # Number of target columns to process in a single batch
    
    def run(self, input_data: Tuple[List[str], List[str]]) -> Dict[str, Dict[str, str]]:
        """
        Match headers to target columns using LLM.
        
        Args:
            input_data: A tuple of (potential_headers, target_columns)
            
        Returns:
            A dictionary with matches for each target column
        """
        potential_headers, target_columns = input_data
        
        if not potential_headers or not target_columns:
            return {}
        
        # Split target columns into smaller batches to avoid exceeding model context limits
        target_batches = [target_columns[i:i + self.batch_size] for i in range(0, len(target_columns), self.batch_size)]
        
        all_matches = {}
        
        for batch in target_batches:
            prompt = self._create_matching_prompt(potential_headers, batch)
            
            try:
                response = self.llm.invoke(prompt)
                batch_matches = parse_json_response(response.content)
                
                if batch_matches:
                    # Add batch matches to all_matches
                    all_matches.update(batch_matches)
                else:
                    logger.error("Could not parse matches from LLM response")
                    # Add default "No match found" for all columns in this batch
                    for target in batch:
                        all_matches[target] = {
                            "match": "No match found",
                            "confidence": "low"
                        }
            
            except Exception as e:
                logger.error(f"Error matching headers: {e}")
                # Add default "No match found" for all columns in this batch
                for target in batch:
                    all_matches[target] = {
                        "match": "No match found",
                        "confidence": "low"
                    }
        
        # Ensure all target columns have a match entry
        for target in target_columns:
            if target not in all_matches:
                all_matches[target] = {
                    "match": "No match found",
                    "confidence": "low"
                }
        
        return all_matches
    
    def _create_matching_prompt(self, potential_headers: List[str], target_columns: List[str]) -> str:
        """
        Create a prompt for matching headers to target columns.
        
        Args:
            potential_headers: A list of potential headers
            target_columns: A list of target column names
            
        Returns:
            A prompt string
        """
        return f"""
I have extracted these potential headers or labels from an Excel file:

{json.dumps(potential_headers, indent=2)}

I need to find matches for these target column names:

{json.dumps(target_columns, indent=2)}

For each target column, find the best matching header from the Excel file.
Consider exact matches, semantic similarity, abbreviations, and partial matches.

Return ONLY a JSON object with this structure:
{{
    "target_column_name": {{
        "match": "best_matching_header_from_excel",
        "confidence": "high|medium|low"
    }},
    ...
}}

If no good match exists for a target column, use "No match found" as the value for "match".
        """
