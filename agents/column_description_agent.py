import logging
from typing import Dict, List, Any, Optional

from agents.base_agent import BaseAgent
from tools.column_description_tool import ColumnDescriptionTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ColumnDescriptionAgent(BaseAgent):
    """Agent for describing target columns using LLM."""
    
    def __init__(self, verbose: bool = True):
        """Initialize the column description agent."""
        super().__init__(
            name="column_description_agent",
            description="Describes target columns using LLM",
            verbose=verbose
        )
        self.add_tool(ColumnDescriptionTool())
    
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Describe target columns using schema if available, or LLM otherwise.
        
        Args:
            state: The current state, must contain 'target_columns'
            
        Returns:
            The updated state with 'column_descriptions' added
        """
        self.think("Starting to describe target columns")
        
        target_columns = state.get('target_columns')
        schema = state.get('schema')  # Get schema from state if available
        
        if not target_columns:
            self.think("No target columns provided, returning empty descriptions")
            logger.error("No target columns provided to ColumnDescriptionAgent")
            state['column_descriptions'] = {}
            return state
        
        self.think(f"Analyzing {len(target_columns)} target columns")
        if target_columns:
            self.think(f"Sample target columns: {', '.join(target_columns[:5])}")
            if len(target_columns) > 5:
                self.think(f"And {len(target_columns) - 5} more...")
        
        # Initialize column descriptions dictionary
        column_descriptions = {}
        
        # If schema is available, use it for column descriptions
        if schema and 'properties' in schema:
            self.think("Schema found, using schema for column descriptions")
            schema_prop_count = len(schema['properties'])
            logger.info(f"Using schema with {schema_prop_count} properties for column descriptions")
            logger.info(f"Schema property keys: {list(schema['properties'].keys())}")
            
            # Extract descriptions from schema - case insensitive matching
            schema_columns_used = 0
            schema_props_lower = {k.lower(): k for k in schema['properties'].keys()}
            
            for column in target_columns:
                # Try exact match first
                if column in schema['properties']:
                    schema_key = column
                    self.think(f"Exact match found for '{column}' in schema")
                # Then try case-insensitive match
                elif column.lower() in schema_props_lower:
                    schema_key = schema_props_lower[column.lower()]
                    self.think(f"Case-insensitive match found for '{column}' as '{schema_key}' in schema")
                else:
                    self.think(f"Column '{column}' not found in schema properties")
                    logger.warning(f"Column '{column}' not found in schema properties: {list(schema['properties'].keys())[:5]}...")
                    continue
                
                # Use the properties from the schema
                column_properties = schema['properties'][schema_key]
                column_descriptions[column] = {
                    'description': column_properties.get('description', ''),
                    'data_type': column_properties.get('type', 'string'),
                    'sample_values': column_properties.get('samples', [])
                }
                schema_columns_used += 1
                self.think(f"Using schema description for '{column}'")
                logger.info(f"Using schema for column: {column} (schema key: {schema_key})")
            
            logger.info(f"Used schema for {schema_columns_used} out of {len(target_columns)} columns")
            
            # If all columns are covered by schema, don't call LLM at all
            if schema_columns_used == len(target_columns):
                self.think("All columns found in schema, skipping LLM description generation")
                logger.info("All columns covered by schema, skipping LLM description generation")
                
                # Update the state
                state['column_descriptions'] = column_descriptions
                self.think("Finished describing target columns using schema only")
                return state
        
        # For any columns not covered by schema, use the LLM tool
        missing_columns = [col for col in target_columns if col not in column_descriptions]
        
        if missing_columns:
            self.think(f"Using ColumnDescriptionTool for {len(missing_columns)} columns not in schema")
            logger.info(f"Generating descriptions for {len(missing_columns)} missing columns: {missing_columns}")
            
            # Use the column description tool
            column_description_tool = self.tools[0]
            missing_descriptions = column_description_tool.run(missing_columns)
            
            # Update the column descriptions dictionary
            column_descriptions.update(missing_descriptions)
        
        self.think(f"Generated descriptions for {len(column_descriptions)} target columns")
        if column_descriptions:
            sample_column = next(iter(column_descriptions))
            self.think(f"Sample description for '{sample_column}': {column_descriptions[sample_column]}")
        
        logger.info(f"Generated descriptions for {len(column_descriptions)} target columns")
        
        # Update the state
        state['column_descriptions'] = column_descriptions
        
        self.think("Finished describing target columns")
        return state
