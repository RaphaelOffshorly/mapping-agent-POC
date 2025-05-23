import logging
import json
from typing import Dict, List, Any, Optional, Tuple

from agents.base_agent import BaseAgent
from tools.cell_coordinate_tool import CellCoordinateTool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CellCoordinateAgent(BaseAgent):
    """Agent for finding exact cell coordinates of selected data."""
    
    def __init__(self, verbose: bool = True):
        """Initialize the cell coordinate agent."""
        super().__init__(
            name="cell_coordinate_agent",
            description="Finds exact cell coordinates of selected data",
            verbose=verbose
        )
        self.add_tool(CellCoordinateTool())
    
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find exact cell coordinates of selected data.
        
        Args:
            state: The current state, must contain 'file_path', 'matches', 'sample_data',
                  'export_selections', and can optionally contain 'selected_cells' (from the Excel UI)
                  and 'data_coordinates' (from the suggestion agent)
            
        Returns:
            The updated state with 'cell_coordinates' added
        """
        self.think("Starting to find exact cell coordinates of selected data")
        
        file_path = state.get('file_path')
        matches = state.get('matches', {})
        sample_data = state.get('sample_data', {})
        suggested_data = state.get('suggested_data', {})
        export_selections = state.get('export_selections', {})
        selected_cells = state.get('selected_cells', {})
        column_ranges = state.get('column_ranges', {})
        
        if not file_path:
            self.think("Missing file path, returning empty cell coordinates")
            logger.error("Missing file path for CellCoordinateAgent")
            state['cell_coordinates'] = {}
            return state
        
        self.think(f"Processing file: {file_path}")
        self.think(f"Export selections: {export_selections}")
        
        # Log information about selected cells and column ranges
        if selected_cells:
            self.think(f"Selected cells from UI: {selected_cells}")
        else:
            self.think("No manually selected cells from UI")
            
        if column_ranges:
            self.think(f"Column ranges from suggestion: {len(column_ranges)} target columns")
            # Show a few examples
            sample_ranges = list(column_ranges.items())[:2]
            for target, ranges in sample_ranges:
                if ranges:
                    self.think(f"Sample column ranges for '{target}': {ranges[:2]}")
                    if len(ranges) > 2:
                        self.think(f"And {len(ranges) - 2} more column ranges...")
        else:
            self.think("No column ranges from suggestion agent")
        
        # Use the cell coordinate tool
        cell_coordinate_tool = self.tools[0]
        self.think("Using CellCoordinateTool to find exact cell coordinates")
        
        result = cell_coordinate_tool.run((
            file_path, 
            matches, 
            sample_data, 
            suggested_data, 
            export_selections,
            selected_cells,
            column_ranges
        ))
        
        if "error" in result:
            self.think(f"Error finding cell coordinates: {result['error']}")
            state['cell_coordinates'] = {}
            state['mapping_json'] = "{}"
            return state
        
        mapping = result.get("mapping", {})
        
        self.think(f"Found cell coordinates for {len(mapping)} target columns")
        if mapping:
            # Display a few sample coordinates
            sample_entries = list(mapping.items())[:3]
            for target, coords in sample_entries:
                if coords:
                    self.think(f"Cell coordinates for '{target}': {coords}")
                else:
                    self.think(f"No cell coordinates found for '{target}'")
            if len(mapping) > 3:
                self.think(f"And cell coordinates for {len(mapping) - 3} more target columns...")
        
        logger.info(f"Found cell coordinates for {len(mapping)} target columns")
        
        # Convert mapping to JSON string
        mapping_json = json.dumps(mapping, indent=2)
        
        # Update the state
        state['cell_coordinates'] = mapping
        state['mapping_json'] = mapping_json
        
        self.think("Finished finding cell coordinates")
        return state
