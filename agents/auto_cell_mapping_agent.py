import json
import logging
from typing import Dict, List, Any, Optional

from agents.base_agent import BaseAgent
from utils.excel import find_cell_coordinates_for_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutoCellMappingAgent(BaseAgent):
    """
    Agent that automatically finds the cell coordinates for the selected data in the Excel file.
    This agent is used when the user clicks the "Auto Map" button.
    """
    
    def __init__(self, verbose: bool = True):
        """Initialize the auto cell mapping agent."""
        super().__init__(
            name="auto_cell_mapping_agent",
            description="Automatically finds cell coordinates for selected data",
            verbose=verbose
        )
    
    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the agent to find cell coordinates for the selected data.
        
        Args:
            state: The current workflow state
            
        Returns:
            The updated workflow state with cell coordinates
        """
        self.think("Starting to automatically find cell coordinates for selected data")
        
        # Extract required data from state
        file_path = state.get("file_path")
        target_columns = state.get("target_columns", [])
        export_selections = state.get("export_selections", {})
        sample_data = state.get("sample_data", {})
        suggested_data = state.get("suggested_data", {})
        selected_cells = state.get("selected_cells", {})
        
        if not file_path:
            self.think("Missing file path, returning empty cell coordinates")
            return {**state, "error": "No file path provided"}
        
        if not target_columns:
            self.think("Missing target columns, returning empty cell coordinates")
            return {**state, "error": "No target columns provided"}
        
        self.think(f"Processing file: {file_path}")
        self.think(f"Target columns: {target_columns}")
        self.think(f"Export selections: {export_selections}")
        self.think(f"User selected cells: {list(selected_cells.keys())}")
        
        # Create a mapping dictionary to store the results
        mapping = {}
        
        # Process each target column
        for target_column in target_columns:
            self.think(f"Processing target column: {target_column}")
            
            # Initialize with empty mapping for this target column
            mapping[target_column] = []
            
            # Check if there are user-selected coordinates for this target column
            selected_cells = state.get("selected_cells", {})
            
            # Determine which data source to use based on export_selections
            data_source = export_selections.get(target_column, "sample")
            self.think(f"Using data source: {data_source}")
            
            # Check if there are user-selected coordinates for this target column
            if target_column in selected_cells and selected_cells[target_column]:
                self.think(f"Found user-selected coordinates for {target_column}")
                
                # Use the user-selected coordinates and skip auto-mapping for this column
                # This preserves user-mapped data even when auto-map is clicked
                mapping[target_column] = [f"{sheet}!{cell}" for sheet, cell in selected_cells[target_column]]
                continue
            
            if data_source == "ai":
                data_values = suggested_data.get(target_column, [])
            else:  # Default to sample data
                data_values = sample_data.get(target_column, [])
            
            # If no data values, keep empty mapping for this target column
            if not data_values:
                self.think(f"No data values found for {target_column}, using empty mapping")
                continue
            
            self.think(f"Found {len(data_values)} data values for {target_column}")
            
            # Find cell coordinates for the data values
            try:
                self.think(f"Searching for cell coordinates for {target_column}")
                cell_coordinates = find_cell_coordinates_for_data(
                    file_path, 
                    data_values
                )
                
                # Format the cell coordinates as a comma-separated string
                if cell_coordinates:
                    # Format the coordinates as a list of strings in the format "Sheet1!A1"
                    formatted_coords = [f"{sheet}!{cell}" for sheet, cell in cell_coordinates]
                    
                    self.think(f"Found {len(formatted_coords)} cell coordinates for {target_column}")
                    
                    # Add to mapping
                    mapping[target_column] = formatted_coords
                else:
                    self.think(f"No cell coordinates found for {target_column}, using empty mapping")
            except Exception as e:
                self.think(f"Error finding cell coordinates for {target_column}: {e}")
        
        self.think(f"Found cell coordinates for {len(mapping)} target columns")
        if mapping:
            # Display a few sample coordinates
            sample_entries = list(mapping.items())[:3]
            for target, coords in sample_entries:
                self.think(f"Cell coordinates for '{target}': {coords}")
            if len(mapping) > 3:
                self.think(f"And cell coordinates for {len(mapping) - 3} more target columns...")
        
        # Convert mapping to JSON
        mapping_json = json.dumps(mapping, indent=2)
        
        self.think("Finished finding cell coordinates")
        
        # Return the updated state
        return {
            **state,
            "cell_coordinates": mapping,
            "mapping_json": mapping_json
        }
