import logging
from typing import Dict, List, Any, Optional, Tuple, Annotated, TypedDict
from enum import Enum

from langchain_core.messages import BaseMessage
from langgraph.graph import StateGraph, END

from agents.header_extractor_agent import HeaderExtractorAgent
from agents.column_description_agent import ColumnDescriptionAgent
from agents.header_matching_agent import HeaderMatchingAgent
from agents.sample_data_agent import SampleDataAgent
from agents.suggestion_agent import SuggestionAgent
from agents.cell_coordinate_agent import CellCoordinateAgent
from agents.auto_cell_mapping_agent import AutoCellMappingAgent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the state type
class WorkflowState(TypedDict):
    file_path: str
    target_columns: List[str]
    potential_headers: Optional[List[str]]
    column_descriptions: Optional[Dict[str, Dict[str, Any]]]
    matches: Optional[Dict[str, Dict[str, str]]]
    sample_data: Optional[Dict[str, List[str]]]
    suggested_headers: Optional[Dict[str, str]]
    suggested_data: Optional[Dict[str, List[str]]]
    excel_preview: Optional[Dict[str, Any]]
    export_selections: Optional[Dict[str, str]]
    cell_coordinates: Optional[Dict[str, str]]
    column_ranges: Optional[Dict[str, List[Tuple[str, str, str, str]]]]
    error: Optional[str]

# Define the workflow steps
class WorkflowStep(str, Enum):
    EXTRACT_HEADERS = "extract_headers"
    DESCRIBE_COLUMNS = "describe_columns"
    MATCH_HEADERS = "match_headers"
    EXTRACT_SAMPLE_DATA = "extract_sample_data"
    SUGGEST = "suggest"
    FIND_CELL_COORDINATES = "find_cell_coordinates"
    AUTO_CELL_MAPPING = "auto_cell_mapping"
    END = "end"

# Create the agents
header_extractor_agent = HeaderExtractorAgent(verbose=True)
column_description_agent = ColumnDescriptionAgent(verbose=True)
header_matching_agent = HeaderMatchingAgent(verbose=True)
sample_data_agent = SampleDataAgent(verbose=True)
suggestion_agent = SuggestionAgent(verbose=True)
cell_coordinate_agent = CellCoordinateAgent(verbose=True)
auto_cell_mapping_agent = AutoCellMappingAgent(verbose=True)

# Define the workflow
def create_workflow() -> StateGraph:
    """
    Create the workflow graph.
    
    Returns:
        A StateGraph instance
    """
    # Create the graph
    workflow = StateGraph(WorkflowState)
    
    # Add the nodes
    workflow.add_node(WorkflowStep.EXTRACT_HEADERS, header_extractor_agent.run)
    workflow.add_node(WorkflowStep.DESCRIBE_COLUMNS, column_description_agent.run)
    workflow.add_node(WorkflowStep.MATCH_HEADERS, header_matching_agent.run)
    workflow.add_node(WorkflowStep.EXTRACT_SAMPLE_DATA, sample_data_agent.run)
    workflow.add_node(WorkflowStep.SUGGEST, suggestion_agent.run)
    workflow.add_node(WorkflowStep.FIND_CELL_COORDINATES, cell_coordinate_agent.run)
    workflow.add_node(WorkflowStep.AUTO_CELL_MAPPING, auto_cell_mapping_agent.run)
    
    # Define the edges
    workflow.add_edge(WorkflowStep.EXTRACT_HEADERS, WorkflowStep.DESCRIBE_COLUMNS)
    workflow.add_edge(WorkflowStep.DESCRIBE_COLUMNS, WorkflowStep.MATCH_HEADERS)
    workflow.add_edge(WorkflowStep.MATCH_HEADERS, WorkflowStep.EXTRACT_SAMPLE_DATA)
    workflow.add_edge(WorkflowStep.EXTRACT_SAMPLE_DATA, WorkflowStep.SUGGEST)
    workflow.add_edge(WorkflowStep.SUGGEST, WorkflowStep.FIND_CELL_COORDINATES)
    workflow.add_edge(WorkflowStep.FIND_CELL_COORDINATES, WorkflowStep.AUTO_CELL_MAPPING)
    workflow.add_edge(WorkflowStep.AUTO_CELL_MAPPING, END)
    
    # Set the entry point
    workflow.set_entry_point(WorkflowStep.EXTRACT_HEADERS)
    
    return workflow

# Function to run the workflow
def run_workflow(file_path: str, target_columns: List[str], skip_suggestion: bool = False) -> Dict[str, Any]:
    """
    Run the workflow with the given inputs.
    
    Args:
        file_path: The path to the Excel file
        target_columns: A list of target column names
        skip_suggestion: Whether to skip the suggestion step
        
    Returns:
        The final state of the workflow
    """
    # Create the initial state
    initial_state: WorkflowState = {
        "file_path": file_path,
        "target_columns": target_columns,
        "potential_headers": None,
        "column_descriptions": None,
        "matches": None,
        "sample_data": None,
        "suggested_headers": None,
        "suggested_data": None,
        "excel_preview": None,
        "export_selections": {},
        "cell_coordinates": None,
        "column_ranges": None,
        "error": None
    }
    
    # Create the workflow
    workflow = create_workflow()
    
    # Compile the workflow
    app = workflow.compile()
    
    try:
        if skip_suggestion:
            logger.info("Skipping suggestion step")
            # Run the workflow up to the sample data extraction step
            # First extract headers
            state = header_extractor_agent.run(initial_state)
            # Then describe columns
            state = column_description_agent.run(state)
            # Then match headers
            state = header_matching_agent.run(state)
            # Then extract sample data
            result = sample_data_agent.run(state)
            return result
        else:
            # Run the full workflow
            result = app.invoke(initial_state)
            return result
    except Exception as e:
        logger.error(f"Error running workflow: {e}")
        return {
            **initial_state,
            "error": str(e)
        }
