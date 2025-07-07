import os
import json
import logging
import uuid
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple

from langchain_core.messages import HumanMessage, SystemMessage
from utils.llm import get_llm

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema storage directory - use absolute path to avoid any path issues
SCHEMA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas"))
logger.info(f"Using schema directory: {SCHEMA_DIR}")
os.makedirs(SCHEMA_DIR, exist_ok=True)

def analyze_target_columns(file_path: str, sheet_name: Optional[str] = None, target_columns: Optional[List[str]] = None, existing_schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Analyze target columns from Excel file and return schema data.
    
    Args:
        file_path: Path to the Excel file
        sheet_name: Name of the sheet to analyze (uses first sheet if None)
        target_columns: List of target columns to analyze
        existing_schema: Optional existing schema to preserve explicit type specifications
    
    Returns:
        A dictionary containing the schema data
    """
    logger.info(f"Analyzing target columns in {file_path}")
    
    try:
        # Read the Excel file
        if sheet_name:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(file_path)
        
        # Clean column names FIRST before creating sample_df
        df.columns = df.columns.str.strip()
        
        # Get sample data for description generation AFTER cleaning column names
        sample_df = df.head(10)
        
        # Initialize the schema matching sample_schema.json format
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {},
            "required": []
        }
        
        # Debug logging for column names
        logger.info(f"DataFrame columns after cleaning: {list(df.columns)}")
        logger.info(f"Target columns to process: {target_columns}")
        
        # Process each target column if provided, or use all columns
        columns_to_process = target_columns if target_columns else df.columns.tolist()
        logger.info(f"Processing {len(columns_to_process)} columns")
        
        for column_name in columns_to_process:
            try:
                logger.info(f"Processing column: '{column_name}'")
                
                # Check if column exists in DataFrame
                if column_name not in df.columns:
                    logger.warning(f"Column '{column_name}' not found in DataFrame")
                    logger.info(f"Available columns: {list(df.columns)}")
                    
                    # Try to find a close match
                    close_match = None
                    for col in df.columns:
                        if col.lower().strip() == column_name.lower().strip():
                            close_match = col
                            break
                    
                    if close_match:
                        logger.info(f"Found close match: '{close_match}' for '{column_name}'")
                        column_name = close_match
                    else:
                        # Create a default schema entry for missing columns
                        logger.warning(f"No close match found for '{column_name}', creating default schema")
                        schema["properties"][column_name] = {
                            "description": f"The \"{column_name}\" column represents data related to {column_name.lower().replace('_', ' ')}.",
                            "type": "string"
                        }
                        schema["required"].append(column_name)
                        continue
                
                # Get the column data
                column_data = df[column_name]
                logger.info(f"Successfully accessed column '{column_name}' with {len(column_data)} rows")
                
                # Check if we have an existing schema with explicit type for this column
                if existing_schema and 'properties' in existing_schema and column_name in existing_schema['properties']:
                    existing_property = existing_schema['properties'][column_name]
                    # Preserve explicit type specifications from existing schema
                    if 'type' in existing_property:
                        type_info = {key: value for key, value in existing_property.items() if key in ['type', 'items']}
                        logger.info(f"Using existing schema type for '{column_name}': {type_info}")
                    else:
                        # Infer the type if no explicit type in existing schema
                        type_info = infer_type(column_data)
                        logger.info(f"Inferred type for '{column_name}': {type_info}")
                else:
                    # Infer the type if no existing schema
                    type_info = infer_type(column_data)
                    logger.info(f"Inferred type for '{column_name}': {type_info}")
                
                # Get sample data from the column (now both df and sample_df have cleaned column names)
                sample_data = sample_df[column_name].tolist()
                # Handle NaN values
                sample_data = [str(s) if pd.notna(s) else "" for s in sample_data]
                logger.info(f"Sample data for '{column_name}': {sample_data[:3]}...")
                
                # Generate a description
                description = generate_column_description(column_name, sample_data)
                logger.info(f"Generated description for '{column_name}': {description[:50]}...")
                
                # Create the property schema matching sample_schema.json format
                property_schema = {
                    "description": description,
                    "type": type_info["type"]
                }
                
                # Add any additional schema properties (like items for arrays)
                for key, value in type_info.items():
                    if key != "type":  # Already added
                        property_schema[key] = value
                
                # Add to the schema
                schema["properties"][column_name] = property_schema
                schema["required"].append(column_name)
                
                logger.info(f"Successfully processed column '{column_name}'")
                
            except Exception as column_error:
                logger.error(f"Error processing column '{column_name}': {column_error}", exc_info=True)
                
                # Create a fallback schema entry
                schema["properties"][column_name] = {
                    "description": f"The \"{column_name}\" column represents data related to {column_name.lower().replace('_', ' ')}.",
                    "type": "string"
                }
                schema["required"].append(column_name)
                
                # Continue processing other columns instead of failing completely
                continue
        
        logger.info(f"Schema generation completed with {len(schema['properties'])} properties")
        return schema
    
    except Exception as e:
        logger.error(f"Error analyzing target columns: {e}", exc_info=True)
        raise

def infer_type(series: pd.Series) -> Dict[str, Any]:
    """
    Infer the JSON schema type from a pandas Series.
    
    Args:
        series: A pandas Series representing a column of data
        
    Returns:
        A dictionary containing type information and additional schema properties
    """
    try:
        # Check if the series is entirely null
        if series.isna().all():
            return {"type": "string"}  # Default to string for empty columns
        
        # Get non-null values for analysis
        non_null = series.dropna()
        if len(non_null) == 0:
            return {"type": "string"}
        
        # Get the pandas dtype name
        dtype = series.dtype.name
        
        # For object type, need more sophisticated analysis
        if "object" in dtype:
            # Check if values look like arrays (contain separators)
            array_indicators = non_null.astype(str).str.contains(r'[,;|]', na=False)
            has_array_indicators = array_indicators.any()
            
            if has_array_indicators:
                # This looks like array data, determine the item type
                # Sample some values to determine the underlying type
                sample_values = []
                for val in non_null.head(10):
                    # Split by common separators
                    if isinstance(val, str):
                        parts = []
                        for sep in [',', ';', '|']:
                            if sep in val:
                                parts = [p.strip() for p in val.split(sep) if p.strip()]
                                break
                        if parts:
                            sample_values.extend(parts)
                        else:
                            sample_values.append(val)
                    else:
                        sample_values.append(str(val))
                
                # Analyze the sample values to determine item type
                item_type = "string"  # default
                if sample_values:
                    # Try to determine if all values are numbers
                    try:
                        numeric_values = []
                        for v in sample_values:
                            try:
                                numeric_values.append(float(v))
                            except:
                                break
                        if len(numeric_values) == len(sample_values):
                            item_type = "number"
                    except:
                        pass
                    
                    # Try to determine if all values are booleans
                    if item_type == "string":
                        boolean_values = [v.lower() in ['true', 'false', 'yes', 'no', '1', '0'] for v in sample_values]
                        if all(boolean_values):
                            item_type = "boolean"
                
                return {
                    "type": "array",
                    "items": {"type": item_type}
                }
            
            # Check uniqueness to determine if it should be array or single value
            unique_values = non_null.unique()
            if len(unique_values) > 1:
                # Multiple unique values - could be array if they look like lists
                # For now, default to string for object types without separators
                return {"type": "string"}
            else:
                # Only one unique value repeated - single value type
                sample_val = unique_values[0]
                # Try to infer type from the single value
                if isinstance(sample_val, str):
                    # Check if it's a number
                    try:
                        float(sample_val)
                        return {"type": "number"}
                    except:
                        pass
                    # Check if it's a boolean
                    if sample_val.lower() in ['true', 'false', 'yes', 'no']:
                        return {"type": "boolean"}
                return {"type": "string"}
        
        # Handle standard pandas dtypes
        elif "int" in dtype:
            # Check uniqueness for numbers too
            unique_values = non_null.unique()
            if len(unique_values) > 3:  # More than 3 unique values suggests array
                return {
                    "type": "array",
                    "items": {"type": "number"}
                }
            return {"type": "number"}
        elif "float" in dtype:
            # Check uniqueness for numbers too
            unique_values = non_null.unique()
            if len(unique_values) > 3:  # More than 3 unique values suggests array
                return {
                    "type": "array",
                    "items": {"type": "number"}
                }
            return {"type": "number"}
        elif "bool" in dtype:
            return {"type": "boolean"}
        elif "datetime" in dtype or "date" in dtype:
            return {"type": "string"}  # Use string for dates
        
        # Default to string for any other type
        return {"type": "string"}
    
    except Exception as e:
        logger.error(f"Error in infer_type: {e}")
        return {"type": "string"}  # Safe fallback

def generate_column_description(column_name: str, sample_data: List[Any]) -> str:
    """
    Generate a description for a column using LLM.
    
    Args:
        column_name: The name of the column
        sample_data: Sample data from the column
        
    Returns:
        A string description of what the column represents
    """
    try:
        # Clean and prepare sample data
        clean_samples = []
        for item in sample_data:
            if pd.isna(item):
                clean_samples.append("NULL")
            elif isinstance(item, (int, float, bool)):
                clean_samples.append(str(item))
            elif isinstance(item, str):
                clean_samples.append(f'"{item}"')
            else:
                clean_samples.append(str(item))
        
        # Limit to 5 samples
        sample_str = ", ".join(clean_samples[:5])
        
        # Use LLM to generate description
        llm = get_llm()
        
        system_message = SystemMessage(
            content="You are a data analyst who provides concise, accurate descriptions of data columns."
        )
        
        human_message = HumanMessage(
            content=f"""
            Column name: "{column_name}"
            Sample values: {sample_str}
            
            Based on the column name and sample values, provide a concise one-sentence description 
            of what this column represents. Focus on the business meaning, not the data type.
            
            Description:
            """
        )
        
        # Call the LLM
        response = llm.invoke([system_message, human_message])
        
        # Extract the description
        description = response.content.strip()
        
        # Remove quotes if the model returned them
        description = description.strip('"')
        
        return description
    
    except Exception as e:
        logger.error(f"Error generating description for column '{column_name}': {e}")
        return f"Column containing {column_name} data"

def create_initial_schema(target_columns: List[str], file_path: Optional[str] = None, sheet_name: Optional[str] = None, existing_schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Create an initial schema based on target columns.
    
    Args:
        target_columns: List of target column names
        file_path: Optional path to Excel file for type inference
        sheet_name: Optional sheet name if file_path provided
        existing_schema: Optional existing schema to preserve explicit type specifications
    
    Returns:
        A dictionary containing the schema
    """
    if file_path:
        # Use Excel file for type inference and LLM for descriptions
        logger.info(f"Creating schema with type inference from Excel file: {file_path}")
        return analyze_target_columns(file_path, sheet_name, target_columns, existing_schema)
    else:
        # Create a basic schema with string types and auto-generated descriptions
        logger.info("Creating basic schema with default values (no Excel file provided)")
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {},
            "required": target_columns  # Set all target columns as required
        }
        
        for column_name in target_columns:
            schema["properties"][column_name] = {
                "description": f"The \"{column_name}\" column represents data related to {column_name.lower().replace('_', ' ')}.",
                "type": "string"   # Default to string type
            }
        
        return schema

def save_schema(schema_data: Dict[str, Any], schema_name: str) -> Dict[str, Any]:
    """
    Save a schema with a name.
    
    Args:
        schema_data: The schema to save (should be just the schema, not wrapped)
        schema_name: The name to save the schema as
    
    Returns:
        A dictionary with status information
    """
    try:
        # Create a unique ID for the schema
        schema_id = str(uuid.uuid4())
        
        logger.info(f"Saving schema with ID: {schema_id}, name: {schema_name}")
        
        # Validate that we're saving just the schema data, not the wrapped format
        if "id" in schema_data or "name" in schema_data or "timestamp" in schema_data:
            logger.warning("Schema data appears to be wrapped, extracting actual schema")
            if "schema" in schema_data:
                actual_schema = schema_data["schema"]
            else:
                # If it has metadata fields but no schema field, assume the whole thing is the schema
                actual_schema = {k: v for k, v in schema_data.items() 
                               if k not in ["id", "name", "timestamp"]}
        else:
            actual_schema = schema_data
        
        # Validate the actual schema
        validation = validate_schema(actual_schema)
        if not validation.get('valid', False):
            logger.error(f"Invalid schema being saved: {validation.get('error')}")
            return {
                "success": False,
                "error": f"Invalid schema: {validation.get('error', 'Unknown error')}"
            }
        
        # Check if this is an array of objects schema
        is_array_schema = is_array_of_object_schema(actual_schema)
        
        # Create the schema object with metadata wrapper
        schema_obj = {
            "id": schema_id,
            "name": schema_name,
            "timestamp": pd.Timestamp.now().isoformat(),
            "schema": actual_schema
        }
        
        # Add array metadata
        if is_array_schema:
            # Extract array name and description
            array_property = next(iter(actual_schema["properties"].values()))
            array_name = next(iter(actual_schema["properties"].keys()))
            array_description = array_property.get("description", "")
            
            schema_obj["is_array_of_objects"] = True
            schema_obj["array_config"] = {
                "name": array_name,
                "description": array_description
            }
        else:
            schema_obj["is_array_of_objects"] = False
        
        # Ensure schema directory exists
        os.makedirs(SCHEMA_DIR, exist_ok=True)
        
        # Save to file
        file_path = os.path.join(SCHEMA_DIR, f"{schema_id}.json")
        with open(file_path, 'w') as f:
            json.dump(schema_obj, f, indent=2)
        
        # Verify the file was written
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            logger.info(f"Schema saved successfully to {file_path} ({file_size} bytes)")
        else:
            logger.error(f"Failed to write schema file: {file_path} does not exist after save attempt")
            return {
                "success": False,
                "error": "Failed to write schema file"
            }
        
        return {
            "success": True,
            "id": schema_id,
            "name": schema_name,
            "message": "Schema saved successfully"
        }
    
    except Exception as e:
        logger.error(f"Error saving schema: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def list_saved_schemas() -> List[Dict[str, Any]]:
    """
    List all saved schemas.
    
    Returns:
        A list of schema metadata
    """
    schemas = []
    
    try:
        # Ensure schema directory exists
        os.makedirs(SCHEMA_DIR, exist_ok=True)
        
        # Check if directory is empty
        if not os.path.exists(SCHEMA_DIR) or not os.listdir(SCHEMA_DIR):
            logger.info(f"Schema directory is empty or does not exist: {SCHEMA_DIR}")
            return []
        
        # List contents of the directory for debugging
        dir_contents = os.listdir(SCHEMA_DIR)
        json_files = [f for f in dir_contents if f.endswith('.json')]
        logger.info(f"Found {len(dir_contents)} items in schema directory, {len(json_files)} JSON files")
        
        # List all JSON files in the schema directory
        for filename in json_files:
            file_path = os.path.join(SCHEMA_DIR, filename)
            
            try:
                if os.path.isfile(file_path):
                    with open(file_path, 'r') as f:
                        schema_obj = json.load(f)
                    
                    # Extract metadata
                    schema_id = schema_obj.get("id", os.path.splitext(filename)[0])
                    schema_name = schema_obj.get("name", "Unnamed schema")
                    timestamp = schema_obj.get("timestamp", "")
                    
                    logger.info(f"Found schema: ID={schema_id}, Name={schema_name}, Time={timestamp}")
                    
                    schemas.append({
                        "id": schema_id,
                        "name": schema_name,
                        "timestamp": timestamp
                    })
                else:
                    logger.warning(f"Found non-file item in schema directory: {filename}")
            except Exception as e:
                logger.error(f"Error reading schema file {filename}: {e}", exc_info=True)
        
        # Sort by timestamp (newest first)
        schemas.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        logger.info(f"Returning {len(schemas)} schemas")
        return schemas
    
    except Exception as e:
        logger.error(f"Error listing schemas: {e}", exc_info=True)
        return []

def load_schema(schema_id: str) -> Dict[str, Any]:
    """
    Load a schema by ID.
    
    Args:
        schema_id: The ID of the schema to load
    
    Returns:
        The schema data or error information
    """
    try:
        file_path = os.path.join(SCHEMA_DIR, f"{schema_id}.json")
        
        logger.info(f"Attempting to load schema from: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"Schema file not found: {file_path}")
            return {
                "success": False,
                "error": f"Schema with ID {schema_id} not found"
            }
        
        with open(file_path, 'r') as f:
            schema_obj = json.load(f)
        
        logger.info(f"Successfully loaded schema: ID={schema_id}, Name={schema_obj.get('name', 'Unnamed schema')}")
        
        return {
            "success": True,
            "id": schema_obj.get("id", schema_id),
            "name": schema_obj.get("name", "Unnamed schema"),
            "timestamp": schema_obj.get("timestamp", ""),
            "schema": schema_obj.get("schema", {})
        }
    
    except Exception as e:
        logger.error(f"Error loading schema {schema_id}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def delete_schema(schema_id: str) -> Dict[str, Any]:
    """
    Delete a schema by ID.
    
    Args:
        schema_id: The ID of the schema to delete
    
    Returns:
        A dictionary with status information
    """
    try:
        # Clean the schema_id to ensure it's safe
        schema_id = schema_id.strip()
        logger.info(f"Attempting to delete schema with ID: {schema_id}")
        
        file_path = os.path.join(SCHEMA_DIR, f"{schema_id}.json")
        logger.info(f"Full file path for deletion: {file_path}")
        
        # Check if schema directory exists
        if not os.path.exists(SCHEMA_DIR):
            logger.error(f"Schema directory does not exist: {SCHEMA_DIR}")
            return {
                "success": False,
                "error": f"Schema directory not found: {SCHEMA_DIR}"
            }
        
        # Check if the file exists
        if not os.path.exists(file_path):
            logger.error(f"Schema file does not exist: {file_path}")
            # List all schemas to help debug
            all_schemas = os.listdir(SCHEMA_DIR)
            logger.info(f"Available schema files: {all_schemas}")
            return {
                "success": False,
                "error": f"Schema with ID {schema_id} not found"
            }
        
        # Check if we have permission to delete the file
        if not os.access(file_path, os.W_OK):
            logger.error(f"No write permission for schema file: {file_path}")
            return {
                "success": False,
                "error": f"No permission to delete schema file: {file_path}"
            }
        
        # Delete the file
        logger.info(f"Deleting schema file: {file_path}")
        os.remove(file_path)
        
        # Verify deletion
        if os.path.exists(file_path):
            logger.error(f"Failed to delete schema file: {file_path}")
            return {
                "success": False,
                "error": f"File deletion failed for unknown reason"
            }
        
        logger.info(f"Schema {schema_id} deleted successfully")
        return {
            "success": True,
            "message": f"Schema {schema_id} deleted successfully"
        }
    
    except Exception as e:
        logger.error(f"Error deleting schema {schema_id}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

def convert_property_to_optional(property_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a property schema from required format to optional format (allows null).
    
    Args:
        property_schema: The property schema to convert
    
    Returns:
        Updated property schema that allows null values
    """
    # If already has anyOf with null, return as is
    if "anyOf" in property_schema:
        has_null = any(item.get("type") == "null" for item in property_schema["anyOf"])
        if has_null:
            return property_schema
    
    # Get the description to preserve it
    description = property_schema.get("description", "")
    
    # Ensure we have a type field
    if "type" not in property_schema:
        logger.warning(f"Property schema missing 'type' field: {property_schema}")
        # Add a fallback type
        property_schema = property_schema.copy()
        property_schema["type"] = "string"
    
    # Handle array types
    if property_schema.get("type") == "array":
        main_type_obj = {
            "type": "array",
            "items": property_schema.get("items", {"type": "string"})
        }
        # Copy any additional properties from the original schema
        for key, value in property_schema.items():
            if key not in ["type", "items", "description"]:
                main_type_obj[key] = value
        
        return {
            "anyOf": [
                main_type_obj,
                {"type": "null"}
            ],
            "description": description
        }
    
    # Handle non-array types
    else:
        main_type_obj = {"type": property_schema["type"]}
        # Copy any additional properties from the original schema
        for key, value in property_schema.items():
            if key not in ["type", "description"]:
                main_type_obj[key] = value
        
        return {
            "anyOf": [
                main_type_obj,
                {"type": "null"}
            ],
            "description": description
        }

def convert_property_to_required(property_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a property schema from optional format to required format (removes null option).
    
    Args:
        property_schema: The property schema to convert
    
    Returns:
        Updated property schema that doesn't allow null values
    """
    # Get the description to preserve it
    description = property_schema.get("description", "")
    
    # Handle anyOf format
    if "anyOf" in property_schema:
        # Find the non-null type
        non_null_types = [item for item in property_schema["anyOf"] if item.get("type") != "null"]
        
        if non_null_types:
            # Take the first non-null type
            main_type = non_null_types[0]
            
            # Create the result schema starting with the main type
            result = {
                "description": description
            }
            
            # Copy all properties from the main type
            for key, value in main_type.items():
                result[key] = value
            
            # Ensure 'type' field is present
            if "type" not in result:
                result["type"] = "string"  # fallback
            
            # Special handling for arrays - ensure items is properly structured
            if result.get("type") == "array" and "items" in result:
                items = result["items"]
                # If items has anyOf, extract the non-null type
                if isinstance(items, dict) and "anyOf" in items:
                    non_null_item_types = [item for item in items["anyOf"] if item.get("type") != "null"]
                    if non_null_item_types:
                        result["items"] = non_null_item_types[0]
                    else:
                        result["items"] = {"type": "string"}  # fallback
            
            return result
        else:
            # No non-null types found, fallback to string
            return {
                "type": "string",
                "description": description
            }
    
    # If already in required format, ensure it has a type field
    if "type" not in property_schema:
        # This is the problematic case - return a corrected schema
        result = property_schema.copy()
        result["type"] = "string"  # fallback type
        return result
    
    # Already in required format and has type field
    return property_schema

def update_schema_required_status(schema_data: Dict[str, Any], column_name: str, is_required: bool) -> Dict[str, Any]:
    """
    Update a schema to change the required status of a specific column.
    
    Args:
        schema_data: The schema to update
        column_name: The column to update
        is_required: Whether the column should be required
    
    Returns:
        Updated schema data
    """
    import copy
    
    # Make a deep copy to avoid modifying the original
    updated_schema = copy.deepcopy(schema_data)
    
    # Ensure required array exists
    if "required" not in updated_schema:
        updated_schema["required"] = []
    
    # Ensure properties exists
    if "properties" not in updated_schema:
        updated_schema["properties"] = {}
    
    # Check if the column exists in properties
    if column_name not in updated_schema["properties"]:
        logger.warning(f"Column '{column_name}' not found in schema properties")
        return updated_schema
    
    logger.info(f"Updating required status for '{column_name}' to {is_required}")
    logger.info(f"Current required list: {updated_schema.get('required', [])}")
    
    # Update the required array
    if is_required:
        # Add to required list if not already there
        if column_name not in updated_schema["required"]:
            updated_schema["required"].append(column_name)
            logger.info(f"Added '{column_name}' to required list")
        
        # Convert property to required format (remove anyOf with null)
        original_property = updated_schema["properties"][column_name]
        updated_property = convert_property_to_required(original_property)
        updated_schema["properties"][column_name] = updated_property
        logger.info(f"Converted property '{column_name}' to required format")
        
    else:
        # Remove from required list if present
        if column_name in updated_schema["required"]:
            updated_schema["required"].remove(column_name)
            logger.info(f"Removed '{column_name}' from required list")
        
        # Convert property to optional format (add anyOf with null)
        original_property = updated_schema["properties"][column_name]
        updated_property = convert_property_to_optional(original_property)
        updated_schema["properties"][column_name] = updated_property
        logger.info(f"Converted property '{column_name}' to optional format")
    
    logger.info(f"Final required list: {updated_schema.get('required', [])}")
    return updated_schema

def is_array_of_object_schema(schema_data: Dict[str, Any]) -> bool:
    """
    Check if schema represents array of objects format.
    
    Args:
        schema_data: The schema to check
    
    Returns:
        True if this is an array of objects schema, False otherwise
    """
    try:
        if not isinstance(schema_data, dict):
            return False
        
        properties = schema_data.get("properties", {})
        
        # Check if there's exactly one property that's an array of objects
        if len(properties) != 1:
            return False
        
        # Get the single property
        array_property = next(iter(properties.values()))
        
        # Check if it's an array type with object items
        if array_property.get("type") == "array":
            items = array_property.get("items", {})
            return (items.get("type") == "object" and 
                    "properties" in items and 
                    isinstance(items["properties"], dict))
        
        return False
    
    except Exception as e:
        logger.error(f"Error checking array of object schema: {e}")
        return False

def convert_to_array_of_object_schema(schema_data: Dict[str, Any], array_name: str, array_description: str) -> Dict[str, Any]:
    """
    Convert regular schema to array of object format.
    
    Args:
        schema_data: The original schema
        array_name: Name for the array wrapper
        array_description: Description for the array wrapper
    
    Returns:
        Schema converted to array of objects format
    """
    try:
        if not schema_data or "properties" not in schema_data:
            raise ValueError("Invalid schema data")
        
        # Flatten array properties to their base types
        flattened_properties = flatten_array_properties(schema_data["properties"])
        
        # Create array of objects schema
        array_schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                array_name: {
                    "description": array_description,
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": flattened_properties,
                        "required": schema_data.get("required", [])
                    }
                }
            },
            "required": [array_name]
        }
        
        logger.info(f"Converted schema to array of objects with name '{array_name}'")
        return array_schema
    
    except Exception as e:
        logger.error(f"Error converting to array of object schema: {e}")
        raise

def convert_from_array_of_object_schema(schema_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract inner schema from array of object format.
    
    Args:
        schema_data: The array of objects schema
    
    Returns:
        The extracted inner schema
    """
    try:
        if not is_array_of_object_schema(schema_data):
            raise ValueError("Schema is not in array of objects format")
        
        # Get the first (and only) property which should be the array
        array_property = next(iter(schema_data["properties"].values()))
        items_schema = array_property["items"]
        
        # Extract the inner schema
        inner_schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": items_schema.get("properties", {}),
            "required": items_schema.get("required", [])
        }
        
        logger.info("Extracted inner schema from array of objects")
        return inner_schema
    
    except Exception as e:
        logger.error(f"Error converting from array of object schema: {e}")
        raise

def flatten_array_properties(properties: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert array properties to their base types.
    
    Args:
        properties: Dictionary of property schemas
    
    Returns:
        Properties with arrays flattened to base types
    """
    flattened = {}
    
    for prop_name, prop_schema in properties.items():
        try:
            # Create a copy to avoid modifying original
            new_prop = prop_schema.copy()
            
            # Handle direct array type
            if prop_schema.get("type") == "array":
                items = prop_schema.get("items", {})
                base_type = items.get("type", "string")
                
                # Convert to base type, preserve description
                new_prop = {
                    "type": base_type,
                    "description": prop_schema.get("description", "")
                }
                logger.info(f"Flattened array property '{prop_name}' from array to {base_type}")
            
            # Handle anyOf structure with arrays
            elif "anyOf" in prop_schema:
                anyof_items = prop_schema["anyOf"]
                array_item = None
                null_item = None
                
                # Find array and null items
                for item in anyof_items:
                    if item.get("type") == "array":
                        array_item = item
                    elif item.get("type") == "null":
                        null_item = item
                
                if array_item:
                    # Convert array to base type, keep anyOf structure for optional
                    items = array_item.get("items", {})
                    base_type = items.get("type", "string")
                    
                    if null_item:
                        # Optional field - use anyOf with base type and null
                        new_prop = {
                            "anyOf": [
                                {"type": base_type},
                                {"type": "null"}
                            ],
                            "description": prop_schema.get("description", "")
                        }
                    else:
                        # Required field - use base type directly
                        new_prop = {
                            "type": base_type,
                            "description": prop_schema.get("description", "")
                        }
                    logger.info(f"Flattened anyOf array property '{prop_name}' to {base_type}")
            
            flattened[prop_name] = new_prop
            
        except Exception as e:
            logger.error(f"Error flattening property '{prop_name}': {e}")
            # Keep original property if flattening fails
            flattened[prop_name] = prop_schema
    
    return flattened

def validate_schema(schema_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a schema for correctness.
    
    Args:
        schema_data: The schema to validate
    
    Returns:
        A dictionary with validation results
    """
    try:
        # Check for required top-level properties
        required_props = ["type", "properties"]
        missing_props = [prop for prop in required_props if prop not in schema_data]
        
        if missing_props:
            return {
                "valid": False,
                "error": f"Schema missing required properties: {', '.join(missing_props)}"
            }
        
        # Check that type is "object"
        if schema_data.get("type") != "object":
            return {
                "valid": False,
                "error": "Schema must have type 'object'"
            }
        
        # Check properties
        properties = schema_data.get("properties", {})
        if not properties:
            return {
                "valid": False,
                "error": "Schema must have at least one property"
            }
        
        # Special validation for array of objects schema
        if is_array_of_object_schema(schema_data):
            # Validate array structure
            array_property = next(iter(properties.values()))
            items = array_property.get("items", {})
            
            if not items.get("properties"):
                return {
                    "valid": False,
                    "error": "Array of objects schema must have properties in items"
                }
            
            # Validate each property in the array items
            for prop_name, prop_schema in items.get("properties", {}).items():
                has_type = False
                
                # Check for direct type field
                if "type" in prop_schema:
                    has_type = True
                # Check for anyOf with type fields
                elif "anyOf" in prop_schema:
                    anyof_items = prop_schema.get("anyOf", [])
                    if any("type" in item for item in anyof_items):
                        has_type = True
                
                if not has_type:
                    logger.error(f"Array item property '{prop_name}' validation failed. Schema: {prop_schema}")
                    return {
                        "valid": False,
                        "error": f"Array item property '{prop_name}' missing required 'type' field"
                    }
        else:
            # Standard schema validation - check that all properties have a type (either directly or in anyOf)
            for prop_name, prop_schema in properties.items():
                has_type = False
                
                # Check for direct type field
                if "type" in prop_schema:
                    has_type = True
                # Check for anyOf with type fields
                elif "anyOf" in prop_schema:
                    anyof_items = prop_schema.get("anyOf", [])
                    if any("type" in item for item in anyof_items):
                        has_type = True
                
                if not has_type:
                    logger.error(f"Property '{prop_name}' validation failed. Schema: {prop_schema}")
                    return {
                        "valid": False,
                        "error": f"Property '{prop_name}' missing required 'type' field"
                    }
        
        # Schema is valid
        return {
            "valid": True,
            "message": "Schema is valid"
        }
    
    except Exception as e:
        logger.error(f"Error validating schema: {e}")
        return {
            "valid": False,
            "error": str(e)
        }
