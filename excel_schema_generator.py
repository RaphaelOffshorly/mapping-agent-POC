#!/usr/bin/env python3
"""
Excel Schema Generator

This script reads an Excel file and generates a JSON schema where each column
becomes a property with its data type and an LLM-generated description.
"""

import os
import sys
import json
import argparse
import pandas as pd
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    print("Error: ANTHROPIC_API_KEY environment variable not found.")
    print("Please create a .env file with your Anthropic API key or set it in your environment.")
    sys.exit(1)

# Initialize the Claude model
llm = ChatAnthropic(
    model="claude-3-7-sonnet-latest",
    anthropic_api_key=ANTHROPIC_API_KEY,
    temperature=0.3
)

def infer_type(series: pd.Series) -> Dict[str, Any]:
    """
    Infer the JSON schema type from a pandas Series.
    
    Args:
        series: A pandas Series representing a column of data
        
    Returns:
        A dictionary containing type information and additional schema properties
    """
    # Check if the series is entirely null
    if series.isna().all():
        return {"type": "string"}  # Default to string for empty columns
    
    # Get the pandas dtype name
    dtype = series.dtype.name
    
    # Handle different pandas dtypes
    if "int" in dtype:
        return {"type": "integer"}
    elif "float" in dtype:
        return {"type": "number"}
    elif "bool" in dtype:
        return {"type": "boolean"}
    elif "datetime" in dtype or "date" in dtype:
        return {"type": "string"}  # Use string for dates with format
    elif "object" in dtype:
        # For object type, check if all non-null values are strings
        non_null = series.dropna()
        if len(non_null) == 0:
            return {"type": "string"}
        
        # Check if all values are strings
        if all(isinstance(x, str) for x in non_null):
            # Check if there are multiple unique values that might indicate an array
            unique_values = non_null.unique()
            if len(unique_values) > 1 and len(unique_values) < len(non_null) * 0.7:  # Heuristic for potential array
                return {
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {"type": "string"},
                            {"type": "null"}
                        ]
                    }
                }
            return {"type": "string"}
        
        # Check if all values are lists or tuples (explicit arrays)
        if all(isinstance(x, (list, tuple)) for x in non_null):
            # Determine the type of items in the array
            item_type = "string"  # Default
            
            # Sample items to determine their type
            items = [item for sublist in non_null for item in sublist if not pd.isna(item)]
            
            if items:
                if all(isinstance(x, int) for x in items):
                    item_type = "integer"
                elif all(isinstance(x, float) for x in items):
                    item_type = "number"
                elif all(isinstance(x, bool) for x in items):
                    item_type = "boolean"
                # Default to string for mixed types or actual strings
            
            return {
                "type": "array",
                "items": {
                    "anyOf": [
                        {"type": item_type},
                        {"type": "null"}
                    ]
                }
            }
            
        # Check if all values are dictionaries
        if all(isinstance(x, dict) for x in non_null):
            return {"type": "object"}
            
    # Default to string for any other type
    return {"type": "string"}

def generate_description(column_name: str, sample_data: List[Any]) -> str:
    """
    Generate a description for a column using Anthropic's Claude model via LangChain.
    
    Args:
        column_name: The name of the column
        sample_data: Sample data from the column
        
    Returns:
        A string description of what the column represents
    """
    # Clean and prepare sample data for the prompt
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
    
    # Limit to 5 samples to keep the prompt size reasonable
    sample_str = ", ".join(clean_samples[:5])
    
    # Create the messages for Claude
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
    
    try:
        # Call Claude via LangChain
        response = llm.invoke([system_message, human_message])
        
        # Extract the description from the response
        description = response.content.strip()
        
        # Remove quotes if the model returned them
        description = description.strip('"')
        
        return description
    
    except Exception as e:
        print(f"Error generating description for column '{column_name}': {e}")
        return f"Column containing {column_name} data"

def generate_schema(excel_path: str, sheet_name: Optional[str] = None, sample_rows: int = 10) -> Dict[str, Any]:
    """
    Generate a JSON schema from an Excel file.
    
    Args:
        excel_path: Path to the Excel file
        sheet_name: Name of the sheet to use (uses first sheet if None)
        sample_rows: Number of rows to sample for type inference and description generation
        
    Returns:
        A dictionary representing the JSON schema
    """
    try:
        # Read the Excel file
        print(f"Reading Excel file: {excel_path}")
        if sheet_name:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(excel_path)
        
        # Get a sample of rows for description generation
        sample_df = df.head(sample_rows)
        
        # Initialize the schema
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {},
            "required": []
        }
        
        # Process each column
        total_columns = len(df.columns)
        for i, column_name in enumerate(df.columns):
            print(f"Processing column {i+1}/{total_columns}: {column_name}")
            
            # Infer the type and additional schema properties
            type_info = infer_type(df[column_name])
            
            # Generate a description
            sample_data = sample_df[column_name].tolist()
            
            # Handle REF/reference errors or blank data by making assumptions
            has_errors = False
            for idx, item in enumerate(sample_data):
                if pd.isna(item) or (isinstance(item, str) and ("#REF" in item or "reference error" in item.lower())):
                    has_errors = True
            
            description = generate_description(column_name, sample_data)
            if has_errors:
                description += " (Note: Some sample data contained errors or was blank; description is based on available data and column name)"
            
            # Create the property schema
            property_schema = {"description": description}
            
            # Add type information and any additional schema properties
            for key, value in type_info.items():
                property_schema[key] = value
            
            # Add to the schema
            schema["properties"][column_name] = property_schema
            
            # Add all columns to required fields regardless of null values
            schema["required"].append(column_name)
        
        return schema
    
    except Exception as e:
        print(f"Error generating schema: {e}")
        sys.exit(1)

def main():
    """Main function to parse arguments and generate the schema."""
    parser = argparse.ArgumentParser(description="Generate a JSON schema from an Excel file")
    parser.add_argument("excel_path", help="Path to the Excel file")
    parser.add_argument("--output", "-o", help="Output JSON file path (defaults to excel_filename_schema.json if not specified)")
    parser.add_argument("--sheet", "-s", help="Sheet name to use (uses first sheet if not specified)")
    parser.add_argument("--sample", "-n", type=int, default=10, help="Number of rows to sample for description generation")
    
    args = parser.parse_args()
    
    # Generate the schema
    schema = generate_schema(args.excel_path, args.sheet, args.sample)
    
    # Determine output path
    output_path = args.output
    if not output_path:
        # Create default output path based on input filename
        base_filename = os.path.basename(args.excel_path)
        filename_without_ext = os.path.splitext(base_filename)[0]
        output_path = f"{filename_without_ext}_schema.json"
    
    # Output the schema to file
    with open(output_path, 'w') as f:
        json.dump(schema, f, indent=2)
    print(f"Schema written to {output_path}")
    
    # Also print to stdout for convenience
    print("\nGenerated Schema:")
    print(json.dumps(schema, indent=2))

if __name__ == "__main__":
    main()
