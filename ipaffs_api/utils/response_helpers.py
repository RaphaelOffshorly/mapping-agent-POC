import json
import uuid
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
import numpy as np
import pandas as pd
from flask import jsonify

class NpEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle NumPy values and NaN."""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (np.bool_)):
            return bool(obj)
        if pd.isna(obj):
            return ""
        return super(NpEncoder, self).default(obj)

class ResponseFormatter:
    """Utility class for formatting consistent API responses."""
    
    @staticmethod
    def success_response(
        data: Any = None,
        csv_data: Optional[Dict] = None,
        session_id: Optional[str] = None,
        message: Optional[str] = None,
        meta: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Create a standardized success response."""
        response = {
            "success": True,
            "data": data,
            "csv_data": csv_data,
            "meta": {
                "session_id": session_id or str(uuid.uuid4()),
                "timestamp": datetime.utcnow().isoformat(),
                "format": ResponseFormatter._determine_format(csv_data),
                **(meta or {})
            },
            "error": None
        }
        
        if message:
            response["message"] = message
            
        return response
    
    @staticmethod
    def error_response(
        error_message: str,
        error_code: Optional[str] = None,
        status_code: int = 400,
        session_id: Optional[str] = None,
        data: Any = None
    ) -> tuple:
        """Create a standardized error response with status code."""
        response = {
            "success": False,
            "data": data,
            "csv_data": None,
            "meta": {
                "session_id": session_id,
                "timestamp": datetime.utcnow().isoformat(),
                "format": None,
                "error_code": error_code
            },
            "error": error_message
        }
        
        return jsonify(response), status_code
    
    @staticmethod
    def csv_response(
        csv_data: Dict,
        session_id: Optional[str] = None,
        message: Optional[str] = None,
        additional_data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Create a response focused on CSV data."""
        return ResponseFormatter.success_response(
            data=additional_data,
            csv_data=csv_data,
            session_id=session_id,
            message=message
        )
    
    @staticmethod
    def _determine_format(csv_data: Optional[Dict]) -> Optional[str]:
        """Determine the format of CSV data."""
        if not csv_data or not csv_data.get("data"):
            return None
            
        data_rows = csv_data.get("data", [])
        headers = csv_data.get("headers", [])
        
        if len(data_rows) == 1:
            return "single_row"
        elif len(data_rows) > 1:
            # Check if it's array of objects format
            if headers and all(isinstance(row, dict) for row in data_rows):
                return "array_of_objects"
            else:
                return "multi_row"
        else:
            return "empty"
    
    @staticmethod
    def sanitize_csv_data(csv_data: Dict) -> Dict:
        """Sanitize CSV data to remove NaN values and ensure JSON compatibility."""
        if not csv_data:
            return csv_data
            
        sanitized_data = {
            "headers": csv_data.get("headers", []),
            "data": []
        }
        
        for row in csv_data.get("data", []):
            if isinstance(row, dict):
                sanitized_row = {}
                for key, value in row.items():
                    # Convert NaN values to empty strings
                    if value is None or (isinstance(value, float) and np.isnan(value)):
                        sanitized_row[key] = ""
                    elif isinstance(value, (dict, list)):
                        sanitized_row[key] = json.dumps(value)
                    else:
                        sanitized_row[key] = value
                sanitized_data["data"].append(sanitized_row)
            else:
                # Handle list format
                sanitized_data["data"].append(row)
        
        return sanitized_data

def convert_extracted_data_to_csv(extracted_data: Dict) -> Dict:
    """Convert extracted data from various formats to standardized CSV format."""
    if not extracted_data:
        return {"headers": [], "data": []}
    
    # Check for array of objects format
    array_field = None
    for field, value in extracted_data.items():
        if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
            array_field = field
            break
    
    if array_field:
        # Array of objects format
        objects_array = extracted_data[array_field]
        
        # Extract all unique keys as headers
        all_keys = set()
        for obj in objects_array:
            if isinstance(obj, dict):
                all_keys.update(obj.keys())
        
        headers = sorted(list(all_keys))
        
        # Convert each object to a row
        data_rows = []
        for obj in objects_array:
            if isinstance(obj, dict):
                row = {}
                for header in headers:
                    value = obj.get(header, "")
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    elif value is None:
                        value = ""
                    row[header] = str(value)
                data_rows.append(row)
        
        return {"headers": headers, "data": data_rows}
    else:
        # Single row format
        headers = list(extracted_data.keys())
        row = {}
        
        for field in headers:
            value = extracted_data.get(field, "")
            if isinstance(value, (list, dict)):
                value = json.dumps(value)
            elif value is None:
                value = ""
            row[field] = str(value)
        
        return {"headers": headers, "data": [row]}

def convert_sample_data_to_csv(target_columns: List[str], sample_data: Dict) -> Dict:
    """Convert sample data format to CSV format."""
    if not target_columns or not sample_data:
        return {"headers": [], "data": []}
    
    # Determine maximum number of rows
    max_rows = 0
    for col in target_columns:
        col_data = sample_data.get(col, [])
        max_rows = max(max_rows, len(col_data))
    
    # Create rows
    data_rows = []
    for row_idx in range(max_rows):
        row = {}
        for col in target_columns:
            col_data = sample_data.get(col, [])
            row[col] = col_data[row_idx] if row_idx < len(col_data) else ""
        data_rows.append(row)
    
    return {"headers": target_columns, "data": data_rows}
