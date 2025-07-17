import os
import json
import tempfile
import logging
from typing import Dict, Any, Optional, Tuple
from werkzeug.utils import secure_filename

# Import existing agents and utilities from the main project
import sys
sys.path.append('..')

from agents.pdf_extract_agent import PDFExtractAgent
from ipaffs_api.utils.response_helpers import convert_extracted_data_to_csv, ResponseFormatter
from ipaffs_api.config.api_config import APIConfig

logger = logging.getLogger(__name__)

class PDFExtractionService:
    """Service for handling PDF extraction operations."""
    
    def __init__(self):
        self.upload_folder = APIConfig.UPLOAD_FOLDER
        self.ipaffs_schema_path = APIConfig.IPAFFS_SCHEMA_PATH
        
    def extract_ipaffs_pdf(self, pdf_content: bytes, filename: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Extract data from PDF using IPAFFS schema.
        
        Args:
            pdf_content: PDF file content as bytes
            filename: Original filename
            
        Returns:
            Tuple of (success, result_data)
        """
        try:
            # Save PDF temporarily
            safe_filename = secure_filename(filename)
            pdf_filepath = os.path.join(self.upload_folder, f"temp_ipaffs_{safe_filename}")
            
            with open(pdf_filepath, 'wb') as f:
                f.write(pdf_content)
            
            logger.info(f"Saved PDF file: {pdf_filepath}")
            
            # Load IPAFFS schema
            ipaffs_schema_path = os.path.join(os.getcwd(), self.ipaffs_schema_path)
            if not os.path.exists(ipaffs_schema_path):
                return False, {"error": "IPAFFS schema file not found"}
            
            # Load schema data
            with open(ipaffs_schema_path, 'r') as f:
                schema_data = json.load(f)
            
            logger.info(f"Loaded IPAFFS schema with {len(schema_data.get('properties', {}))} properties")
            
            # Create temporary schema file for the agent
            schema_filename = f"temp_ipaffs_schema_{int(os.path.getmtime(pdf_filepath))}.json"
            schema_filepath = os.path.join(self.upload_folder, schema_filename)
            
            with open(schema_filepath, 'w') as f:
                json.dump(schema_data, f, indent=2)
            
            logger.info(f"Created temporary IPAFFS schema file: {schema_filepath}")
            
            # Initialize PDF extract agent
            pdf_agent = PDFExtractAgent(verbose=True)
            
            # Run the agent
            state = {
                'schema_path': schema_filepath,
                'pdf_path': pdf_filepath
            }
            result = pdf_agent.run(state)
            
            # Clean up temporary files
            try:
                if os.path.exists(pdf_filepath):
                    os.remove(pdf_filepath)
                if os.path.exists(schema_filepath):
                    os.remove(schema_filepath)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary files: {e}")
            
            if result.get('error'):
                return False, {"error": result['error']}
            
            # Get extracted data
            extracted_data = result.get('data', {})
            
            if not extracted_data:
                return False, {"error": "No data extracted from PDF"}
            
            # Convert to CSV format
            csv_data = convert_extracted_data_to_csv(extracted_data)
            
            # Clear the Variety column if it exists
            self._clear_variety_column(csv_data)
            
            # Generate column descriptions from schema
            column_descriptions = {}
            if 'properties' in schema_data:
                for field, props in schema_data['properties'].items():
                    description = props.get('description', f"IPAFFS data extracted from PDF for {field}")
                    data_type = props.get('type', 'string')
                    column_descriptions[field] = {
                        'description': description,
                        'data_type': data_type,
                        'sample_values': []
                    }
            
            logger.info(f"PDF extraction completed successfully. Extracted {len(csv_data['headers'])} fields")
            
            return True, {
                "extracted_data": extracted_data,
                "csv_data": csv_data,
                "column_descriptions": column_descriptions,
                "filename": filename
            }
            
        except Exception as e:
            logger.error(f"Error extracting PDF data: {e}", exc_info=True)
            
            # Clean up temporary files in case of error
            try:
                if 'pdf_filepath' in locals() and os.path.exists(pdf_filepath):
                    os.remove(pdf_filepath)
                if 'schema_filepath' in locals() and os.path.exists(schema_filepath):
                    os.remove(schema_filepath)
            except:
                pass
                
            return False, {"error": str(e)}
    
    def validate_pdf_file(self, pdf_content: bytes, filename: str) -> Tuple[bool, Optional[str]]:
        """
        Validate PDF file before processing.
        
        Args:
            pdf_content: PDF file content
            filename: Original filename
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check file extension
        if not filename.lower().endswith('.pdf'):
            return False, "File must be a PDF"
        
        # Check file size
        if len(pdf_content) > APIConfig.MAX_CONTENT_LENGTH:
            return False, f"File size exceeds {APIConfig.MAX_CONTENT_LENGTH / (1024*1024):.1f}MB limit"
        
        # Check if content is not empty
        if len(pdf_content) == 0:
            return False, "PDF file is empty"
        
        # Basic PDF signature check
        if not pdf_content.startswith(b'%PDF-'):
            return False, "File does not appear to be a valid PDF"
        
        return True, None
    
    def _clear_variety_column(self, csv_data: Dict[str, Any]) -> None:
        """
        Clear the Variety column from CSV data if it exists.
        
        Args:
            csv_data: CSV data dictionary with headers and rows
        """
        try:
            headers = csv_data.get('headers', [])
            rows = csv_data.get('rows', [])
            
            # Find the Variety column index (case-insensitive)
            variety_index = None
            for i, header in enumerate(headers):
                if header.lower() == 'variety':
                    variety_index = i
                    break
            
            if variety_index is not None:
                # Clear all values in the Variety column
                for row in rows:
                    if variety_index < len(row):
                        row[variety_index] = ''
                
                logger.info(f"Cleared Variety column at index {variety_index}")
            else:
                logger.info("No Variety column found to clear")
                
        except Exception as e:
            logger.warning(f"Error clearing Variety column: {e}")
    
    def get_ipaffs_schema(self) -> Dict[str, Any]:
        """
        Get the IPAFFS schema.
        
        Returns:
            IPAFFS schema as dictionary
        """
        try:
            ipaffs_schema_path = os.path.join(os.getcwd(), self.ipaffs_schema_path)
            if not os.path.exists(ipaffs_schema_path):
                raise FileNotFoundError("IPAFFS schema file not found")
            
            with open(ipaffs_schema_path, 'r') as f:
                schema_data = json.load(f)
            
            return schema_data
            
        except Exception as e:
            logger.error(f"Error loading IPAFFS schema: {e}")
            raise e
