import os
import json
import logging
import tempfile
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
import sys

# Add parent directory to path to import existing modules
sys.path.append('..')

# Import existing modules and agents
from eppo_lookup import EPPOLookup
from eppo_lookup_optimized import EPPOLookupOptimized
from utils.commodity_filter import get_commodity_filter
from agents.pdf_extract_agent import PDFExtractAgent
from agents.csv_edit_supervisor import CSVEditSupervisorAgent

# Import API-specific modules
from ipaffs_api.config.api_config import APIConfig
from ipaffs_api.utils.response_helpers import (
    ResponseFormatter, NpEncoder, convert_extracted_data_to_csv,
    convert_sample_data_to_csv
)
from ipaffs_api.utils.session_manager import InMemorySessionManager
from ipaffs_api.models.ipaffs_models import (
    CSVData, ExtractPDFResponse, CompatibilityCheckResponse,
    PrefillEPPOResponse, ChatResponse, ValidationResponse,
    ExportCSVResponse, HealthResponse, validate_ipaffs_headers
)
from ipaffs_api.services.pdf_service import PDFExtractionService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = APIConfig.SECRET_KEY
app.config['MAX_CONTENT_LENGTH'] = APIConfig.MAX_CONTENT_LENGTH
app.json_encoder = NpEncoder

# Enable CORS
CORS(app, origins=APIConfig.CORS_ORIGINS)

# Initialize session manager
session_manager = InMemorySessionManager(
    max_sessions=APIConfig.MAX_SESSIONS,
    session_timeout=APIConfig.SESSION_TIMEOUT
)

# Initialize services
pdf_service = PDFExtractionService()

# Initialize global EPPO lookup instance
eppo_lookup_instance = None

# Initialize global EPPO lookup instances
eppo_lookup_optimized_instance = None

def get_eppo_lookup():
    """Get the global EPPO lookup instance."""
    global eppo_lookup_instance
    if eppo_lookup_instance is None:
        try:
            eppo_lookup_instance = EPPOLookup(use_pool=True)
            logger.info("Global EPPO lookup instance initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize EPPO lookup instance: {e}")
            eppo_lookup_instance = EPPOLookup(use_pool=False)
            logger.info("EPPO lookup instance initialized without pooling (fallback)")
    return eppo_lookup_instance

def get_eppo_lookup_optimized():
    """Get the global optimized EPPO lookup instance."""
    global eppo_lookup_optimized_instance
    if eppo_lookup_optimized_instance is None:
        try:
            eppo_lookup_optimized_instance = EPPOLookupOptimized(use_pool=True)
            logger.info("Global optimized EPPO lookup instance initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize optimized EPPO lookup instance: {e}")
            logger.info("Falling back to standard EPPO lookup")
            return get_eppo_lookup()
    return eppo_lookup_optimized_instance

# Health check endpoint
@app.route('/api/v1/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        response = HealthResponse(
            status="healthy",
            timestamp=ResponseFormatter.success_response()["meta"]["timestamp"],
            version=APIConfig.API_VERSION,
            active_sessions=session_manager.get_session_count()
        )
        return jsonify(response.dict())
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return ResponseFormatter.error_response(
            "Service unavailable",
            status_code=503
        )

# Get IPAFFS schema endpoint
@app.route('/api/v1/ipaffs/schema', methods=['GET'])
def get_ipaffs_schema():
    """Get the IPAFFS schema."""
    try:
        schema = pdf_service.get_ipaffs_schema()
        response = ResponseFormatter.success_response(
            data={"schema": schema},
            message="IPAFFS schema retrieved successfully"
        )
        return jsonify(response)
    except Exception as e:
        logger.error(f"Error getting IPAFFS schema: {e}")
        return ResponseFormatter.error_response(
            f"Failed to retrieve IPAFFS schema: {str(e)}",
            status_code=500
        )

# PDF extraction endpoint
@app.route('/api/v1/ipaffs/extract-pdf', methods=['POST'])
def extract_pdf():
    """Extract data from PDF using IPAFFS schema."""
    try:
        # Check if PDF file is in request
        if 'pdf_file' not in request.files:
            return ResponseFormatter.error_response(
                "No PDF file provided",
                error_code="MISSING_FILE"
            )
        
        pdf_file = request.files['pdf_file']
        session_id = request.form.get('session_id')
        
        if pdf_file.filename == '':
            return ResponseFormatter.error_response(
                "No file selected",
                error_code="EMPTY_FILENAME"
            )
        
        # Read PDF content
        pdf_content = pdf_file.read()
        
        # Validate PDF file
        is_valid, error_msg = pdf_service.validate_pdf_file(pdf_content, pdf_file.filename)
        if not is_valid:
            return ResponseFormatter.error_response(
                error_msg,
                error_code="INVALID_FILE"
            )
        
        # Extract data from PDF
        success, result = pdf_service.extract_ipaffs_pdf(pdf_content, pdf_file.filename)
        
        if not success:
            return ResponseFormatter.error_response(
                result.get("error", "PDF extraction failed"),
                error_code="EXTRACTION_FAILED"
            )
        
        # Create or update session
        if not session_id:
            session_id = session_manager.create_session({
                "extracted_data": result["extracted_data"],
                "csv_data": result["csv_data"],
                "column_descriptions": result["column_descriptions"],
                "filename": result["filename"]
            })
        else:
            session_manager.update_session(session_id, {
                "extracted_data": result["extracted_data"],
                "csv_data": result["csv_data"],
                "column_descriptions": result["column_descriptions"],
                "filename": result["filename"]
            })
        
        # Sanitize CSV data for response
        csv_data = ResponseFormatter.sanitize_csv_data(result["csv_data"])
        
        response = ResponseFormatter.success_response(
            data={
                "extraction_id": session_id,
                "column_descriptions": result["column_descriptions"],
                "filename": result["filename"]
            },
            csv_data=csv_data,
            session_id=session_id,
            message="PDF data extracted successfully"
        )
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error extracting PDF: {e}", exc_info=True)
        return ResponseFormatter.error_response(
            f"PDF extraction failed: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

# IPAFFS compatibility check endpoint
@app.route('/api/v1/ipaffs/check-compatibility', methods=['POST'])
def check_compatibility():
    """Check IPAFFS compatibility of CSV data."""
    try:
        data = request.get_json()
        if not data:
            return ResponseFormatter.error_response(
                "No JSON data provided",
                error_code="MISSING_DATA"
            )
        
        csv_data = data.get('csv_data')
        session_id = data.get('session_id')
        
        if not csv_data:
            return ResponseFormatter.error_response(
                "No CSV data provided",
                error_code="MISSING_CSV_DATA"
            )
        
        # Validate CSV data structure
        if not isinstance(csv_data, dict) or 'headers' not in csv_data:
            return ResponseFormatter.error_response(
                "Invalid CSV data format",
                error_code="INVALID_CSV_FORMAT"
            )
        
        headers = csv_data.get('headers', [])
        
        # Check IPAFFS compatibility
        compatibility_result = validate_ipaffs_headers(headers)
        
        # Create or update session
        if not session_id:
            session_id = session_manager.create_session({
                "compatibility_check": compatibility_result,
                "csv_data": csv_data
            })
        else:
            session_manager.update_session(session_id, {
                "compatibility_check": compatibility_result,
                "csv_data": csv_data
            })
        
        # Sanitize CSV data for response
        sanitized_csv_data = ResponseFormatter.sanitize_csv_data(csv_data)
        
        response = ResponseFormatter.success_response(
            data={
                "compatible": compatibility_result["compatible"],
                "matched_headers": compatibility_result["matched_headers"],
                "missing_headers": compatibility_result["missing_headers"],
                "total_matched": compatibility_result["total_matched"],
                "total_required": compatibility_result["total_required"]
            },
            csv_data=sanitized_csv_data,
            session_id=session_id,
            message=f"Compatibility check completed. {'Compatible' if compatibility_result['compatible'] else 'Not compatible'} with IPAFFS requirements."
        )
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error checking compatibility: {e}", exc_info=True)
        return ResponseFormatter.error_response(
            f"Compatibility check failed: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

# EPPO pre-fill endpoint
@app.route('/api/v1/ipaffs/prefill-eppo', methods=['POST'])
def prefill_eppo():
    """Pre-fill IPAFFS data using EPPO database."""
    try:
        data = request.get_json()
        if not data:
            return ResponseFormatter.error_response(
                "No JSON data provided",
                error_code="MISSING_DATA"
            )
        
        csv_data = data.get('csv_data')
        session_id = data.get('session_id')
        
        if not csv_data:
            return ResponseFormatter.error_response(
                "No CSV data provided",
                error_code="MISSING_CSV_DATA"
            )
        
        # Initialize EPPO lookup and commodity filter
        lookup = get_eppo_lookup_optimized()
        commodity_filter = get_commodity_filter()
        
        headers = csv_data.get('headers', [])
        data_rows = csv_data.get('data', [])
        
        # Find genus and species column
        genus_species_col = None
        for header in headers:
            if 'genus' in header.lower() and 'species' in header.lower():
                genus_species_col = header
                break
        
        if not genus_species_col:
            return ResponseFormatter.error_response(
                "Genus and Species column not found in CSV data",
                error_code="MISSING_GENUS_SPECIES"
            )
        
        # Extract genus/species data
        genus_species_data = []
        for row in data_rows:
            genus_species_data.append(row.get(genus_species_col, ''))
        
        # Filter out empty entries for batch processing
        non_empty_entries = [(i, name) for i, name in enumerate(genus_species_data) if name and name.strip()]
        
        # Process batch IPAFFS lookup for non-empty entries
        eppo_codes = [''] * len(genus_species_data)
        commodity_options = [[] for _ in range(len(genus_species_data))]
        
        if non_empty_entries:
            try:
                # Extract just the names for batch lookup
                batch_names = [name for _, name in non_empty_entries]
                
                # Use optimized batch IPAFFS lookup
                batch_results = lookup.batch_ipaffs_lookup(batch_names)
                
                logger.info(f"Batch lookup processed {len(batch_names)} entries")
                
                # Process batch results
                for (original_index, genus_species), name in zip(non_empty_entries, batch_names):
                    if name in batch_results:
                        eppo_code, results = batch_results[name]
                        
                        if eppo_code:
                            eppo_codes[original_index] = eppo_code
                            
                            # Filter results for valid commodity codes
                            filtered_results = commodity_filter.filter_eppo_results(results)
                            
                            options = []
                            if filtered_results:
                                for commodity_name, eppo, commodity_code, description in filtered_results:
                                    options.append({
                                        'code': commodity_code,
                                        'description': description,
                                        'display': f"{commodity_code} - {description}"
                                    })
                            
                            commodity_options[original_index] = options
                            
                            logger.info(f"Found EPPO code '{eppo_code}' for '{genus_species}' with {len(options)} commodity options")
                        else:
                            logger.info(f"No EPPO code found for '{genus_species}'")
                    else:
                        logger.warning(f"No batch result found for '{genus_species}'")
                        
            except Exception as e:
                logger.error(f"Error in batch EPPO lookup: {e}")
                # Fallback to individual lookups if batch fails
                for i, genus_species in enumerate(genus_species_data):
                    if not genus_species or genus_species.strip() == '':
                        continue
                    
                    try:
                        eppo_code, results = lookup.enhanced_lookup_ipaffs(genus_species)
                        
                        if eppo_code:
                            eppo_codes[i] = eppo_code
                            
                            filtered_results = commodity_filter.filter_eppo_results(results)
                            
                            options = []
                            if filtered_results:
                                for commodity_name, eppo, commodity_code, description in filtered_results:
                                    options.append({
                                        'code': commodity_code,
                                        'description': description,
                                        'display': f"{commodity_code} - {description}"
                                    })
                            
                            commodity_options[i] = options
                            
                    except Exception as e:
                        logger.error(f"Error in individual EPPO lookup for '{genus_species}': {e}")
        
        # Update CSV data with EPPO codes
        updated_data_rows = []
        eppo_col = None
        
        # Find or create EPPO code column
        for header in headers:
            if 'eppo' in header.lower() and 'code' in header.lower():
                eppo_col = header
                break
        
        if not eppo_col:
            eppo_col = 'EPPO code'
            headers.append(eppo_col)
        
        # Update rows with EPPO codes
        for i, row in enumerate(data_rows):
            updated_row = row.copy()
            if i < len(eppo_codes) and eppo_codes[i]:
                # Only update if current value is empty
                current_value = updated_row.get(eppo_col, '')
                if not current_value or str(current_value).strip() == '':
                    updated_row[eppo_col] = eppo_codes[i]
            updated_data_rows.append(updated_row)
        
        # Create updated CSV data
        updated_csv_data = {
            "headers": headers,
            "data": updated_data_rows
        }
        
        # Create or update session
        if not session_id:
            session_id = session_manager.create_session({
                "csv_data": updated_csv_data,
                "eppo_codes": eppo_codes,
                "commodity_options": commodity_options
            })
        else:
            session_manager.update_session(session_id, {
                "csv_data": updated_csv_data,
                "eppo_codes": eppo_codes,
                "commodity_options": commodity_options
            })
        
        # Sanitize CSV data for response
        sanitized_csv_data = ResponseFormatter.sanitize_csv_data(updated_csv_data)
        
        response = ResponseFormatter.success_response(
            data={
                "eppo_codes_added": len([code for code in eppo_codes if code]),
                "commodity_options": commodity_options,
                "is_array_format": len(data_rows) > 1
            },
            csv_data=sanitized_csv_data,
            session_id=session_id,
            message=f"EPPO pre-fill completed. Added {len([code for code in eppo_codes if code])} EPPO codes."
        )
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in EPPO pre-fill: {e}", exc_info=True)
        return ResponseFormatter.error_response(
            f"EPPO pre-fill failed: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

# CSV data management endpoints
@app.route('/api/v1/ipaffs/csv-data/<session_id>', methods=['GET'])
def get_csv_data(session_id: str):
    """Get CSV data for a session."""
    try:
        session_data = session_manager.get_session(session_id)
        if not session_data:
            return ResponseFormatter.error_response(
                "Session not found or expired",
                error_code="SESSION_NOT_FOUND",
                status_code=404
            )
        
        csv_data = session_data.get('csv_data', {"headers": [], "data": []})
        sanitized_csv_data = ResponseFormatter.sanitize_csv_data(csv_data)
        
        response = ResponseFormatter.csv_response(
            csv_data=sanitized_csv_data,
            session_id=session_id,
            message="CSV data retrieved successfully"
        )
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error getting CSV data: {e}")
        return ResponseFormatter.error_response(
            f"Failed to retrieve CSV data: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

@app.route('/api/v1/ipaffs/csv-data/<session_id>', methods=['POST'])
def update_csv_data(session_id: str):
    """Update CSV data for a session."""
    try:
        data = request.get_json()
        if not data:
            return ResponseFormatter.error_response(
                "No JSON data provided",
                error_code="MISSING_DATA"
            )
        
        csv_data = data.get('csv_data')
        if not csv_data:
            return ResponseFormatter.error_response(
                "No CSV data provided",
                error_code="MISSING_CSV_DATA"
            )
        
        # Update session
        success = session_manager.update_session(session_id, {"csv_data": csv_data})
        if not success:
            return ResponseFormatter.error_response(
                "Session not found or expired",
                error_code="SESSION_NOT_FOUND",
                status_code=404
            )
        
        # Sanitize CSV data for response
        sanitized_csv_data = ResponseFormatter.sanitize_csv_data(csv_data)
        
        response = ResponseFormatter.csv_response(
            csv_data=sanitized_csv_data,
            session_id=session_id,
            message="CSV data updated successfully"
        )
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error updating CSV data: {e}")
        return ResponseFormatter.error_response(
            f"Failed to update CSV data: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

@app.route('/api/v1/ipaffs/csv-data/<session_id>', methods=['DELETE'])
def delete_csv_data(session_id: str):
    """Delete CSV data session."""
    try:
        success = session_manager.delete_session(session_id)
        if not success:
            return ResponseFormatter.error_response(
                "Session not found",
                error_code="SESSION_NOT_FOUND",
                status_code=404
            )
        
        response = ResponseFormatter.success_response(
            message="Session deleted successfully"
        )
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error deleting session: {e}")
        return ResponseFormatter.error_response(
            f"Failed to delete session: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

# AI Chat endpoint
@app.route('/api/v1/ipaffs/chat', methods=['POST'])
def chat_with_csv_editor():
    """Handle AI chat interactions for CSV editing."""
    try:
        data = request.get_json()
        if not data:
            return ResponseFormatter.error_response(
                "No JSON data provided",
                error_code="MISSING_DATA"
            )
        
        message = data.get('message')
        csv_data = data.get('csv_data')
        session_id = data.get('session_id')
        thread_id = data.get('thread_id')
        source_data = data.get('source_data', {})
        
        if not message or not csv_data:
            return ResponseFormatter.error_response(
                "Missing required parameters: message and csv_data",
                error_code="MISSING_PARAMETERS"
            )
        
        # Create temporary CSV file for the agent
        import tempfile
        import pandas as pd
        from langchain_core.messages import HumanMessage
        
        headers = csv_data.get('headers', [])
        data_rows = csv_data.get('data', [])
        
        # Create DataFrame from CSV data
        df = pd.DataFrame(data_rows)
        
        # Create temporary CSV file
        fd, csv_file_path = tempfile.mkstemp(prefix='temp_csv_', suffix='.csv')
        os.close(fd)
        
        df.to_csv(csv_file_path, index=False)
        
        # Initialize CSV Edit Supervisor Agent
        try:
            supervisor_agent = CSVEditSupervisorAgent(verbose=True)
        except Exception as e:
            logger.error(f"Error initializing CSV Edit Supervisor Agent: {e}")
            return ResponseFormatter.error_response(
                f"Failed to initialize AI supervisor agent: {str(e)}",
                error_code="AGENT_INIT_FAILED"
            )
        
        # Create message
        user_message = HumanMessage(content=message)
        
        # Prepare state
        if thread_id:
            # Resume conversation
            state = {
                'messages': [user_message],
                'csv_file_path': csv_file_path,
                'source_data': source_data,
                'thread_id': thread_id,
                'original_request': data.get('original_request', ''),
                'rewritten_request': data.get('rewritten_request', ''),
                'in_clarification_mode': data.get('in_clarification_mode', False),
                'is_request_clarified': data.get('is_request_clarified', False),
                'clarification_count': data.get('clarification_count', 0),
                'last_active_node': data.get('last_active_node', '')
            }
            result = supervisor_agent.resume(state, message)
        else:
            # New conversation
            state = {
                'messages': [user_message],
                'csv_file_path': csv_file_path,
                'source_data': source_data,
                'original_request': message
            }
            result = supervisor_agent.run(state)
        
        # Check if human input is needed
        interrupt_message = result.get('interrupt_message')
        needs_input = result.get('needs_input', False)
        
        if needs_input and interrupt_message:
            # Return interruption response
            response = ResponseFormatter.success_response(
                data={
                    'needs_input': True,
                    'interrupt_message': interrupt_message,
                    'thread_id': result.get('thread_id'),
                    'original_request': result.get('original_request', ''),
                    'rewritten_request': result.get('rewritten_request', ''),
                    'in_clarification_mode': result.get('in_clarification_mode', False),
                    'is_request_clarified': result.get('is_request_clarified', False),
                    'clarification_count': result.get('clarification_count', 0),
                    'last_active_node': result.get('last_active_node', '')
                },
                csv_data=ResponseFormatter.sanitize_csv_data(csv_data),
                session_id=session_id,
                message="Human input required"
            )
            return jsonify(response)
        
        # Process the response
        from langchain_core.messages import AIMessage
        
        # Extract agent responses
        agent_responses = []
        for msg in result.get('messages', []):
            if isinstance(msg, HumanMessage) and hasattr(msg, 'name'):
                if msg.name in ['csv_edit', 'supervisor', 'csv_verifier']:
                    agent_responses.append(msg.content)
        
        ai_response = "\n\n".join(agent_responses) if agent_responses else "The CSV editor processed your request."
        
        # Check if CSV was modified
        csv_data_changed = False
        new_csv_data = csv_data
        
        try:
            if os.path.exists(csv_file_path):
                modified_df = pd.read_csv(csv_file_path)
                new_csv_data = {
                    'headers': list(modified_df.columns),
                    'data': modified_df.to_dict('records')
                }
                
                # Check if data changed
                csv_data_changed = (
                    len(new_csv_data['headers']) != len(csv_data.get('headers', [])) or
                    len(new_csv_data['data']) != len(csv_data.get('data', [])) or
                    str(new_csv_data) != str(csv_data)
                )
        except Exception as e:
            logger.error(f"Error reading modified CSV: {e}")
        
        # Clean up temporary file
        try:
            if os.path.exists(csv_file_path):
                os.remove(csv_file_path)
        except Exception as e:
            logger.warning(f"Failed to remove temporary CSV file: {e}")
        
        # Update session if provided
        if session_id and csv_data_changed:
            session_manager.update_session(session_id, {"csv_data": new_csv_data})
        
        # Sanitize CSV data for response
        sanitized_csv_data = ResponseFormatter.sanitize_csv_data(new_csv_data)
        
        response = ResponseFormatter.success_response(
            data={
                'response': ai_response,
                'csv_data_changed': csv_data_changed,
                'needs_input': False,
                'thread_id': result.get('thread_id')
            },
            csv_data=sanitized_csv_data,
            session_id=session_id,
            message="AI response generated successfully"
        )
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in CSV editing chat: {e}", exc_info=True)
        return ResponseFormatter.error_response(
            f"Chat processing failed: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

# Commodity selection endpoints
@app.route('/api/v1/ipaffs/commodity-selections', methods=['POST'])
def update_commodity_selections():
    """Update commodity code selections."""
    try:
        data = request.get_json()
        if not data:
            return ResponseFormatter.error_response(
                "No JSON data provided",
                error_code="MISSING_DATA"
            )
        
        selections = data.get('selections', [])
        session_id = data.get('session_id')
        
        if not selections:
            return ResponseFormatter.error_response(
                "No selections provided",
                error_code="MISSING_SELECTIONS"
            )
        
        # Update session with commodity selections
        if session_id:
            session_data = session_manager.get_session(session_id)
            if session_data:
                commodity_selections = session_data.get('commodity_selections', {})
                
                updated_count = 0
                for selection in selections:
                    row_index = selection.get('row_index')
                    commodity_code = selection.get('commodity_code')
                    display_text = selection.get('display_text', '')
                    
                    if row_index is not None:
                        commodity_selections[str(row_index)] = {
                            'code': commodity_code,
                            'display_text': display_text
                        }
                        updated_count += 1
                
                session_manager.update_session(session_id, {
                    'commodity_selections': commodity_selections
                })
                
                response = ResponseFormatter.success_response(
                    data={'updated_count': updated_count},
                    session_id=session_id,
                    message=f"Successfully updated {updated_count} commodity selections"
                )
                
                return jsonify(response)
        
        return ResponseFormatter.error_response(
            "Session not found",
            error_code="SESSION_NOT_FOUND",
            status_code=404
        )
        
    except Exception as e:
        logger.error(f"Error updating commodity selections: {e}")
        return ResponseFormatter.error_response(
            f"Failed to update commodity selections: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

# Export CSV endpoint
@app.route('/api/v1/ipaffs/export-csv', methods=['POST'])
def export_csv():
    """Export CSV data."""
    try:
        data = request.get_json()
        if not data:
            return ResponseFormatter.error_response(
                "No JSON data provided",
                error_code="MISSING_DATA"
            )
        
        csv_data = data.get('csv_data')
        session_id = data.get('session_id')
        export_format = data.get('export_format', 'csv')
        
        if not csv_data:
            return ResponseFormatter.error_response(
                "No CSV data provided",
                error_code="MISSING_CSV_DATA"
            )
        
        # Generate CSV content
        from io import StringIO
        import csv
        
        output = StringIO()
        writer = csv.writer(output)
        
        headers = csv_data.get('headers', [])
        data_rows = csv_data.get('data', [])
        
        # Write headers
        writer.writerow(headers)
        
        # Write data rows
        for row in data_rows:
            if isinstance(row, dict):
                row_data = [row.get(header, '') for header in headers]
                writer.writerow(row_data)
            else:
                writer.writerow(row)
        
        csv_content = output.getvalue()
        output.close()
        
        # Generate filename
        filename = f"ipaffs_export_{session_id[:8] if session_id else 'data'}.csv"
        
        response = ResponseFormatter.success_response(
            data={
                "csv_content": csv_content,
                "filename": filename,
                "export_format": export_format
            },
            csv_data=ResponseFormatter.sanitize_csv_data(csv_data),
            session_id=session_id,
            message="CSV export completed successfully"
        )
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        return ResponseFormatter.error_response(
            f"CSV export failed: {str(e)}",
            error_code="INTERNAL_ERROR",
            status_code=500
        )

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return ResponseFormatter.error_response(
        "Endpoint not found",
        error_code="NOT_FOUND",
        status_code=404
    )

@app.errorhandler(405)
def method_not_allowed(error):
    return ResponseFormatter.error_response(
        "Method not allowed",
        error_code="METHOD_NOT_ALLOWED",
        status_code=405
    )

@app.errorhandler(413)
def too_large(error):
    return ResponseFormatter.error_response(
        "File too large",
        error_code="FILE_TOO_LARGE",
        status_code=413
    )

@app.errorhandler(500)
def internal_error(error):
    return ResponseFormatter.error_response(
        "Internal server error",
        error_code="INTERNAL_ERROR",
        status_code=500
    )

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))  # Use different port than main app
    app.run(host='0.0.0.0', port=port, debug=True)
