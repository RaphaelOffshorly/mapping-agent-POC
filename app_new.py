import os
import json
import logging
import subprocess
import time
import uuid
import numpy as np
from langchain_core.messages import AIMessage
from agents.csv_edit_agent import CSVEditAgent
import tempfile
import os
from langchain_core.messages import HumanMessage
from flask import Flask, render_template, request, jsonify, session, send_file
from eppo_lookup import EPPOLookup
from utils.commodity_filter import get_commodity_filter
from werkzeug.utils import secure_filename
import tempfile
from dotenv import load_dotenv
import pandas as pd

# Import schema builder module
import schema_builder

# Custom JSON encoder to handle NaN values
class NpEncoder(json.JSONEncoder):
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

from config.config import Config
from utils.excel import get_excel_preview
from workflow import run_workflow
from agents.pdf_extract_agent import PDFExtractAgent

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.secret_key = Config.SECRET_KEY
app.config['UPLOAD_FOLDER'] = Config.UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = Config.MAX_CONTENT_LENGTH
app.config['SESSION_TYPE'] = Config.SESSION_TYPE
app.config['SESSION_FILE_DIR'] = Config.SESSION_FILE_DIR
app.config['SESSION_PERMANENT'] = Config.SESSION_PERMANENT
app.config['PERMANENT_SESSION_LIFETIME'] = Config.PERMANENT_SESSION_LIFETIME

# Create session directory if it doesn't exist
os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)

# Initialize session extension
from flask_session import Session
Session(app)

# Initialize global EPPO lookup instance (with connection pooling for better performance)
# This will be shared across all requests to avoid creating new connections each time
eppo_lookup_instance = None

def get_eppo_lookup():
    """Get the global EPPO lookup instance, creating it if needed."""
    global eppo_lookup_instance
    if eppo_lookup_instance is None:
        try:
            eppo_lookup_instance = EPPOLookup(use_pool=True)
            logger.info("Global EPPO lookup instance initialized successfully with connection pooling")
        except Exception as e:
            logger.error(f"Failed to initialize EPPO lookup instance: {e}")
            # Create without pooling as fallback
            eppo_lookup_instance = EPPOLookup(use_pool=False)
            logger.info("EPPO lookup instance initialized without connection pooling (fallback)")
    return eppo_lookup_instance

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/pdf_upload')
def pdf_upload():
    """Route for the PDF upload page"""
    return render_template('pdf_upload.html')

@app.route('/ipaffs_upload')
def ipaffs_upload():
    """Route for the IPAFFS upload page"""
    return render_template('ipaffs_upload.html')

@app.route('/generate_schema', methods=['POST'])
def generate_schema():
    """API endpoint to generate a schema from an Excel file"""
    if 'excel_file' not in request.files:
        return jsonify({'error': 'No Excel file provided'})

    excel_file = request.files['excel_file']
    excel_sheet_name = request.form.get('excel_sheet_name', '')

    if excel_file.filename == '':
        return jsonify({'error': 'No selected file'})

    if not excel_file.filename.endswith(('.xlsx', '.xls', '.csv')):
        return jsonify({'error': 'File must be an Excel or CSV file'})

    try:
        # Save the file temporarily
        excel_filename = secure_filename(excel_file.filename)
        excel_filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_schema_{excel_filename}")
        excel_file.save(excel_filepath)

        # Create a unique schema file path
        schema_filename = f"schema_{int(os.path.getmtime(excel_filepath))}.json"
        schema_filepath = os.path.join(app.config['UPLOAD_FOLDER'], schema_filename)

        # Run the excel_schema_generator script
        cmd = ['python', 'excel_schema_generator.py', excel_filepath]
        if excel_sheet_name:
            cmd.extend(['--sheet', excel_sheet_name])
        cmd.extend(['--output', schema_filepath])

        logger.info(f"Running schema generator: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"Schema generation failed: {result.stderr}")
            return jsonify({'error': f'Schema generation failed: {result.stderr}'})

        # Read the generated schema
        with open(schema_filepath, 'r') as f:
            schema = json.load(f)

        # Store the schema path in session
        session['schema_filepath'] = schema_filepath
        session['excel_filepath'] = excel_filepath

        # Return the schema
        return jsonify({
            'success': True,
            'schema': schema
        })

    except Exception as e:
        logger.error(f"Error generating schema: {e}")
        return jsonify({'error': str(e)})

@app.route('/save_temp_schema', methods=['POST'])
def save_temp_schema():
    """API endpoint to save a temporary edited schema to the session"""
    try:
        data = request.json
        schema = data.get('schema')

        if not schema:
            return jsonify({'error': 'No schema provided'})

        # Get the schema path from session
        schema_filepath = session.get('schema_filepath')
        if not schema_filepath:
            # Create a new schema file if none exists
            schema_filename = f"schema_{int(time.time())}.json"
            schema_filepath = os.path.join(app.config['UPLOAD_FOLDER'], schema_filename)
            session['schema_filepath'] = schema_filepath

        # Save the schema
        with open(schema_filepath, 'w') as f:
            json.dump(schema, f, indent=2)

        return jsonify({
            'success': True,
            'message': 'Schema saved successfully'
        })

    except Exception as e:
        logger.error(f"Error saving schema: {e}")
        return jsonify({'error': str(e)})

@app.route('/extract_pdf', methods=['POST'])
def extract_pdf():
    """API endpoint to extract data from a PDF file using the saved schema"""
    if 'pdf_file' not in request.files:
        return jsonify({'error': 'No PDF file provided'})

    pdf_file = request.files['pdf_file']

    if pdf_file.filename == '':
        return jsonify({'error': 'No selected file'})

    if not pdf_file.filename.endswith('.pdf'):
        return jsonify({'error': 'File must be a PDF'})

    try:
        # Check for schema data in the request first (new flow), then fall back to session (old flow)
        schema_data = None
        schema_filepath = None
        
        # New flow: Check if schema is provided directly in the request
        if 'schema' in request.form:
            try:
                schema_data = json.loads(request.form.get('schema'))
                logger.info(f"Using schema from request form with {len(schema_data.get('properties', {}))} properties")
                
                # Create a temporary schema file for the agent
                schema_filename = f"temp_schema_{int(time.time())}.json"
                schema_filepath = os.path.join(app.config['UPLOAD_FOLDER'], schema_filename)
                
                with open(schema_filepath, 'w') as f:
                    json.dump(schema_data, f, indent=2)
                    
                logger.info(f"Created temporary schema file: {schema_filepath}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing schema JSON: {e}")
                return jsonify({'error': 'Invalid schema format'})
        else:
            # Old flow: Get the schema path from session
            schema_filepath = session.get('schema_filepath')
            if not schema_filepath or not os.path.exists(schema_filepath):
                return jsonify({'error': 'No schema found. Please generate a schema first.'})
            
            # Load schema data from file
            with open(schema_filepath, 'r') as f:
                schema_obj = json.load(f)
            
            # Extract just the schema part if it's wrapped with metadata
            if 'schema' in schema_obj:
                schema_data = schema_obj['schema']
                logger.info(f"Extracted schema from wrapper object")
            else:
                schema_data = schema_obj
                logger.info(f"Using schema data directly (no wrapper)")
            
            logger.info(f"Using schema from session with {len(schema_data.get('properties', {}))} properties")

        # Save the PDF file temporarily
        pdf_filename = secure_filename(pdf_file.filename)
        pdf_filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{pdf_filename}")
        pdf_file.save(pdf_filepath)

        # Initialize the PDF extract agent
        pdf_agent = PDFExtractAgent(verbose=True)

        # Run the agent
        state = {
            'schema_path': schema_filepath,
            'pdf_path': pdf_filepath
        }
        result = pdf_agent.run(state)

        if result.get('error'):
            return jsonify({'error': result['error']})

        # Get the extracted data
        extracted_data = result.get('data', {})

        # Store the data in session
        session['filename'] = pdf_filename
        session['pdf_filepath'] = pdf_filepath
        session['extracted_data'] = extracted_data
        session['schema_filepath'] = schema_filepath
        
        # Generate column descriptions from schema
        column_descriptions = {}
        if 'properties' in schema_data:
            for field, props in schema_data['properties'].items():
                description = props.get('description', f"Data extracted from PDF for {field}")
                data_type = props.get('type', 'string')
                column_descriptions[field] = {
                    'description': description,
                    'data_type': data_type,
                    'sample_values': []
                }
        
        # Store the column descriptions in session
        session['column_descriptions'] = column_descriptions

        return jsonify({
            'success': True,
            'redirect': '/pdf_results'
        })

    except Exception as e:
        logger.error(f"Error extracting PDF data: {e}")
        return jsonify({'error': str(e)})

@app.route('/extract_ipaffs_pdf', methods=['POST'])
def extract_ipaffs_pdf():
    """API endpoint to extract data from a PDF file using the IPAFFS schema"""
    if 'pdf_file' not in request.files:
        return jsonify({'error': 'No PDF file provided'})

    pdf_file = request.files['pdf_file']

    if pdf_file.filename == '':
        return jsonify({'error': 'No selected file'})

    if not pdf_file.filename.endswith('.pdf'):
        return jsonify({'error': 'File must be a PDF'})

    try:
        # Load the IPAFFS schema automatically
        ipaffs_schema_path = os.path.join(os.getcwd(), 'ipaffs_schema.json')
        if not os.path.exists(ipaffs_schema_path):
            return jsonify({'error': 'IPAFFS schema file not found'})

        # Load the schema data
        with open(ipaffs_schema_path, 'r') as f:
            schema_data = json.load(f)
        
        logger.info(f"Loaded IPAFFS schema with {len(schema_data.get('properties', {}))} properties")

        # Create a temporary schema file for the agent
        schema_filename = f"temp_ipaffs_schema_{int(time.time())}.json"
        schema_filepath = os.path.join(app.config['UPLOAD_FOLDER'], schema_filename)
        
        with open(schema_filepath, 'w') as f:
            json.dump(schema_data, f, indent=2)
            
        logger.info(f"Created temporary IPAFFS schema file: {schema_filepath}")

        # Save the PDF file temporarily
        pdf_filename = secure_filename(pdf_file.filename)
        pdf_filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_ipaffs_{pdf_filename}")
        pdf_file.save(pdf_filepath)

        # Initialize the PDF extract agent
        pdf_agent = PDFExtractAgent(verbose=True)

        # Run the agent
        state = {
            'schema_path': schema_filepath,
            'pdf_path': pdf_filepath
        }
        result = pdf_agent.run(state)

        if result.get('error'):
            return jsonify({'error': result['error']})

        # Get the extracted data
        extracted_data = result.get('data', {})

        # Store the data in session
        session['filename'] = pdf_filename
        session['pdf_filepath'] = pdf_filepath
        session['extracted_data'] = extracted_data
        session['schema_filepath'] = schema_filepath
        
        # Generate column descriptions from IPAFFS schema
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
        
        # Store the column descriptions in session
        session['column_descriptions'] = column_descriptions

        logger.info(f"IPAFFS PDF extraction completed successfully for {pdf_filename}")

        return jsonify({
            'success': True,
            'redirect': '/pdf_results'
        })

    except Exception as e:
        logger.error(f"Error extracting IPAFFS PDF data: {e}")
        return jsonify({'error': str(e)})

@app.route('/get_sheets', methods=['POST'])
def get_sheets():
    """API endpoint to get the sheet names from an Excel file"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'File must be an Excel file (.xlsx or .xls)'})

    try:
        # Save the file temporarily
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_sheets_{filename}")
        file.save(filepath)

        # Get the sheet names
        excel_file = pd.ExcelFile(filepath)
        sheet_names = excel_file.sheet_names

        # Clean up
        os.remove(filepath)

        return jsonify({'sheets': sheet_names})
    except Exception as e:
        logger.error(f"Error getting sheet names: {e}")
        return jsonify({'error': str(e)})

@app.route('/get_target_columns', methods=['POST'])
def get_target_columns():
    """API endpoint to get the target columns from an Excel file"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']
    sheet_name = request.form.get('sheet', '')

    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'File must be an Excel file (.xlsx or .xls)'})

    try:
        # Save the file temporarily
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_target_{filename}")
        file.save(filepath)

        # Read the Excel file
        if sheet_name:
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=None)
        else:
            # Use the first sheet if none specified
            excel_file = pd.ExcelFile(filepath)
            if not excel_file.sheet_names:
                # Clean up
                os.remove(filepath)
                return jsonify({'error': 'No sheets found in the Excel file'})
            
            sheet_name = excel_file.sheet_names[0]
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=None)

        # Check if DataFrame is empty
        if df.empty or len(df) == 0:
            # Clean up
            os.remove(filepath)
            return jsonify({'error': 'The selected sheet is empty or contains no data'})

        # Try to find the header row
        from utils.common import infer_header_row
        header_index = infer_header_row(df)
        if header_index is not None and header_index < len(df):
            # Extract headers from the inferred header row
            headers = df.iloc[header_index].astype(str).tolist()
            target_columns = [h.strip() for h in headers if h.strip()]
        else:
            # If no header row found, use the first row
            if len(df) > 0:
                headers = df.iloc[0].astype(str).tolist()
                target_columns = [h.strip() for h in headers if h.strip()]
            else:
                # Clean up
                os.remove(filepath)
                return jsonify({'error': 'No data found in the sheet'})

        # Clean up
        os.remove(filepath)

        # Ensure we have valid target columns
        if not target_columns:
            return jsonify({'error': 'No valid column headers found in the sheet'})

        return jsonify({'target_columns': target_columns})
    except Exception as e:
        logger.error(f"Error getting target columns: {e}")
        # Clean up if file exists
        try:
            if 'filepath' in locals() and os.path.exists(filepath):
                os.remove(filepath)
        except:
            pass
        return jsonify({'error': str(e)})

@app.route('/create_schema', methods=['POST'])
def create_schema():
    """API endpoint to generate initial schema for target columns"""
    try:
        # Target columns can come from form or be extracted from target file
        target_columns_list = []
        target_file_path = None
        
        logger.info(f"create_schema called with form keys: {list(request.form.keys())}")
        logger.info(f"create_schema called with files keys: {list(request.files.keys())}")
        
        # Get target columns from form if provided
        if 'target_columns' in request.form:
            target_columns = request.form.get('target_columns', '')
            target_columns_list = [col.strip() for col in target_columns.split(',') if col.strip()]
            logger.info(f"Target columns from form: {target_columns_list}")
        
        # If target file is provided, use it for schema generation
        if 'target_file' in request.files:
            target_file = request.files['target_file']
            target_sheet_name = request.form.get('target_sheet_name', '')
            
            logger.info(f"Target file provided: {target_file.filename}, sheet: {target_sheet_name}")
            
            # Save target file temporarily
            target_filename = secure_filename(target_file.filename)
            target_file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_target_{target_filename}")
            target_file.save(target_file_path)
            logger.info(f"Target file saved to: {target_file_path}")
            
            try:
                # Extract target columns from file if we don't have them already
                if not target_columns_list:
                    from utils.common import infer_header_row
                    if target_sheet_name:
                        target_df = pd.read_excel(target_file_path, sheet_name=target_sheet_name, header=None)
                    else:
                        # Use the first sheet if none specified
                        target_excel_file = pd.ExcelFile(target_file_path)
                        target_sheet_name = target_excel_file.sheet_names[0]
                        target_df = pd.read_excel(target_file_path, sheet_name=target_sheet_name, header=None)
                    
                    # Find header row and extract column names
                    header_index = infer_header_row(target_df)
                    if header_index is not None:
                        headers = target_df.iloc[header_index].astype(str).tolist()
                    else:
                        headers = target_df.iloc[0].astype(str).tolist()
                    
                    target_columns_list = [h.strip() for h in headers if h.strip()]
                    logger.info(f"Extracted target columns from file: {target_columns_list}")
            except Exception as e:
                logger.error(f"Error extracting target columns from file: {e}")
                # Clean up
                if os.path.exists(target_file_path):
                    os.remove(target_file_path)
                return jsonify({'error': f'Error extracting target columns: {str(e)}'})
        
        if not target_columns_list:
            return jsonify({'error': 'No target columns provided'})
        
        try:
            logger.info(f"Generating schema with {len(target_columns_list)} columns, using file: {target_file_path}")
            
            # Get existing schema from session if available to preserve explicit type specifications
            existing_schema = session.get('temp_schema') or session.get('schema')
            if existing_schema:
                logger.info(f"Found existing schema with {len(existing_schema.get('properties', {}))} properties, will preserve explicit types")
            
            # Generate schema from target file if available, otherwise just use columns
            schema = schema_builder.create_initial_schema(
                target_columns_list,
                target_file_path,  # Use the target file path, not the source file
                request.form.get('target_sheet_name', ''),  # Use target sheet name
                existing_schema  # Pass existing schema to preserve explicit types
            )
            
            # Log the schema
            logger.info(f"Generated schema with {len(schema.get('properties', {}))} properties")
            
            # Store in session in both keys to ensure consistency
            session['temp_schema'] = schema
            session['schema'] = schema  # Store in both keys
            
            # Log schema creation
            logger.info(f"SCHEMA DEBUG: Generated schema with {len(schema.get('properties', {}))} properties")
            logger.info(f"SCHEMA DEBUG: Schema properties: {list(schema.get('properties', {}).keys())}")
            
            # Clean up if needed
            if target_file_path and os.path.exists(target_file_path):
                os.remove(target_file_path)
                logger.info(f"Removed temporary target file: {target_file_path}")
            
            return jsonify({
                'success': True,
                'schema': schema
            })
            
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            # Clean up
            if target_file_path and os.path.exists(target_file_path):
                os.remove(target_file_path)
            return jsonify({'error': str(e)})
    
    except Exception as e:
        logger.error(f"Error in create_schema: {e}")
        return jsonify({'error': str(e)})

@app.route('/save_schema', methods=['POST'])
def save_schema_endpoint():
    """API endpoint to save a schema with a name"""
    try:
        data = request.json
        schema_data = data.get('schema')
        schema_name = data.get('name', 'Unnamed Schema')
        
        logger.info(f"Saving schema with name: {schema_name}")
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"Schema directory: {os.path.abspath(schema_builder.SCHEMA_DIR)}")
        
        # Check if schema directory exists and is writable
        if not os.path.exists(schema_builder.SCHEMA_DIR):
            logger.warning(f"Schema directory does not exist, creating: {schema_builder.SCHEMA_DIR}")
            try:
                os.makedirs(schema_builder.SCHEMA_DIR, exist_ok=True)
                logger.info(f"Created schema directory: {schema_builder.SCHEMA_DIR}")
            except Exception as dir_err:
                logger.error(f"Failed to create schema directory: {dir_err}", exc_info=True)
                return jsonify({'error': f'Failed to create schema directory: {str(dir_err)}'})
        else:
            # Check if directory is writable
            if not os.access(schema_builder.SCHEMA_DIR, os.W_OK):
                logger.error(f"Schema directory exists but is not writable: {schema_builder.SCHEMA_DIR}")
                # Try to fix permissions
                try:
                    os.chmod(schema_builder.SCHEMA_DIR, 0o777)  # Full permissions
                    logger.info(f"Set permissions on schema directory to 777")
                except Exception as perm_err:
                    logger.error(f"Failed to set permissions: {perm_err}", exc_info=True)
                    return jsonify({'error': f'Schema directory is not writable: {str(perm_err)}'})
        
        if not schema_data:
            logger.error("No schema data provided in save_schema_endpoint")
            return jsonify({'error': 'No schema data provided'})
        
        # Log what we received for debugging
        logger.info(f"Received schema data type: {type(schema_data)}")
        if isinstance(schema_data, dict):
            logger.info(f"Schema data keys: {list(schema_data.keys())}")
            if 'properties' in schema_data:
                logger.info(f"Schema has {len(schema_data['properties'])} properties")
            else:
                logger.warning("Schema data does not have 'properties' key")
        
        # The schema_builder.save_schema function now handles unwrapping if needed
        # But let's ensure we're passing the right data
        if isinstance(schema_data, dict) and 'schema' in schema_data:
            logger.info("Schema data appears to be wrapped, using wrapped data as-is")
            # The save_schema function will extract it properly
        elif isinstance(schema_data, dict) and 'properties' in schema_data:
            logger.info("Schema data appears to be unwrapped schema")
        else:
            logger.warning(f"Unexpected schema data format: {schema_data}")
        
        # Save schema - the save_schema function will handle extraction/validation
        logger.info(f"Calling schema_builder.save_schema with schema data")
        result = schema_builder.save_schema(schema_data, schema_name)
        
        # Log the result
        if result.get('success'):
            logger.info(f"Schema saved successfully with ID: {result.get('id')}")
            
            # Verify the file actually exists
            expected_file_path = os.path.join(schema_builder.SCHEMA_DIR, f"{result.get('id')}.json")
            if os.path.exists(expected_file_path):
                file_size = os.path.getsize(expected_file_path)
                logger.info(f"Verified schema file exists: {expected_file_path} ({file_size} bytes)")
                
                # Also verify the content is correct by reading it back
                try:
                    with open(expected_file_path, 'r') as f:
                        saved_content = json.load(f)
                    
                    if 'schema' in saved_content and 'properties' in saved_content['schema']:
                        num_properties = len(saved_content['schema']['properties'])
                        logger.info(f"Verified saved schema has {num_properties} properties")
                    else:
                        logger.warning("Saved schema file has unexpected format")
                except Exception as read_err:
                    logger.error(f"Error reading back saved schema: {read_err}")
            else:
                logger.error(f"Schema file does not exist after save: {expected_file_path}")
                return jsonify({
                    'error': f'Schema file not found after save. This may be a permission issue.',
                    'details': {
                        'expected_path': expected_file_path,
                        'schema_dir': schema_builder.SCHEMA_DIR,
                        'schema_id': result.get('id')
                    }
                })
        else:
            logger.error(f"Schema save failed: {result.get('error')}")
        
        # List schemas after save to verify the schema appears in the list
        try:
            schemas = schema_builder.list_saved_schemas()
            logger.info(f"After save: found {len(schemas)} schemas in directory")
            for i, schema in enumerate(schemas):
                logger.info(f"  Schema {i+1}: ID={schema.get('id')}, Name={schema.get('name')}")
        except Exception as list_err:
            logger.error(f"Error listing schemas after save: {list_err}", exc_info=True)
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error saving schema: {e}", exc_info=True)
        return jsonify({'error': str(e)})

@app.route('/list_schemas', methods=['GET'])
def list_schemas():
    """API endpoint to list all saved schemas with optional mode filtering"""
    try:
        # Get the mode parameter from query string (pdf or excel)
        mode = request.args.get('mode', '').lower()
        logger.info(f"Listing saved schemas for mode: {mode}")
        
        # Let's manually check the schemas directory to help with debugging
        schema_dir = os.path.abspath(schema_builder.SCHEMA_DIR)
        logger.info(f"Using schema directory absolute path: {schema_dir}")
        
        # Check if directory exists and is accessible
        if not os.path.exists(schema_dir):
            logger.error(f"Schema directory does not exist: {schema_dir}")
            return jsonify({
                'success': False,
                'error': f'Schema directory does not exist: {schema_dir}'
            })
        
        # Check if directory is readable
        if not os.access(schema_dir, os.R_OK):
            logger.error(f"Schema directory is not readable: {schema_dir}")
            return jsonify({
                'success': False,
                'error': f'Schema directory is not readable: {schema_dir}'
            })
        
        # List directory contents and apply filtering
        try:
            dir_contents = os.listdir(schema_dir)
            json_files = [f for f in dir_contents if f.endswith('.json')]
            logger.info(f"Directory listing: Found {len(dir_contents)} items in schema directory, {len(json_files)} JSON files")
            
            # Load and filter schemas based on mode
            filtered_schemas = []
            for filename in json_files:
                file_path = os.path.join(schema_dir, filename)
                try:
                    if os.path.isfile(file_path):
                        with open(file_path, 'r') as f:
                            schema_obj = json.load(f)
                        
                        # Extract metadata
                        schema_id = schema_obj.get("id", os.path.splitext(filename)[0])
                        schema_name = schema_obj.get("name", "Unnamed schema")
                        timestamp = schema_obj.get("timestamp", "")
                        is_array_schema = schema_obj.get("is_array_of_objects", False)
                        
                        # Apply mode-based filtering
                        should_include = True
                        if mode == 'excel':
                            # Excel mode: exclude array of objects schemas
                            should_include = not is_array_schema
                        elif mode == 'pdf':
                            # PDF mode: include all schemas (both regular and array)
                            should_include = True
                        # If no mode specified, include all schemas
                        
                        if should_include:
                            schema_info = {
                                "id": schema_id,
                                "name": schema_name,
                                "timestamp": timestamp,
                                "is_array_of_objects": is_array_schema
                            }
                            
                            # Add array config if available
                            if is_array_schema and "array_config" in schema_obj:
                                schema_info["array_config"] = schema_obj["array_config"]
                            
                            filtered_schemas.append(schema_info)
                            logger.info(f"Included schema: ID={schema_id}, Name={schema_name}, Array={is_array_schema}")
                        else:
                            logger.info(f"Filtered out schema: ID={schema_id}, Name={schema_name}, Array={is_array_schema} (mode={mode})")
                            
                except Exception as e:
                    logger.error(f"Error reading schema file {filename}: {e}", exc_info=True)
            
            # Sort by timestamp (newest first)
            filtered_schemas.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            logger.info(f"Returning {len(filtered_schemas)} filtered schemas for mode '{mode}'")
            
            return jsonify({
                'success': True,
                'schemas': filtered_schemas
            })
            
        except Exception as e:
            logger.error(f"Error listing directory contents: {e}", exc_info=True)
            return jsonify({
                'success': False,
                'error': f'Error listing directory contents: {str(e)}'
            })
    
    except Exception as e:
        logger.error(f"Error listing schemas: {e}", exc_info=True)
        return jsonify({'error': str(e)})

@app.route('/load_schema/<schema_id>', methods=['GET'])
def load_schema(schema_id):
    """API endpoint to load a specific schema"""
    try:
        result = schema_builder.load_schema(schema_id)
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error loading schema: {e}")
        return jsonify({'error': str(e)})

@app.route('/delete_schema/<schema_id>', methods=['DELETE'])
def delete_schema(schema_id):
    """API endpoint to delete a schema"""
    try:
        logger.info(f"Delete schema request received for schema_id: {schema_id}")
        
        # Clean the schema ID to prevent issues
        schema_id = schema_id.strip()
        
        # First check if the schema exists
        file_path = os.path.join(schema_builder.SCHEMA_DIR, f"{schema_id}.json")
        if not os.path.exists(file_path):
            logger.error(f"Schema file not found for deletion: {file_path}")
            
            # List available schemas for debugging
            if os.path.exists(schema_builder.SCHEMA_DIR):
                available_schemas = os.listdir(schema_builder.SCHEMA_DIR)
                logger.info(f"Available schema files in directory: {available_schemas}")
            else:
                logger.error(f"Schema directory does not exist: {schema_builder.SCHEMA_DIR}")
            
            return jsonify({
                'success': False,
                'error': f"Schema with ID {schema_id} not found at {file_path}"
            })
        
        # If the schema exists, attempt to delete it
        logger.info(f"Attempting to delete schema file: {file_path}")
        result = schema_builder.delete_schema(schema_id)
        
        # Log the result for debugging
        if result.get('success'):
            logger.info(f"Schema deletion successful: {schema_id}")
        else:
            logger.error(f"Schema deletion failed: {result.get('error')}")
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error in delete_schema endpoint: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/download_schema/<schema_id>', methods=['GET'])
def download_schema(schema_id):
    """API endpoint to download a schema as JSON"""
    try:
        result = schema_builder.load_schema(schema_id)
        
        if not result.get('success', False):
            return jsonify({'error': result.get('error', 'Schema not found')})
        
        schema_data = result.get('schema', {})
        schema_name = result.get('name', 'schema')
        
        # Create response with JSON file
        from flask import Response
        response = Response(
            json.dumps(schema_data, indent=2),
            mimetype='application/json',
            headers={
                'Content-Disposition': f'attachment; filename={schema_name}.json'
            }
        )
        
        return response
    
    except Exception as e:
        logger.error(f"Error downloading schema: {e}")
        return jsonify({'error': str(e)})

@app.route('/upload_schema', methods=['POST'])
def upload_schema():
    """API endpoint to upload and process a schema from JSON file"""
    try:
        if 'schema_file' not in request.files:
            return jsonify({'error': 'No schema file provided'})
        
        schema_file = request.files['schema_file']
        
        if schema_file.filename == '':
            return jsonify({'error': 'No selected file'})
        
        if not schema_file.filename.endswith('.json'):
            return jsonify({'error': 'File must be a JSON file'})
        
        try:
            # Read the JSON file
            schema_data = json.load(schema_file)
            
            # Validate schema
            validation = schema_builder.validate_schema(schema_data)
            if not validation.get('valid', False):
                return jsonify({'error': f'Invalid schema: {validation.get("error", "Unknown error")}'})
            
            # Store in session in both keys to ensure consistency
            session['temp_schema'] = schema_data
            session['schema'] = schema_data  # Add to both keys
            
            # Log schema upload
            logger.info(f"SCHEMA DEBUG: Schema uploaded and stored in session")
            logger.info(f"SCHEMA DEBUG: Schema has {len(schema_data.get('properties', {}))} properties")
            logger.info(f"SCHEMA DEBUG: Schema properties: {list(schema_data.get('properties', {}).keys())}")
            
            return jsonify({
                'success': True,
                'schema': schema_data
            })
            
        except json.JSONDecodeError:
            return jsonify({'error': 'Invalid JSON file'})
        
    except Exception as e:
        logger.error(f"Error uploading schema: {e}")
        return jsonify({'error': str(e)})

@app.route('/update_schema_required', methods=['POST'])
def update_schema_required():
    """API endpoint to update the required status of a column in the current schema"""
    try:
        data = request.json
        column_name = data.get('column_name')
        is_required = data.get('is_required', False)
        current_schema_from_frontend = data.get('current_schema')
        
        if not column_name:
            return jsonify({'error': 'Column name not provided'})
        
        # Use the schema from frontend if provided (includes all local changes)
        # Otherwise fall back to session schema
        if current_schema_from_frontend:
            current_schema = current_schema_from_frontend
            logger.info(f"Using schema from frontend with {len(current_schema.get('properties', {}))} properties")
        else:
            current_schema = session.get('temp_schema') or session.get('schema')
            logger.info(f"Using schema from session with {len(current_schema.get('properties', {}))} properties")
        
        if not current_schema:
            return jsonify({'error': 'No active schema found'})
        
        logger.info(f"Updating required status for column '{column_name}' to {is_required}")
        
        # Use the new schema format function to update the required status
        updated_schema = schema_builder.update_schema_required_status(
            current_schema, column_name, is_required
        )
        
        # Update the schema in session to keep it synchronized
        session['temp_schema'] = updated_schema
        session['schema'] = updated_schema
        
        logger.info(f"Schema updated successfully. Required columns: {updated_schema.get('required', [])}")
        
        return jsonify({
            'success': True,
            'schema': updated_schema,
            'message': f'Column {column_name} {"is now required" if is_required else "is now optional"}'
        })
        
    except Exception as e:
        logger.error(f"Error updating schema required status: {e}")
        return jsonify({'error': str(e)})

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'File must be an Excel file (.xlsx or .xls)'})

    # Get the selected sheet name if provided
    sheet_name = request.form.get('sheet_name', '')

    # Check if we're using a target file
    target_file = request.files.get('target_file')
    target_sheet_name = request.form.get('target_sheet_name', '')
    
    # Get target columns from form or target file
    if target_file:
        try:
            # Save the target file temporarily
            target_filename = secure_filename(target_file.filename)
            target_filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_target_{target_filename}")
            target_file.save(target_filepath)
            
            # Read the target file
            if target_sheet_name:
                target_df = pd.read_excel(target_filepath, sheet_name=target_sheet_name, header=None)
            else:
                # Use the first sheet if none specified
                target_excel_file = pd.ExcelFile(target_filepath)
                target_sheet_name = target_excel_file.sheet_names[0]
                target_df = pd.read_excel(target_filepath, sheet_name=target_sheet_name, header=None)
            
            # Try to find the header row
            from utils.common import infer_header_row
            header_index = infer_header_row(target_df)
            if header_index is not None:
                # Extract headers from the inferred header row
                headers = target_df.iloc[header_index].astype(str).tolist()
                target_columns_list = [h.strip() for h in headers if h.strip()]
            else:
                # If no header row found, use the first row
                headers = target_df.iloc[0].astype(str).tolist()
                target_columns_list = [h.strip() for h in headers if h.strip()]
            
            # Clean up
            os.remove(target_filepath)
            
            if not target_columns_list:
                return jsonify({'error': 'No valid target columns found in the target file'})
        except Exception as e:
            logger.error(f"Error processing target file: {e}")
            return jsonify({'error': f'Error processing target file: {str(e)}'})
    else:
        # Get target columns from form
        target_columns = request.form.get('target_columns', '')
        if not target_columns:
            return jsonify({'error': 'Target columns not provided'})
        
        target_columns_list = [col.strip() for col in target_columns.split(',') if col.strip()]
        if not target_columns_list:
            return jsonify({'error': 'No valid target columns provided'})

    filepath = None
    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        # Store the uploaded file temporarily for re-analysis
        temp_file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{filename}")
        file.seek(0)  # Reset file pointer to beginning
        file.save(temp_file_path)
        
        # Check if a schema is available in the session
        schema = None
        if 'temp_schema' in session:
            schema = session.get('temp_schema')
            logger.info(f"Using schema from session['temp_schema'] with {len(schema.get('properties', {}))} properties")
        elif 'schema' in session:
            schema = session.get('schema')
            logger.info(f"Using schema from session['schema'] with {len(schema.get('properties', {}))} properties")
        
        # Process the file with the selected sheet if provided
        if sheet_name:
            # Read only the selected sheet
            excel_file = pd.ExcelFile(filepath)
            if sheet_name in excel_file.sheet_names:
                # Create a new Excel file with only the selected sheet
                temp_sheet_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_sheet_{filename}")
                df = pd.read_excel(filepath, sheet_name=sheet_name, header=None)
                with pd.ExcelWriter(temp_sheet_path) as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
                
                # Store the sheet name in session for later use
                session['selected_sheet'] = sheet_name
                
                # Process the single-sheet file with schema if available
                if schema:
                    logger.info(f"Running workflow with schema (sheet-specific) for columns: {target_columns_list}")
                    results = run_workflow(temp_sheet_path, target_columns_list, schema=schema, skip_suggestion=True)
                else:
                    logger.info(f"Running workflow without schema (sheet-specific)")
                    results = run_workflow(temp_sheet_path, target_columns_list, skip_suggestion=True)
                
                # Clean up
                os.remove(temp_sheet_path)
            else:
                return jsonify({'error': f'Sheet "{sheet_name}" not found in the Excel file'})
        else:
            # Process the entire file with schema if available
            if schema:
                logger.info(f"Running workflow with schema for columns: {target_columns_list}")
                results = run_workflow(temp_file_path, target_columns_list, schema=schema, skip_suggestion=True)
            else:
                logger.info(f"Running workflow without schema")
                results = run_workflow(temp_file_path, target_columns_list, skip_suggestion=True)
        
        # Get Excel preview
        excel_preview = get_excel_preview(temp_file_path)
        results['excel_preview'] = excel_preview
        
        # Store only essential data in session to avoid large cookie size
        session['filename'] = file.filename
        session['target_columns'] = target_columns_list
        session['temp_file_path'] = temp_file_path
        session['potential_headers'] = results.get('potential_headers', [])
        session['matches'] = results.get('matches', {})
        session['sample_data'] = results.get('sample_data', {})
        session['column_descriptions'] = results.get('column_descriptions', {})
        # Initialize empty suggested headers and data
        session['suggested_headers'] = {}
        session['suggested_data'] = {}
        # Don't store excel_preview in session as it's too large

        return jsonify({
            'success': True,
            'redirect': '/results'
        })
    except Exception as e:
        logger.error(f"Error during file processing: {e}")
        return jsonify({'error': str(e)})
    finally:
        # Ensure original temporary file is removed if it exists
        if filepath and os.path.exists(filepath):
            os.remove(filepath)

@app.route('/results')
def results():
    filename = session.get('filename', 'Unknown file')
    target_columns = session.get('target_columns', [])
    potential_headers = session.get('potential_headers', [])
    matches = session.get('matches', {})
    sample_data = session.get('sample_data', {})
    column_descriptions = session.get('column_descriptions', {})
    suggested_headers = session.get('suggested_headers', {})
    suggested_data = session.get('suggested_data', {})
    
    # Construct results without the excel_preview
    results = {
        'potential_headers': potential_headers,
        'matches': matches,
        'sample_data': sample_data,
        'column_descriptions': column_descriptions,
        'suggested_headers': suggested_headers,
        'suggested_data': suggested_data
    }
    
    return render_template(
        'results.html',
        results=results,
        filename=filename,
        target_columns=target_columns
    )

@app.route('/pdf_results')
def pdf_results():
    filename = session.get('filename', 'Unknown file')
    extracted_data = session.get('extracted_data', {})
    schema_filepath = session.get('schema_filepath', '')
    column_descriptions = session.get('column_descriptions', {})
    
    # Get the schema content
    schema = {}
    if schema_filepath and os.path.exists(schema_filepath):
        try:
            with open(schema_filepath, 'r') as f:
                schema = json.load(f)
        except Exception as e:
            logger.error(f"Error reading schema file: {e}")
    
    # Construct results
    results = {
        'data': extracted_data,
        'error': None if extracted_data else 'No data extracted'
    }
    
    return render_template(
        'pdf_results.html',
        results=results,
        filename=filename,
        column_descriptions=column_descriptions,
        schema=schema
    )

@app.route('/get_excel_preview', methods=['GET'])
def get_excel_preview_route():
    """API endpoint to get Excel preview data for lazy loading"""
    try:
        temp_file_path = session.get('temp_file_path')
        if not temp_file_path or not os.path.exists(temp_file_path):
            return jsonify({'error': 'Excel file not found'})
        
        # Get query parameters for pagination
        sheet_name = request.args.get('sheet', '')
        start_row = int(request.args.get('start', 0))
        num_rows = int(request.args.get('rows', 50))  # Default to 50 rows per page
        
        # Read the Excel file
        excel_file = pd.ExcelFile(temp_file_path)
        sheet_names = excel_file.sheet_names
        
        # If no sheet specified, use the first one
        if not sheet_name and sheet_names:
            sheet_name = sheet_names[0]
        
        if sheet_name not in sheet_names:
            return jsonify({'error': 'Sheet not found'})
        
        # Read the specified sheet
        df = pd.read_excel(temp_file_path, sheet_name=sheet_name, header=None)
        
        # Get dimensions
        total_rows, total_cols = df.shape
        
        # Calculate end row (capped at total rows)
        end_row = min(start_row + num_rows, total_rows)
        
        # Extract the requested rows
        preview_data = []
        for i in range(start_row, end_row):
            row_data = []
            for j in range(total_cols):
                val = df.iloc[i, j]
                # Format the value
                if pd.isna(val):
                    row_data.append("")
                elif isinstance(val, (int, float)) and val == int(val):
                    row_data.append(str(int(val)))
                else:
                    row_data.append(str(val))
            preview_data.append(row_data)
        
        # Generate row numbers
        row_numbers = [str(i+1) for i in range(start_row, end_row)]
        
        return jsonify({
            'sheet_name': sheet_name,
            'sheet_names': sheet_names,
            'data': preview_data,
            'row_numbers': row_numbers,
            'start_row': start_row,
            'end_row': end_row,
            'total_rows': total_rows,
            'total_cols': total_cols,
            'has_more': end_row < total_rows
        })
    
    except Exception as e:
        logger.error(f"Error generating Excel preview: {e}")
        return jsonify({'error': str(e)})

@app.route('/add_header', methods=['POST'])
def add_header():
    try:
        data = request.json
        new_header = data.get('header')
        
        if not new_header or not new_header.strip():
            return jsonify({'error': 'Header name cannot be empty'})
        
        # Get current headers from session
        potential_headers = session.get('potential_headers', [])
        if not potential_headers:
            return jsonify({'error': 'No active analysis session found'})
        
        # Add header if it doesn't already exist
        if new_header not in potential_headers:
            potential_headers.append(new_header)
            session['potential_headers'] = potential_headers
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Header already exists in the list'})
    
    except Exception as e:
        logger.error(f"Error adding header: {e}")
        return jsonify({'error': str(e)})

@app.route('/re_match', methods=['POST'])
def re_match():
    try:
        data = request.json
        target_column = data.get('target_column')
        
        if not target_column:
            return jsonify({'error': 'Target column not specified'})
        
        # Get current data from session
        potential_headers = session.get('potential_headers', [])
        target_columns = session.get('target_columns', [])
        temp_file_path = session.get('temp_file_path')
        matches = session.get('matches', {})
        
        if not potential_headers or not target_columns or not temp_file_path:
            return jsonify({'error': 'No active analysis session found'})
        
        if not os.path.exists(temp_file_path):
            return jsonify({'error': 'Temporary file no longer available'})
        
        # Create a list with just the one target column
        single_target = [target_column]
        
        # Get schema from session if available
        schema = session.get('temp_schema')
        
        # Run the workflow for just this target column with schema if available
        if schema:
            logger.info(f"Re-matching with schema containing properties: {list(schema.get('properties', {}).keys())}")
            results = run_workflow(temp_file_path, single_target, schema=schema)
        else:
            logger.info("Re-matching without schema")
            results = run_workflow(temp_file_path, single_target)
        
        if results.get('error'):
            return jsonify({'error': results['error']})
        
        # Update just this target in the session
        if 'matches' in results and target_column in results['matches']:
            matches[target_column] = results['matches'][target_column]
            session['matches'] = matches
        
        # Update sample data if available
        if 'sample_data' in results and target_column in results['sample_data']:
            sample_data = session.get('sample_data', {})
            sample_data[target_column] = results['sample_data'][target_column]
            session['sample_data'] = sample_data
        
        return jsonify({'success': True})
    
    except Exception as e:
        logger.error(f"Error re-matching: {e}")
        return jsonify({'error': str(e)})

@app.route('/suggest_header', methods=['POST'])
def suggest_header():
    try:
        data = request.json
        target_column = data.get('target_column')
        
        if not target_column:
            return jsonify({'error': 'Target column not specified'})
        
        # Get current data from session
        temp_file_path = session.get('temp_file_path')
        column_descriptions = session.get('column_descriptions', {})
        
        if not temp_file_path:
            return jsonify({'error': 'No active analysis session found'})
        
        if not os.path.exists(temp_file_path):
            return jsonify({'error': 'Temporary file no longer available'})
        
        # Get the column description if available
        column_description = column_descriptions.get(target_column)
        
        # Get schema from session if available
        schema = session.get('temp_schema')
        
        # Run the workflow for just this target column with schema if available
        if schema:
            logger.info(f"Suggesting header with schema containing properties: {list(schema.get('properties', {}).keys())}")
            results = run_workflow(temp_file_path, [target_column], schema=schema)
        else:
            logger.info("Suggesting header without schema")
            results = run_workflow(temp_file_path, [target_column])
        
        if results.get('error'):
            return jsonify({'error': results['error']})
        
        # Get the suggested header
        suggested_header = None
        if 'suggested_headers' in results and target_column in results['suggested_headers']:
            suggested_header = results['suggested_headers'][target_column]
        
        if not suggested_header:
            return jsonify({'error': 'Could not generate a suggested header'})
        
        # Store the suggested header in the session
        suggested_headers = session.get('suggested_headers', {})
        suggested_headers[target_column] = suggested_header
        session['suggested_headers'] = suggested_headers
        
        return jsonify({
            'success': True,
            'suggested_header': suggested_header,
            'target_column': target_column
        })
    
    except Exception as e:
        logger.error(f"Error suggesting header: {e}")
        return jsonify({'error': str(e)})

@app.route('/suggest_sample_data', methods=['POST'])
def suggest_sample_data():
    try:
        data = request.json
        target_column = data.get('target_column')
        
        if not target_column:
            return jsonify({'error': 'Target column not specified'})
        
        # Get current data from session
        temp_file_path = session.get('temp_file_path')
        matches = session.get('matches', {})
        column_descriptions = session.get('column_descriptions', {})
        
        if not temp_file_path:
            return jsonify({'error': 'No active analysis session found'})
        
        if not os.path.exists(temp_file_path):
            return jsonify({'error': 'Temporary file no longer available'})
        
        # Check if this target column has a match
        has_match = target_column in matches and matches[target_column].get('match') != "No match found"
        
        # Get schema from session if available
        schema = session.get('temp_schema')
        
        # Run the workflow for just this target column with schema if available
        if schema:
            logger.info(f"Suggesting sample data with schema containing properties: {list(schema.get('properties', {}).keys())}")
            results = run_workflow(temp_file_path, [target_column], schema=schema)
        else:
            logger.info("Suggesting sample data without schema")
            results = run_workflow(temp_file_path, [target_column])
        
        if results.get('error'):
            return jsonify({'error': results['error']})
        
        # Get the suggested data
        suggested_data_values = None
        if 'suggested_data' in results and target_column in results['suggested_data']:
            suggested_data_values = results['suggested_data'][target_column]
        
        if not suggested_data_values:
            return jsonify({'error': 'Could not generate suggested data'})
        
        # Store the suggested data in the session
        suggested_data = session.get('suggested_data', {})
        suggested_data[target_column] = suggested_data_values
        session['suggested_data'] = suggested_data
        
        return jsonify({
            'success': True,
            'sample_data': suggested_data_values,
            'has_match': has_match,
            'target_column': target_column
        })
    
    except Exception as e:
        logger.error(f"Error suggesting sample data: {e}")
        return jsonify({'error': str(e)})

@app.route('/re_analyze_all', methods=['POST'])
def re_analyze_all():
    try:
        # Get current data from session
        potential_headers = session.get('potential_headers', [])
        target_columns = session.get('target_columns', [])
        temp_file_path = session.get('temp_file_path')
        
        if not potential_headers or not target_columns or not temp_file_path:
            return jsonify({'error': 'No active analysis session found'})
        
        if not os.path.exists(temp_file_path):
            return jsonify({'error': 'Temporary file no longer available'})
        
        # Get schema from session if available
        schema = session.get('temp_schema')
        
        # Run the workflow with schema if available
        if schema:
            logger.info(f"Re-analyzing all with schema containing properties: {list(schema.get('properties', {}).keys())}")
            results = run_workflow(temp_file_path, target_columns, schema=schema)
        else:
            logger.info("Re-analyzing all without schema")
            results = run_workflow(temp_file_path, target_columns)
        
        if results.get('error'):
            return jsonify({'error': results['error']})
        
        # Update the session with new results
        session['matches'] = results.get('matches', {})
        session['sample_data'] = results.get('sample_data', {})
        session['column_descriptions'] = results.get('column_descriptions', {})
        session['suggested_headers'] = results.get('suggested_headers', {})
        session['suggested_data'] = results.get('suggested_data', {})
        
        return jsonify({'success': True})
    
    except Exception as e:
        logger.error(f"Error re-analyzing: {e}")
        return jsonify({'error': str(e)})

@app.route('/get_all_sample_data', methods=['GET'])
def get_all_sample_data():
    """API endpoint to get all sample data for all target columns"""
    try:
        # Get data from session
        sample_data = session.get('sample_data', {})
        
        if not sample_data:
            return jsonify({'error': 'No sample data found in session'})
        
        return jsonify({
            'success': True,
            'sample_data': sample_data
        })
    
    except Exception as e:
        logger.error(f"Error getting all sample data: {e}")
        return jsonify({'error': str(e)})

@app.route('/update_sample_data', methods=['POST'])
def update_sample_data():
    try:
        data = request.json
        target_column = data.get('target_column')
        selected_data = data.get('selected_data', [])
        
        if not target_column:
            return jsonify({'error': 'Target column not specified'})
        
        if not selected_data:
            return jsonify({'error': 'No data selected'})
        
        # Get current sample data from session
        sample_data = session.get('sample_data', {})
        
        if not sample_data:
            return jsonify({'error': 'No active analysis session found'})
        
        # Update the sample data for this target column
        sample_data[target_column] = selected_data
        session['sample_data'] = sample_data
        
        # Get match information for this target column
        matches = session.get('matches', {})
        has_match = target_column in matches and matches[target_column].get('match') != "No match found"
        
        return jsonify({
            'success': True,
            'message': f'Sample data updated for {target_column}',
            'sample_data': selected_data,
            'has_match': has_match,
            'target_column': target_column
        })
    
    except Exception as e:
        logger.error(f"Error updating sample data: {e}")
        return jsonify({'error': str(e)})

@app.route('/export_csv', methods=['POST'])
def export_csv():
    try:
        # Get data from the request
        data = request.json
        export_selections = data.get('export_selections', {})
        
        # Get data from session
        target_columns = session.get('target_columns', [])
        matches = session.get('matches', {})
        sample_data = session.get('sample_data', {})
        suggested_data = session.get('suggested_data', {})
        
        if not target_columns or not matches:
            return jsonify({'error': 'No active analysis session found'})
        
        # Create a CSV file in memory
        from io import StringIO
        import csv
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Write header row - only include the target column names
        writer.writerow(target_columns)
        
        # Determine the maximum number of data rows across all columns
        max_rows = 0
        for target in target_columns:
            # Check which data source to use based on export_selections
            selection = export_selections.get(target, 'sample')
            if selection == 'ai':
                target_data = suggested_data.get(target, [])
            else:  # Default to sample data
                # Use the most recently updated sample data from the session
                target_data = sample_data.get(target, [])
            
            max_rows = max(max_rows, len(target_data))
        
        # Write data rows
        for row_idx in range(max_rows):
            row_data = []
            for target in target_columns:
                # Check which data source to use based on export_selections
                selection = export_selections.get(target, 'sample')
                if selection == 'ai':
                    target_data = suggested_data.get(target, [])
                else:  # Default to sample data
                    # Use the most recently updated sample data from the session
                    target_data = sample_data.get(target, [])
                
                # Add the data value if it exists for this row, otherwise add empty string
                row_data.append(target_data[row_idx] if row_idx < len(target_data) else '')
            
            writer.writerow(row_data)
        
        # Prepare response
        output.seek(0)
        
        # Return CSV content
        return jsonify({
            'success': True,
            'csv_content': output.getvalue()
        })
    
    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        return jsonify({'error': str(e)})

@app.route('/export_mapping', methods=['POST'])
def export_mapping():
    try:
        # Get data from the request
        data = request.json
        export_selections = data.get('export_selections', {})
        selected_cells = data.get('selected_cells', {})
        auto_mapping = data.get('auto_mapping', False)
        all_target_columns = data.get('all_target_columns', [])
        
        # Get data from session
        target_columns = session.get('target_columns', [])
        matches = session.get('matches', {})
        sample_data = session.get('sample_data', {})
        suggested_data = session.get('suggested_data', {})
        temp_file_path = session.get('temp_file_path')
        
        # If auto_mapping is True and all_target_columns is provided, use it instead of target_columns
        if auto_mapping and all_target_columns:
            logger.info(f"Using all_target_columns for auto mapping: {all_target_columns}")
            target_columns = all_target_columns
        
        if not target_columns or not matches or not temp_file_path:
            return jsonify({'error': 'No active analysis session found'})
        
        # Create the state
        state = {
            "file_path": temp_file_path,
            "target_columns": target_columns,
            "matches": matches,
            "sample_data": sample_data,
            "suggested_data": suggested_data,
            "export_selections": export_selections,
            "selected_cells": selected_cells
        }
        
        # Determine which agent to use based on auto_mapping flag
        if auto_mapping:
            # Use the auto cell mapping agent
            from agents.auto_cell_mapping_agent import AutoCellMappingAgent
            agent = AutoCellMappingAgent(verbose=True)
            logger.info("Using AutoCellMappingAgent for automatic cell mapping")
        else:
            # Use the manual cell coordinate agent
            from agents.cell_coordinate_agent import CellCoordinateAgent
            agent = CellCoordinateAgent(verbose=True)
            logger.info("Using CellCoordinateAgent for manual cell mapping")
        
        # Run the agent
        result = agent.run(state)
        
        # Get the mapping JSON
        mapping_json = result.get('mapping_json', '{}')
        
        # Return JSON content
        return jsonify({
            'success': True,
            'mapping': json.loads(mapping_json)
        })
    
    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        return jsonify({'error': str(e)})

@app.route('/download_csv', methods=['GET'])
def download_csv():
    try:
        # Check if we're in PDF mode or Excel mode
        extracted_data = session.get('extracted_data')
        if extracted_data:
            # PDF mode - export the extracted data with proper array handling
            from io import StringIO
            import csv
            
            output = StringIO()
            writer = csv.writer(output)
            
            # Check if this is array of objects data
            schema_filepath = session.get('schema_filepath', '')
            is_array_schema = False
            array_field_name = None
            
            if schema_filepath and os.path.exists(schema_filepath):
                try:
                    with open(schema_filepath, 'r') as f:
                        schema = json.load(f)
                    
                    # Check if this is an array of objects schema
                    is_array_schema = schema_builder.is_array_of_object_schema(schema)
                    if is_array_schema:
                        array_field_name = next(iter(schema['properties'].keys()))
                        logger.info(f"Detected array of objects schema with field: {array_field_name}")
                except Exception as e:
                    logger.error(f"Error reading schema: {e}")
            
            if is_array_schema and array_field_name and array_field_name in extracted_data:
                # Handle array of objects data
                array_data = extracted_data[array_field_name]
                if isinstance(array_data, list) and len(array_data) > 0:
                    # Get headers from the first object and ensure all objects have all keys
                    all_keys = set()
                    for obj in array_data:
                        if isinstance(obj, dict):
                            all_keys.update(obj.keys())
                    
                    headers = sorted(list(all_keys))  # Sort for consistent order
                    writer.writerow(headers)
                    
                    # Get user commodity selections and find commodity code column
                    commodity_selections = session.get('commodity_selections', {})
                    commodity_code_col = None
                    
                    # Only look for commodity code column if we have selections
                    if commodity_selections:
                        for header in headers:
                            if 'commodity' in header.lower() and 'code' in header.lower():
                                commodity_code_col = header
                                logger.info(f"Found commodity code column for CSV export: {commodity_code_col}")
                                break
                    
                    # Write each object as a row
                    for row_index, obj in enumerate(array_data):
                        if isinstance(obj, dict):
                            row_data = []
                            for header in headers:
                                value = obj.get(header, '')  # Use empty string if key is missing
                                
                                # Only apply commodity code selection if column exists and user has made a selection
                                if (commodity_code_col and 
                                    header == commodity_code_col and 
                                    str(row_index) in commodity_selections and 
                                    commodity_selections[str(row_index)]['code']):
                                    value = commodity_selections[str(row_index)]['code']
                                    logger.info(f"Using user-selected commodity code for row {row_index}: {value}")
                                
                                if isinstance(value, (dict, list)):
                                    value = json.dumps(value)
                                elif value is None:
                                    value = ''
                                row_data.append(str(value))
                            writer.writerow(row_data)
                        else:
                            # If not a dict, create a row with the single value in first column
                            row_data = [str(obj)] + [''] * (len(headers) - 1)
                            writer.writerow(row_data)
                    
                    logger.info(f"Exported {len(array_data)} objects with {len(headers)} columns")
                else:
                    # Empty array or not a list, create headers only
                    try:
                        # Get headers from schema
                        with open(schema_filepath, 'r') as f:
                            schema = json.load(f)
                        array_property = schema['properties'][array_field_name]
                        if 'items' in array_property and 'properties' in array_property['items']:
                            headers = list(array_property['items']['properties'].keys())
                            writer.writerow(headers)
                            logger.info(f"Exported headers only (no data): {headers}")
                    except Exception as e:
                        logger.error(f"Error getting headers from schema: {e}")
                        writer.writerow(['No data available'])
            else:
                # Handle regular PDF data (not array of objects)
                # Get target columns from schema if available, otherwise use extracted field names
                target_columns = []
                
                if schema_filepath and os.path.exists(schema_filepath):
                    try:
                        with open(schema_filepath, 'r') as f:
                            schema = json.load(f)
                        if 'properties' in schema:
                            target_columns = list(schema['properties'].keys())
                            logger.info(f"Using target columns from schema: {target_columns}")
                    except Exception as e:
                        logger.error(f"Error reading schema for target columns: {e}")
                
                # Use target columns if available, otherwise fallback to extracted field names
                fields = target_columns if target_columns else list(extracted_data.keys())
                writer.writerow(fields)
                
                # Check if any field contains an array
                array_fields = [field for field in fields if isinstance(extracted_data.get(field), list)]
                has_arrays = len(array_fields) > 0
                
                if has_arrays:
                    # Find the maximum array length to determine number of rows
                    max_rows = 1
                    for field in array_fields:
                        if isinstance(extracted_data.get(field), list):
                            max_rows = max(max_rows, len(extracted_data[field]))
                    
                    # Get user commodity selections and find commodity code column
                    commodity_selections = session.get('commodity_selections', {})
                    commodity_code_field_index = None
                    
                    # Only look for commodity code field if we have selections
                    if commodity_selections:
                        for i, field in enumerate(fields):
                            if 'commodity' in field.lower() and 'code' in field.lower():
                                commodity_code_field_index = i
                                logger.info(f"Found commodity code field for CSV export: {field} at index {i}")
                                break
                    
                    # Create rows for array data
                    for row_index in range(max_rows):
                        row_data = []
                        for field_index, field in enumerate(fields):
                            value = extracted_data.get(field, '')
                            
                            # Check if this is the commodity code field and user has made a selection
                            if (field_index == commodity_code_field_index and 
                                str(row_index) in commodity_selections and 
                                commodity_selections[str(row_index)]['code']):
                                value = commodity_selections[str(row_index)]['code']
                                logger.info(f"Using user-selected commodity code for row {row_index}: {value}")
                            elif isinstance(value, list):
                                # Use array item if available, otherwise empty string
                                if row_index < len(value):
                                    value = value[row_index]
                                else:
                                    value = ''
                            else:
                                # For non-array fields, use the value only in the first row
                                if row_index == 0:
                                    if isinstance(value, dict):
                                        value = json.dumps(value)
                                    else:
                                        value = value or ''
                                else:
                                    value = ''
                            
                            row_data.append(value)
                        
                        writer.writerow(row_data)
                else:
                    # No arrays, create single row
                    row_data = []
                    for field in fields:
                        value = extracted_data.get(field, '')
                        # Convert complex values to string
                        if isinstance(value, dict):
                            value = json.dumps(value)
                        elif isinstance(value, list):
                            value = json.dumps(value)
                        row_data.append(value)
                    
                    writer.writerow(row_data)
        else:
            # Excel mode - use the sample data
            target_columns = session.get('target_columns', [])
            sample_data = session.get('sample_data', {})
            
            if not target_columns or not sample_data:
                return jsonify({'error': 'No active analysis session found'})
            
            # Create a CSV file in memory
            from io import StringIO
            import csv
            
            output = StringIO()
            writer = csv.writer(output)
            
            # Write header row - only include the target column names
            writer.writerow(target_columns)
            
            # Determine the maximum number of data rows across all columns
            max_rows = 0
            for target in target_columns:
                target_sample_data = sample_data.get(target, [])
                max_rows = max(max_rows, len(target_sample_data))
            
            # Write data rows
            for row_idx in range(max_rows):
                row_data = []
                for target in target_columns:
                    target_sample_data = sample_data.get(target, [])
                    # Add the data value if it exists for this row, otherwise add empty string
                    row_data.append(target_sample_data[row_idx] if row_idx < len(target_sample_data) else '')
                
                writer.writerow(row_data)
        
        # Prepare response
        output.seek(0)
        
        # Create response with CSV file
        from flask import Response
        response = Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={
                'Content-Disposition': 'attachment; filename=header_matching_results.csv'
            }
        )
        
        return response
    
    except Exception as e:
        logger.error(f"Error downloading CSV: {e}")
        return jsonify({'error': str(e)})

# AI Chatbot routes for CSV editing
@app.route('/get_csv_data', methods=['POST'])
def get_csv_data():
    """Get the current CSV data for editing with AI."""
    try:
        # Get export selections from request
        data = request.json
        export_selections = data.get('export_selections', {})
        
        # Check if we're in PDF mode or Excel mode
        extracted_data = session.get('extracted_data')
        target_columns = session.get('target_columns', [])
        sample_data = session.get('sample_data', {})
        suggested_data = session.get('suggested_data', {})
        
        # Determine the mode based on available data
        if extracted_data and not target_columns:
            # PDF mode with single-row extracted data
            logger.info("Getting CSV data for PDF mode (single row)")
            
            # Create CSV data from PDF extracted data
            headers = list(extracted_data.keys())
            csv_data = {
                'headers': headers,
                'data': []
            }
            
            # Create a single row with all the PDF data
            row = {}
            for field in headers:
                value = extracted_data.get(field, '')
                # Convert complex values to string for CSV compatibility
                if isinstance(value, (list, dict)):
                    value = json.dumps(value)
                elif value is None:
                    value = ''
                row[field] = str(value)
            
            csv_data['data'].append(row)
            
            logger.info(f"PDF mode CSV data created with {len(headers)} fields")
            
            return jsonify({
                'success': True,
                'csv_data': csv_data
            })
        elif target_columns:
            # Excel mode OR PDF mode converted to multi-row format
            if extracted_data:
                logger.info("Getting CSV data for PDF mode that was converted to multi-row format")
            else:
                logger.info(f"Getting CSV data for Excel mode with {len(target_columns)} columns")
            
            # Collect data for each column based on selections
            csv_data = {
                'headers': target_columns,
                'data': []
            }
            
            # Determine the maximum number of rows and collect column data
            max_rows = 0
            column_data = {}
            
            for target in target_columns:
                # Check which data source to use based on export_selections
                selection = export_selections.get(target, 'sample')
                
                if selection == 'ai' and target in suggested_data and suggested_data[target]:
                    # Use AI suggested data if available
                    target_data = suggested_data[target]
                else:
                    # Default to sample data
                    target_data = sample_data.get(target, [])
                    
                    # If no sample data, create some placeholder data
                    if not target_data:
                        target_data = ["Sample 1", "Sample 2", "Sample 3"]
                
                # Store the data for this column
                column_data[target] = target_data
                max_rows = max(max_rows, len(target_data))
            
            # Ensure we have at least one row
            max_rows = max(1, max_rows)
            
            # Build row-oriented data
            for row_idx in range(max_rows):
                row = {}
                for target in target_columns:
                    target_data = column_data[target]
                    row[target] = target_data[row_idx] if row_idx < len(target_data) else ''
                
                csv_data['data'].append(row)
            
            return jsonify({
                'success': True,
                'csv_data': csv_data
            })
        else:
            # No active session found
            return jsonify({'error': 'No active analysis session found'})
    
    except Exception as e:
        logger.error(f"Error getting CSV data: {e}")
        return jsonify({'error': str(e)})



@app.route('/chat_with_csv_editor', methods=['POST'])
def chat_with_csv_editor():
    """Handle chatbot interactions for CSV editing with human-in-the-loop capability."""
    try:
        # Get data from request
        data = request.json
        message = data.get('message', '')
        csv_data = data.get('csv_data', {})
        source_data = data.get('source_data', {})
        # Get thread_id to support multi-turn conversations with interruptions
        thread_id = data.get('thread_id')
        # Get the temp file path if this is a continuation of a previous conversation
        csv_file_path = data.get('csv_file_path')
        
        if not source_data:
            source_data = {}
            logger.info("No source data provided, using empty dict")
        
        if not message or not csv_data:
            logger.error("Missing required parameters in chat_with_csv_editor")
            return jsonify({'success': False, 'error': 'Missing required parameters'})
        
        # Initialize logging
        logger.info(f"Chat message received: {message[:50]}...")
        if thread_id:
            logger.info(f"Continuing conversation with thread_id: {thread_id}")
        
        # Import the CSV Edit Supervisor Agent
        try:
            from agents.csv_edit_supervisor import CSVEditSupervisorAgent
            agent = CSVEditSupervisorAgent(verbose=True)
            logger.info("CSV Edit Supervisor Agent initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing CSV Edit Supervisor Agent: {e}", exc_info=True)
            return jsonify({'error': f'Failed to initialize AI supervisor agent: {str(e)}'})
        
        # Convert the CSV data to a pandas DataFrame for the agent
        try:
            import pandas as pd
            import tempfile
            import os
            import json
            
            # Create a message
            from langchain_core.messages import HumanMessage
            user_message = HumanMessage(content=message)
            
            # Check if we're continuing a conversation with a temp file that already exists
            if not csv_file_path or not os.path.exists(csv_file_path):
                # This is a new conversation, create a new temp file
                headers = csv_data.get('headers', [])
                data_rows = csv_data.get('data', [])
                
                # Sanitize data rows to handle NaN values
                sanitized_rows = []
                for row in data_rows:
                    sanitized_row = {}
                    for key, value in row.items():
                        # Convert NaN values to empty strings but preserve actual values
                        if value is None or (isinstance(value, (float, int)) and pd.isna(value)):
                            sanitized_row[key] = ""
                        else:
                            sanitized_row[key] = value
                    
                    # Log the sanitized row for debugging
                    logger.info(f"Sanitized row: {sanitized_row}")
                    
                    # Check if the row has any non-empty values
                    has_data = any(val for val in sanitized_row.values() if val)
                    if not has_data:
                        logger.warning(f"Row contains only empty values: {sanitized_row}")
                    
                    sanitized_rows.append(sanitized_row)
                
                # Log the data before creating DataFrame
                logger.info(f"Creating DataFrame with {len(sanitized_rows)} rows and columns: {headers}")
                if len(sanitized_rows) > 0:
                    logger.info(f"First row sample: {sanitized_rows[0]}")
                
                # Create DataFrame from the sanitized data rows
                df = pd.DataFrame(sanitized_rows)
                
                # Log the DataFrame after creation
                logger.info(f"Created DataFrame with shape: {df.shape}")
                if not df.empty:
                    logger.info(f"DataFrame columns: {list(df.columns)}")
                    logger.info(f"DataFrame first row: {df.iloc[0].to_dict() if len(df) > 0 else 'No rows'}")
                
                # Create a temporary file for the CSV data in the system's temp directory
                import uuid
                fd, csv_file_path = tempfile.mkstemp(prefix='temp_csv_', suffix='.csv')
                os.close(fd)  # Close the file descriptor
                
                # Save the DataFrame to the temporary file
                df.to_csv(csv_file_path, index=False)
                logger.info(f"Saved CSV data to temporary file: {csv_file_path}")
                
                # Verify the CSV file was written correctly
                try:
                    # Read back the CSV file to verify it has data
                    verification_df = pd.read_csv(csv_file_path)
                    logger.info(f"Verification: CSV file contains {len(verification_df)} rows and {len(verification_df.columns)} columns")
                    if not verification_df.empty:
                        logger.info(f"Verification: First row of CSV: {verification_df.iloc[0].to_dict() if len(verification_df) > 0 else 'No rows'}")
                    else:
                        logger.warning("Verification: CSV file is empty (has headers but no data rows)")
                        
                        # If the CSV is empty but we had data, try writing it again with a different method
                        if len(sanitized_rows) > 0:
                            logger.info("Attempting to rewrite CSV with alternative method...")
                            
                            # Method 1: Write directly with csv module
                            import csv
                            with open(csv_file_path, 'w', newline='') as csvfile:
                                # Get field names from the first row
                                fieldnames = list(sanitized_rows[0].keys())
                                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                                
                                # Write headers and rows
                                writer.writeheader()
                                for row in sanitized_rows:
                                    writer.writerow(row)
                            
                            # Verify the rewrite worked
                            verification_df = pd.read_csv(csv_file_path)
                            logger.info(f"After rewrite: CSV file contains {len(verification_df)} rows and {len(verification_df.columns)} columns")
                except Exception as e:
                    logger.error(f"Error verifying CSV file: {e}")
            else:
                # We're continuing with an existing CSV file
                logger.info(f"Continuing with existing CSV file: {csv_file_path}")
            
            # Prepare the state
            if thread_id:
                # If we have a thread_id, this is a continuation of a previous conversation
                logger.info(f"Resuming conversation with thread_id: {thread_id}")
                
                # Get the original request from the request data
                original_request = data.get('original_request', '')
                
                # Log the original request for debugging
                logger.info(f"Original request for resume: '{original_request}'")
                
                # If original request is empty but we are resuming, try to extract it from 
                # the first message's content to preserve context
                if not original_request and message:
                    original_request = message
                    logger.info(f"Using current message as original request: '{original_request}'")
                
                # The state should contain everything needed for resumption
                state = {
                    'messages': [user_message],  # Only need the user's new message for resume
                    'csv_file_path': csv_file_path,
                    'source_data': source_data,
                    'thread_id': thread_id,
                    # Get these from request data if available
                    'original_request': original_request,
                    'rewritten_request': data.get('rewritten_request', ''),
                    'in_clarification_mode': data.get('in_clarification_mode', False),
                    'is_request_clarified': data.get('is_request_clarified', False),
                    'clarification_count': data.get('clarification_count', 0),
                    'last_active_node': data.get('last_active_node', '')
                }
                
                # Resume the conversation by calling the resume method
                logger.info(f"Resuming conversation with state: {state}")
                result = agent.resume(state, message)
            else:
                # This is a new conversation
                logger.info("Starting new conversation")
                # Store the original request in the state
                original_request = message
                logger.info(f"Setting original request: '{original_request}'")
                
                state = {
                    'messages': [user_message],
                    'csv_file_path': csv_file_path,
                    'source_data': source_data,
                    'original_request': original_request  # Explicitly set the original request
                }
                
                # Run the agent
                logger.info("Running CSV Edit Supervisor Agent")
                result = agent.run(state)
            
            logger.info("CSV Edit Supervisor Agent execution completed")
            
            # Check if we need to interrupt for user input
            interrupt_message = result.get('interrupt_message')
            needs_input = result.get('needs_input', False)
            
            if needs_input and interrupt_message:
                logger.info(f"Supervisor agent requesting human input: {interrupt_message}")
                
                # Don't delete the temporary file, as we'll need it for the resumed conversation
                # Instead, return response indicating the need for user input
                # Include thread_id, csv_file_path, and all state needed for resumption
                response_obj = {
                    'success': True,
                    'needs_input': True,
                    'interrupt_message': interrupt_message,
                    'thread_id': result.get('thread_id'),
                    'csv_file_path': csv_file_path,
                    'csv_data': csv_data,  # Return the original data
                    'original_request': result.get('original_request', ''),
                    'rewritten_request': result.get('rewritten_request', ''),
                    'in_clarification_mode': result.get('in_clarification_mode', False),
                    'is_request_clarified': result.get('is_request_clarified', False),
                    'clarification_count': result.get('clarification_count', 0),
                    'last_active_node': result.get('last_active_node', '')
                }
                
                return app.response_class(
                    response=json.dumps(response_obj, cls=NpEncoder),
                    status=200,
                    mimetype='application/json'
                )
            
            # Process the response by extracting messages from named agents
            from langchain_core.messages import AIMessage
            
            # First check if there are any out-of-scope messages from the request_clarifier
            request_clarifier_messages = [msg for msg in result.get('messages', [])
                                         if isinstance(msg, HumanMessage) and getattr(msg, 'name', '') == 'request_clarifier'
                                         and "OUT_OF_SCOPE" in msg.content]
            
            # If we found an out-of-scope message, use the standardized message from Config
            if request_clarifier_messages:
                logger.info("Out-of-scope request detected in response processing")
                response = Config.OUT_OF_SCOPE_MESSAGE
            else:
                # Extract responses from named agents (primary source of information)
                supervisor_messages = [msg for msg in result.get('messages', []) 
                                     if isinstance(msg, HumanMessage) and getattr(msg, 'name', '') == 'supervisor']
                edit_messages = [msg for msg in result.get('messages', []) 
                               if isinstance(msg, HumanMessage) and getattr(msg, 'name', '') == 'csv_edit']
                verifier_messages = [msg for msg in result.get('messages', []) 
                                   if isinstance(msg, HumanMessage) and getattr(msg, 'name', '') == 'csv_verifier']
                
                # Find the action summary message (should be at the end of the conversation)
                action_summary = next((msg.content for msg in reversed(result.get('messages', [])) 
                                     if isinstance(msg, HumanMessage) and getattr(msg, 'name', '') == 'csv_edit' 
                                     and not msg.content.startswith("CSV edit complete")), None)
                
                # Also collect AIMessages as a fallback
                ai_messages = [msg for msg in result.get('messages', []) if isinstance(msg, AIMessage)]
                
                # Build response from components
                agent_responses = []
                
                # Add the action summary first if available (most important)
                if action_summary:
                    agent_responses.append(action_summary)
                else:
                    # If no action summary, compile from individual agent messages
                    if supervisor_messages:
                        agent_responses.append(f"Supervisor: {supervisor_messages[-1].content}")
                    if edit_messages:
                        agent_responses.append(f"Edit: {edit_messages[-1].content}")
                    if verifier_messages:
                        agent_responses.append(f"Verification: {verifier_messages[-1].content}")
                    
                    # If we still don't have any agent responses, fall back to AI messages
                    if not agent_responses and ai_messages:
                        agent_responses.append(ai_messages[-1].content)
                
                # Combine responses or use fallback
                if agent_responses:
                    response = "\n\n".join(agent_responses)
                else:
                    # Only use this generic message if we truly have no responses
                    response = "The CSV editor processed your request but did not provide detailed feedback."
            
            # Check if the CSV file was modified by reading it back
            csv_data_changed = False
            new_csv_data = csv_data
            
            try:
                # Read the CSV file back to check for changes
                if os.path.exists(csv_file_path):
                    # Read the modified CSV file
                    modified_df = pd.read_csv(csv_file_path)
                    
                    # Convert to the expected format
                    new_csv_data = {
                        'headers': list(modified_df.columns),
                        'data': modified_df.to_dict('records')
                    }
                    
                    # Check if the data actually changed
                    if (len(new_csv_data['headers']) != len(csv_data.get('headers', [])) or 
                        len(new_csv_data['data']) != len(csv_data.get('data', []))):
                        csv_data_changed = True
                    else:
                        # Check if content is different
                        original_data_str = str(csv_data)
                        new_data_str = str(new_csv_data)
                        csv_data_changed = original_data_str != new_data_str
                    
                    logger.info(f"CSV file read back successfully, data changed: {csv_data_changed}")
                else:
                    logger.warning(f"CSV file not found after agent execution: {csv_file_path}")
            except Exception as e:
                logger.error(f"Error reading back CSV file: {e}", exc_info=True)
            
            # Clean up the temporary file - only if we're done with the conversation
            # Don't clean up if we are using human-in-the-loop and might need to resume
            if not needs_input and not interrupt_message:
                try:
                    if os.path.exists(csv_file_path):
                        os.remove(csv_file_path)
                        logger.info(f"Removed temporary CSV file: {csv_file_path}")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary CSV file: {e}")
            
            # Update the session data so it's available for the CSV Export tab
            if csv_data_changed:
                headers = new_csv_data.get('headers', [])
                data_rows = new_csv_data.get('data', [])
                
                new_sample_data = {}
                for col in headers:
                    column_data = []
                    for row in data_rows:
                        if col in row:
                            column_data.append(row[col])
                    new_sample_data[col] = column_data
                
                # Update session data - explicitly update this to ensure export has latest data
                logger.info(f"Updating session sample_data with CSV edit agent changes for columns: {headers}")
                
                # Check if any new columns were added that aren't in the target_columns
                target_columns = session.get('target_columns', [])
                new_columns = [col for col in headers if col not in target_columns]
                
                if new_columns:
                    logger.info(f"New columns added by chatbot: {new_columns}")
                    # Add the new columns to target_columns in the session
                    session['target_columns'] = target_columns + new_columns
                
                # Update the sample data with all columns, including new ones
                session['sample_data'] = new_sample_data
            
            # Create response object with sanitized data
            # Manually sanitize the data to ensure no NaN values
            sanitized_csv_data = {
                'headers': new_csv_data.get('headers', []),
                'data': []
            }
            
            # Sanitize each row in the data
            for row in new_csv_data.get('data', []):
                sanitized_row = {}
                for key, value in row.items():
                    # Convert NaN values to empty strings
                    if value is None or (isinstance(value, float) and np.isnan(value)):
                        sanitized_row[key] = ""
                    else:
                        sanitized_row[key] = value
                sanitized_csv_data['data'].append(sanitized_row)
            
            response_obj = {
                'success': True,
                'response': response,
                'csv_data': sanitized_csv_data,
                'csv_data_changed': csv_data_changed,
                'needs_input': False  # Explicitly mark that we don't need input
            }
            
            logger.info("Successfully processed chat_with_csv_editor request with sanitized data")
            # Use the custom JSON encoder to handle NaN values
            return app.response_class(
                response=json.dumps(response_obj, cls=NpEncoder),
                status=200,
                mimetype='application/json'
            )
            
        except Exception as e:
            logger.error(f"Error processing CSV edit: {e}", exc_info=True)
            return jsonify({'success': False, 'error': f'Error processing your request: {str(e)}'})
    
    except Exception as e:
        logger.error(f"Error in CSV editing chat: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

@app.route('/check_ipaffs_compatibility', methods=['POST'])
def check_ipaffs_compatibility():
    """Check if the current PDF data is compatible with IPAFFS requirements."""
    try:
        # IPAFFS required headers (case-insensitive matching)
        ipaffs_required_headers = [
            'commodity code',
            'genus and species', 
            'eppo code',
            'variety',
            'class',
            'intended for final users',
            'commercial flower production',
            'number of packages',
            'type of package',
            'quantity',
            'quantity type',
            'net weight (kg)',
            'controlled atmosphere container'
        ]
        
        # Get current data headers
        extracted_data = session.get('extracted_data', {})
        target_columns = session.get('target_columns', [])
        
        current_headers = []
        
        # Always check actual extracted data first for IPAFFS compatibility
        # The schema might be limited but the actual extraction could have found all fields
        if extracted_data:
            # Check if extracted_data contains array of objects
            array_of_objects_field = None
            for field, value in extracted_data.items():
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    array_of_objects_field = field
                    # Extract headers from the first object
                    current_headers = list(value[0].keys())
                    logger.info(f"Found array of objects in field '{field}', using object keys: {current_headers}")
                    break
            
            if not array_of_objects_field:
                # Single row PDF data - use field names
                current_headers = list(extracted_data.keys())
                logger.info(f"Using extracted_data keys for IPAFFS check: {current_headers}")
        else:
            logger.warning("No data found for IPAFFS compatibility check")
            return jsonify({'compatible': False, 'reason': 'No data found'})
        
        # Normalize headers for comparison (lowercase, strip whitespace)
        normalized_current = [h.lower().strip() for h in current_headers]
        
        logger.info(f"Checking IPAFFS compatibility with headers: {current_headers}")
        logger.info(f"Normalized headers: {normalized_current}")
        
        # Check for required headers with exact matching only
        matched_headers = {}
        missing_headers = []
        
        for required in ipaffs_required_headers:
            found = False
            for current, normalized in zip(current_headers, normalized_current):
                # Exact match only - much stricter
                if (normalized == required.lower() or
                    # Only specific exact variations
                    (required == 'genus and species' and normalized == 'genus and species') or
                    (required == 'eppo code' and normalized in ['eppocode', 'eppo code']) or
                    (required == 'intended for final users' and normalized in ['intended for final users', 'intended for final users (or commercial flower production)']) or
                    (required == 'commercial flower production' and normalized in ['commercial flower production', 'intended for final users (or commercial flower production)']) or
                    (required == 'net weight (kg)' and normalized in ['net weight (kg)', 'net weight'])):
                    matched_headers[required] = current
                    found = True
                    logger.info(f"Matched IPAFFS header '{required}' with '{current}'")
                    break
            
            if not found:
                missing_headers.append(required)
                logger.info(f"Missing IPAFFS header: {required}")
        
        # Determine compatibility - need at least key headers
        key_headers = ['genus and species', 'commodity code', 'eppo code']
        has_key_headers = all(key in matched_headers for key in key_headers)
        
        compatible = has_key_headers and len(matched_headers) >= len(ipaffs_required_headers) * 0.6  # At least 60% match
        
        logger.info(f"IPAFFS compatibility result: {compatible} (matched {len(matched_headers)}/{len(ipaffs_required_headers)})")
        
        return jsonify({
            'compatible': compatible,
            'matched_headers': matched_headers,
            'missing_headers': missing_headers,
            'total_matched': len(matched_headers),
            'total_required': len(ipaffs_required_headers)
        })
    
    except Exception as e:
        logger.error(f"Error checking IPAFFS compatibility: {e}")
        return jsonify({'compatible': False, 'error': str(e)})

@app.route('/prefill_ipaffs', methods=['POST'])
def prefill_ipaffs():
    """Pre-fill IPAFFS data using EPPO database."""
    try:

        
        # Initialize EPPO lookup and commodity filter
        lookup = get_eppo_lookup()
        commodity_filter = get_commodity_filter()
        
        # Get current data
        extracted_data = session.get('extracted_data', {})
        target_columns = session.get('target_columns', [])
        sample_data = session.get('sample_data', {})
        
        # Determine data format and extract genus/species data
        genus_species_data = []
        is_array_format = False
        
        # Always check actual extracted data first, like the compatibility checker
        if extracted_data:
            # Check if extracted_data contains array of objects
            array_of_objects_field = None
            for field, value in extracted_data.items():
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    array_of_objects_field = field
                    # Check if the objects have genus and species
                    first_obj = value[0]
                    genus_species_key = None
                    for key in first_obj.keys():
                        if 'genus' in key.lower() and 'species' in key.lower():
                            genus_species_key = key
                            break
                    
                    if genus_species_key:
                        logger.info(f"Processing IPAFFS pre-fill for array of objects format (field: {field})")
                        is_array_format = True
                        # Extract genus/species data from each object
                        for obj in value:
                            genus_species_data.append(obj.get(genus_species_key, ''))
                        logger.info(f"Found genus/species key '{genus_species_key}' with {len(genus_species_data)} entries")
                        break
            
            if not is_array_format:
                # Single row format
                logger.info("Processing IPAFFS pre-fill for single row format")
                
                # Find genus and species field
                genus_species_field = None
                for field in extracted_data.keys():
                    if 'genus' in field.lower() and 'species' in field.lower():
                        genus_species_field = field
                        break
                
                if not genus_species_field:
                    logger.error(f"Genus and Species field not found in extracted_data keys: {list(extracted_data.keys())}")
                    return jsonify({'error': 'Genus and Species field not found'})
                
                genus_species_value = extracted_data.get(genus_species_field, '')
                if isinstance(genus_species_value, list):
                    genus_species_data = genus_species_value
                else:
                    genus_species_data = [genus_species_value] if genus_species_value else []
                logger.info(f"Found genus/species field '{genus_species_field}' with value: {genus_species_value}")
        elif target_columns and sample_data:
            # Fallback to target_columns only if no extracted_data found array of objects
            is_array_format = True
            logger.info("Processing IPAFFS pre-fill for array/multi-row format (fallback)")
            
            # Find the genus and species column
            genus_species_col = None
            for col in target_columns:
                if 'genus' in col.lower() and 'species' in col.lower():
                    genus_species_col = col
                    break
            
            if not genus_species_col:
                logger.error(f"Genus and Species column not found in target_columns: {target_columns}")
                return jsonify({'error': 'Genus and Species column not found'})
            
            genus_species_data = sample_data.get(genus_species_col, [])
            logger.info(f"Found genus/species column '{genus_species_col}' with {len(genus_species_data)} entries")
        else:
            logger.error("No data found for IPAFFS pre-fill")
            return jsonify({'error': 'No data found for IPAFFS pre-fill'})
        
        # Process each genus/species entry
        eppo_codes = []
        commodity_options = []  # List of options for each row
        existing_commodity_codes = []  # Track existing commodity codes to preserve them
        
        # First, extract existing commodity codes from the data
        if is_array_format and array_of_objects_field and array_of_objects_field in extracted_data:
            # Array of objects format - extract existing commodity codes
            objects_array = extracted_data[array_of_objects_field]
            if objects_array and len(objects_array) > 0:
                first_obj = objects_array[0]
                object_columns = list(first_obj.keys())
                
                # Find commodity code column using flexible matching
                def find_commodity_code_column(columns):
                    for column in columns:
                        column_lower = column.lower().strip()
                        if ('commodity' in column_lower and 'code' in column_lower):
                            return column
                    return None
                
                commodity_code_col = find_commodity_code_column(object_columns)
                
                if commodity_code_col:
                    for obj in objects_array:
                        existing_code = obj.get(commodity_code_col, '')
                        # Only consider non-empty, non-whitespace codes as existing
                        if existing_code and str(existing_code).strip() != '' and str(existing_code).strip() not in ['0', '0.0']:
                            existing_commodity_codes.append(str(existing_code).strip())
                            logger.info(f"Found existing commodity code: {existing_code}")
                        else:
                            existing_commodity_codes.append('')
                else:
                    existing_commodity_codes = [''] * len(objects_array)
            else:
                existing_commodity_codes = [''] * len(genus_species_data)
        elif target_columns and sample_data:
            # Multi-row format - extract existing commodity codes
            # Find commodity code column using flexible matching
            def find_commodity_code_column(columns):
                for column in columns:
                    column_lower = column.lower().strip()
                    if ('commodity' in column_lower and 'code' in column_lower):
                        return column
                return None
            
            commodity_code_col = find_commodity_code_column(target_columns)
            
            if commodity_code_col and commodity_code_col in sample_data:
                existing_codes = sample_data[commodity_code_col]
                for code in existing_codes:
                    # Only consider non-empty, non-whitespace codes as existing
                    if code and str(code).strip() != '' and str(code).strip() not in ['0', '0.0']:
                        existing_commodity_codes.append(str(code).strip())
                        logger.info(f"Found existing commodity code: {code}")
                    else:
                        existing_commodity_codes.append('')
            else:
                existing_commodity_codes = [''] * len(genus_species_data)
        else:
            # Single row format
            existing_code = ''
            for field in extracted_data.keys():
                if 'commodity' in field.lower() and 'code' in field.lower():
                    value = extracted_data.get(field, '')
                    if value and str(value).strip() != '' and str(value).strip() not in ['0', '0.0']:
                        existing_code = str(value).strip()
                        logger.info(f"Found existing commodity code: {existing_code}")
                        break
            existing_commodity_codes = [existing_code]
        
        logger.info(f"Existing commodity codes: {existing_commodity_codes}")
        
        for i, genus_species in enumerate(genus_species_data):
            # Check if this row already has a commodity code (non-empty and non-whitespace)
            has_existing_code = (i < len(existing_commodity_codes) and 
                               existing_commodity_codes[i] and 
                               existing_commodity_codes[i].strip() != '' and
                               existing_commodity_codes[i].strip() not in ['0', '0.0'])
            
            if not genus_species or genus_species.strip() == '':
                eppo_codes.append('')
                commodity_options.append([])
                continue
            
            # Query EPPO database using enhanced IPAFFS approach
            try:
                # Use the new enhanced IPAFFS lookup method
                eppo_code, results = lookup.enhanced_lookup_ipaffs(genus_species)
                
                if eppo_code:
                    eppo_codes.append(eppo_code)
                    
                    # Only generate commodity options if no existing code is present
                    if not has_existing_code:
                        # Filter results to only include valid commodity codes for dropdown options
                        filtered_results = commodity_filter.filter_eppo_results(results)
                        
                        if filtered_results:
                            # Create commodity code options (only valid codes)
                            options = []
                            for commodity_name, eppo, commodity_code, description in filtered_results:
                                options.append({
                                    'code': commodity_code,
                                    'description': description,
                                    'display': f"{commodity_code} - {description}"
                                })
                            commodity_options.append(options)
                            
                            logger.info(f"Enhanced IPAFFS lookup found {len(filtered_results)} valid commodity codes for '{genus_species}' with EPPO code '{eppo_code}'")
                        else:
                            # No valid commodity codes found after filtering
                            # This can happen when we construct an EPPO code but have no matching results in DB
                            # or when results don't match the valid commodity code filter
                            
                            # If we have results but they're filtered out, create options from all valid codes
                            if results:
                                # Create options from all valid commodity codes (from the hardcoded list)
                                # Import the hardcoded commodity codes from eppo_lookup module
                                from eppo_lookup import COMMODITY_CODES
                                options = []
                                for code, description in COMMODITY_CODES.items():
                                    options.append({
                                        'code': code,
                                        'description': description,
                                        'display': f"{code} - {description}"
                                    })
                                commodity_options.append(options)
                                logger.info(f"IPAFFS lookup used fallback: providing all valid commodity codes for '{genus_species}' with constructed EPPO code '{eppo_code}'")
                            else:
                                commodity_options.append([])
                                logger.info(f"No commodity codes found for '{genus_species}' with EPPO code '{eppo_code}'")
                    else:
                        # Row has existing commodity code - don't generate options
                        commodity_options.append([])
                        logger.info(f"Preserving existing commodity code for '{genus_species}' - no dropdown options generated")
                else:
                    eppo_codes.append('')
                    commodity_options.append([])
                    logger.info(f"Enhanced IPAFFS lookup found no EPPO code for '{genus_species}'")
                    
            except Exception as e:
                logger.error(f"Error in enhanced IPAFFS lookup for '{genus_species}': {e}")
                eppo_codes.append('')
                commodity_options.append([])
        
        # Helper functions for flexible column matching
        def find_eppo_column(columns):
            """Find EPPO code column with flexible matching."""
            for column in columns:
                column_lower = column.lower().strip()
                # Check for exact matches and common variations
                if (column_lower == 'eppocode' or 
                    column_lower == 'eppo code' or 
                    column_lower == 'eppo_code' or 
                    column_lower == 'eppo-code' or
                    ('eppo' in column_lower and 'code' in column_lower)):
                    logger.info(f"Found EPPO column '{column}' using flexible match")
                    return column
            logger.info(f"No EPPO column found in: {columns}")
            return None
        
        def find_intended_users_column(columns):
            """Find intended users column with flexible matching."""
            for column in columns:
                column_lower = column.lower().strip()
                # Check for exact matches and common variations
                if (column_lower == 'intended for final users' or
                    column_lower == 'intended for final users (or commercial flower production)' or
                    column_lower == 'intended final users' or
                    column_lower == 'final users' or
                    column_lower == 'intended users' or
                    ('intended' in column_lower and ('final' in column_lower or 'users' in column_lower)) or
                    'commercial flower production' in column_lower):
                    logger.info(f"Found intended users column '{column}' using flexible match")
                    return column
            logger.info(f"No intended users column found in: {columns}")
            return None
        
        def find_controlled_atmosphere(columns):
            """Find intended users column with flexible matching."""
            for column in columns:
                column_lower = column.lower().strip()
                # Check for exact matches and common variations
                if (column_lower == 'controlled atmosphere container' or
                    column_lower == 'controlled atmosphere' or
                    ('controlled' in column_lower and ('atmosphere' in column_lower))):
                    logger.info(f"Found controlled atmosphere container '{column}' using flexible match")
                    return column
            logger.info(f"No intended users column found in: {columns}")
            return None

        def find_type_of_package(columns):
            """Find intended users column with flexible matching."""
            for column in columns:
                column_lower = column.lower().strip()
                # Check for exact matches and common variations
                if (column_lower == 'type of package'):
                    logger.info(f"Found controlled atmosphere container '{column}' using flexible match")
                    return column
            logger.info(f"No intended users column found in: {columns}")
            return None
        

        # Update the data with pre-filled values
        if is_array_format:
            if array_of_objects_field and array_of_objects_field in extracted_data:
                # Array of objects format from PDF - update the objects directly
                logger.info("Updating array of objects data in extracted_data")
                updated_extracted_data = extracted_data.copy()
                objects_array = updated_extracted_data[array_of_objects_field]
                
                # Get column names from the first object
                if objects_array and len(objects_array) > 0:
                    first_obj = objects_array[0]
                    object_columns = list(first_obj.keys())
                    
                    # Find EPPO code column
                    eppo_col = find_eppo_column(object_columns)
                    
                    # Find intended users column  
                    intended_col = find_intended_users_column(object_columns)

                    # Update controlled atmosphere only if empty or not present
                    controlled_atmosphere_col = find_controlled_atmosphere(object_columns)

                    # Find type of package column
                    type_of_package_col = find_type_of_package(object_columns)
                    
                    # Update each object in the array
                    for i, obj in enumerate(objects_array):
                        if i < len(eppo_codes):
                            # Update EPPO code only if it's empty or not present
                            if eppo_col:
                                current_eppo_value = obj.get(eppo_col, '')
                                if not current_eppo_value or str(current_eppo_value).strip() == '':
                                    obj[eppo_col] = eppo_codes[i]
                                    logger.info(f"Updated object {i} EPPO column '{eppo_col}' with '{eppo_codes[i]}'")
                                else:
                                    logger.info(f"Skipped object {i} EPPO column '{eppo_col}' - already has value: '{current_eppo_value}'")
                            else:
                                # Create new EPPO code field only if no existing EPPO field has data
                                has_existing_eppo = False
                                for key, value in obj.items():
                                    if 'eppo' in key.lower() and 'code' in key.lower() and value and str(value).strip():
                                        has_existing_eppo = True
                                        logger.info(f"Found existing EPPO data in field '{key}' for object {i}: '{value}'")
                                        break
                                
                                if not has_existing_eppo:
                                    obj['EPPO code'] = eppo_codes[i]
                                    logger.info(f"Created new EPPO code field for object {i} with '{eppo_codes[i]}'")
                                else:
                                    logger.info(f"Skipped creating EPPO code field for object {i} - existing EPPO data found")
                            
                            # Update intended users only if empty or not present
                            if intended_col:
                                current_value = obj.get(intended_col, '')
                                if not current_value or str(current_value).strip() == '':
                                    obj[intended_col] = 'Yes'
                                    logger.info(f"Updated object {i} intended column '{intended_col}' with 'Yes'")
                                else:
                                    logger.info(f"Skipped object {i} intended column '{intended_col}' - already has value: '{current_value}'")
                            else:
                                # Create new intended users field only if no existing intended field has data
                                has_existing_intended = False
                                for key, value in obj.items():
                                    if ('intended' in key.lower() and ('final' in key.lower() or 'users' in key.lower())) and value and str(value).strip():
                                        has_existing_intended = True
                                        logger.info(f"Found existing intended users data in field '{key}' for object {i}: '{value}'")
                                        break
                                
                                if not has_existing_intended:
                                    obj['Intended for final users'] = 'Yes'
                                    logger.info(f"Created new intended users field for object {i} with 'Yes'")
                                else:
                                    logger.info(f"Skipped creating intended users field for object {i} - existing data found")

                            
                            if controlled_atmosphere_col:  
                                current_value = obj.get(controlled_atmosphere_col, '')
                                if not current_value or str(current_value).strip() == '':
                                    obj[controlled_atmosphere_col] = 'No'
                                    logger.info(f"Updated object {i} controlled atmosphere column '{controlled_atmosphere_col}' with 'No'")
                                else:
                                    logger.info(f"Skipped object {i} controlled atmosphere column '{controlled_atmosphere_col}' - already has value: '{current_value}'")
                            else:
                                # Create new controlled atmosphere field only if no existing controlled atmosphere field has data
                                has_existing_controlled = False
                                for key, value in obj.items():
                                    if ('controlled' in key.lower() and 'atmosphere' in key.lower()) and value and str(value).strip():
                                        has_existing_controlled = True
                                        logger.info(f"Found existing controlled atmosphere data in field '{key}' for object {i}: '{value}'")
                                        break
                                
                                if not has_existing_controlled:
                                    obj['Controlled atmosphere container'] = 'No'
                                    logger.info(f"Created new controlled atmosphere field for object {i} with 'No'")
                                else:
                                    logger.info(f"Skipped creating controlled atmosphere field for object {i} - existing data found")


                            # Update type of package only if empty or not present
                            if type_of_package_col:
                                current_value = obj.get(type_of_package_col, '')
                                if not current_value or str(current_value).strip() == '':
                                    obj[type_of_package_col] = 'PK'
                                    logger.info(f"Updated object {i} type of package column '{type_of_package_col}' with 'PK'")
                                else:
                                    logger.info(f"Skipped object {i} type of package column '{type_of_package_col}' - already has value: '{current_value}'")
                            else:
                                # Create new type of package field only if no existing type of package field has data
                                has_existing_package = False
                                for key, value in obj.items():
                                    if ('type' in key.lower() and 'package' in key.lower()) and value and str(value).strip():
                                        has_existing_package = True
                                        logger.info(f"Found existing type of package data in field '{key}' for object {i}: '{value}'")
                                        break
                                
                                if not has_existing_package:
                                    obj['Type of package'] = 'PK'
                                    logger.info(f"Created new type of package field for object {i} with 'PK'")
                                else:
                                    logger.info(f"Skipped creating type of package field for object {i} - existing data found")
                
                # Update session with modified extracted_data
                session['extracted_data'] = updated_extracted_data
                logger.info("Updated session extracted_data with pre-filled IPAFFS data")
                
            else:
                # Multi-row format using target_columns/sample_data (fallback)
                logger.info("Updating multi-row format using target_columns/sample_data")
                updated_sample_data = sample_data.copy()
                
                # Find EPPO code column
                eppo_col = find_eppo_column(target_columns)
                
                if eppo_col:
                    # Only update EPPO codes where the existing value is empty
                    existing_eppo_data = updated_sample_data.get(eppo_col, [])
                    updated_eppo_data = []
                    for i, existing_value in enumerate(existing_eppo_data):
                        if not existing_value or str(existing_value).strip() == '':
                            # Use new EPPO code if available
                            new_value = eppo_codes[i] if i < len(eppo_codes) else ''
                            updated_eppo_data.append(new_value)
                            if new_value:
                                logger.info(f"Updated row {i} EPPO column '{eppo_col}' with '{new_value}'")
                        else:
                            # Keep existing value
                            updated_eppo_data.append(existing_value)
                            logger.info(f"Kept existing EPPO value for row {i}: '{existing_value}'")
                    updated_sample_data[eppo_col] = updated_eppo_data
                    logger.info(f"Updated existing EPPO column '{eppo_col}' preserving {len([v for v in existing_eppo_data if v and str(v).strip()])} existing values")
                else:
                    # Check if any existing column has EPPO-like data before creating new column
                    has_existing_eppo_data = False
                    for col_name, col_data in updated_sample_data.items():
                        if 'eppo' in col_name.lower() and 'code' in col_name.lower():
                            # Check if this column has any non-empty values
                            if any(value and str(value).strip() for value in col_data):
                                has_existing_eppo_data = True
                                logger.info(f"Found existing EPPO data in column '{col_name}' - skipping new EPPO column creation")
                                break
                    
                    if not has_existing_eppo_data:
                        # Create new EPPO code column
                        target_columns.append('EPPO code')
                        updated_sample_data['EPPO code'] = eppo_codes
                        logger.info(f"Created new EPPO code column with {len(eppo_codes)} codes")
                    else:
                        logger.info("Skipped creating new EPPO code column - existing EPPO data found")
                
                # Find commodity code column and preserve existing codes
                def find_commodity_code_column(columns):
                    for column in columns:
                        column_lower = column.lower().strip()
                        if ('commodity' in column_lower and 'code' in column_lower):
                            return column
                    return None
                
                commodity_code_col = find_commodity_code_column(target_columns)
                
                if commodity_code_col and commodity_code_col in updated_sample_data:
                    existing_commodity_data = updated_sample_data[commodity_code_col]
                    preserved_count = 0
                    for i, existing_value in enumerate(existing_commodity_data):
                        if existing_value and str(existing_value).strip():
                            preserved_count += 1
                            logger.info(f"Preserved existing commodity code for row {i}: '{existing_value}'")
                    logger.info(f"Preserved {preserved_count} existing commodity codes in column '{commodity_code_col}'")
                else:
                    logger.info("No commodity code column found for preservation")
                
                # Find intended users column
                intended_col = find_intended_users_column(target_columns)
                
                if intended_col:
                    intended_data = updated_sample_data.get(intended_col, [])
                    updated_count = 0
                    for i in range(len(intended_data)):
                        if not intended_data[i] or str(intended_data[i]).strip() == '':
                            intended_data[i] = 'Yes'
                            updated_count += 1
                        else:
                            logger.info(f"Kept existing intended users value for row {i}: '{intended_data[i]}'")
                    updated_sample_data[intended_col] = intended_data
                    logger.info(f"Updated existing intended users column '{intended_col}' - filled {updated_count} empty values with 'Yes'")
                else:
                    # Check if any existing column has intended users data before creating new column
                    has_existing_intended_data = False
                    for col_name, col_data in updated_sample_data.items():
                        if ('intended' in col_name.lower() and ('final' in col_name.lower() or 'users' in col_name.lower())):
                            # Check if this column has any non-empty values
                            if any(value and str(value).strip() for value in col_data):
                                has_existing_intended_data = True
                                logger.info(f"Found existing intended users data in column '{col_name}' - skipping new column creation")
                                break
                    
                    if not has_existing_intended_data:
                        # Create new column
                        target_columns.append('Intended for final users')
                        updated_sample_data['Intended for final users'] = ['Yes'] * len(genus_species_data)
                        logger.info(f"Created new intended users column with {len(genus_species_data)} 'Yes' values")
                    else:
                        logger.info("Skipped creating new intended users column - existing data found")

                controlled_atmosphere_col = find_controlled_atmosphere(target_columns)

                if controlled_atmosphere_col:
                    controlled_data = updated_sample_data.get(controlled_atmosphere_col, [])
                    updated_count = 0
                    for i in range(len(controlled_data)):
                        if not controlled_data[i] or str(controlled_data[i]).strip() == '':
                            controlled_data[i] = 'No'
                            updated_count += 1
                        else:
                            logger.info(f"Kept existing controlled atmosphere value for row {i}: '{controlled_data[i]}'")
                    updated_sample_data[controlled_atmosphere_col] = controlled_data
                    logger.info(f"Updated existing controlled atmosphere column '{controlled_atmosphere_col}' - filled {updated_count} empty values with 'No'")
                else:
                    # Check if any existing column has controlled atmosphere data before creating new column
                    has_existing_controlled_data = False
                    for col_name, col_data in updated_sample_data.items():
                        if ('controlled' in col_name.lower() and 'atmosphere' in col_name.lower()):
                            # Check if this column has any non-empty values
                            if any(value and str(value).strip() for value in col_data):
                                has_existing_controlled_data = True
                                logger.info(f"Found existing controlled atmosphere data in column '{col_name}' - skipping new column creation")
                                break
                    
                    if not has_existing_controlled_data:
                        # Create new column
                        target_columns.append('Controlled atmosphere container')
                        updated_sample_data['Controlled atmosphere container'] = ['No'] * len(genus_species_data)
                        logger.info(f"Created new controlled atmosphere column with {len(genus_species_data)} 'No' values")
                    else:
                        logger.info("Skipped creating new controlled atmosphere column - existing data found")

                type_of_package_col = find_type_of_package(target_columns)
                
                if type_of_package_col:
                    type_of_package_data = updated_sample_data.get(type_of_package_col, [])
                    updated_count = 0
                    for i in range(len(type_of_package_data)):
                        if not type_of_package_data[i] or str(type_of_package_data[i]).strip() == '':
                            type_of_package_data[i] = 'PK'
                            updated_count += 1
                        else:
                            logger.info(f"Kept existing type of package value for row {i}: '{type_of_package_data[i]}'")
                    updated_sample_data[type_of_package_col] = type_of_package_data
                    logger.info(f"Updated existing type of package column '{type_of_package_col}' - filled {updated_count} empty values with 'PK'")
                else:
                    # Check if any existing column has type of package data before creating new column
                    has_existing_package_data = False
                    for col_name, col_data in updated_sample_data.items():
                        if ('type' in col_name.lower() and 'package' in col_name.lower()):
                            # Check if this column has any non-empty values
                            if any(value and str(value).strip() for value in col_data):
                                has_existing_package_data = True
                                logger.info(f"Found existing type of package data in column '{col_name}' - skipping new column creation")
                                break
                    
                    if not has_existing_package_data:
                        # Create new column
                        target_columns.append('Type of package')
                        updated_sample_data['Type of package'] = ['PK'] * len(genus_species_data)
                        logger.info(f"Created new type of package column with {len(genus_species_data)} 'PK' values")
                    else:
                        logger.info("Skipped creating new type of package column - existing data found")
                
                # Update session
                session['target_columns'] = target_columns
                session['sample_data'] = updated_sample_data
            
        else:
            # Single row format
            updated_extracted_data = extracted_data.copy()
            
            # Update EPPO code only if empty or not present
            eppo_field = None
            for field in extracted_data.keys():
                if 'eppo' in field.lower() and 'code' in field.lower():
                    eppo_field = field
                    break
            
            if eppo_field:
                current_eppo_value = updated_extracted_data.get(eppo_field, '')
                if not current_eppo_value or str(current_eppo_value).strip() == '':
                    if len(eppo_codes) > 0:
                        updated_extracted_data[eppo_field] = eppo_codes[0] if len(eppo_codes) == 1 else eppo_codes
                        logger.info(f"Updated single row EPPO field '{eppo_field}' with value")
                else:
                    logger.info(f"Skipped single row EPPO field '{eppo_field}' - already has value: '{current_eppo_value}'")
            else:
                # Check if any field has EPPO-like data before creating new field
                has_existing_eppo = False
                for field, value in updated_extracted_data.items():
                    if 'eppo' in field.lower() and 'code' in field.lower() and value and str(value).strip():
                        has_existing_eppo = True
                        logger.info(f"Found existing EPPO data in field '{field}': '{value}'")
                        break
                
                if not has_existing_eppo and len(eppo_codes) > 0:
                    updated_extracted_data['EPPO code'] = eppo_codes[0] if len(eppo_codes) == 1 else eppo_codes
                    logger.info("Created new EPPO code field for single row")
                else:
                    logger.info("Skipped creating EPPO code field - existing data found or no EPPO codes available")
            
            # Pre-fill "Intended for final users" only if empty or not present
            intended_field = None
            for field in extracted_data.keys():
                if 'intended' in field.lower() and 'final' in field.lower():
                    intended_field = field
                    break
            
            if intended_field:
                current_value = updated_extracted_data.get(intended_field, '')
                if not current_value or str(current_value).strip() == '':
                    updated_extracted_data[intended_field] = 'Yes'
                    logger.info(f"Updated single row intended field '{intended_field}' with 'Yes'")
                else:
                    logger.info(f"Skipped single row intended field '{intended_field}' - already has value: '{current_value}'")
            else:
                # Check if any field has intended users data before creating new field
                has_existing_intended = False
                for field, value in updated_extracted_data.items():
                    if ('intended' in field.lower() and ('final' in field.lower() or 'users' in field.lower())) and value and str(value).strip():
                        has_existing_intended = True
                        logger.info(f"Found existing intended users data in field '{field}': '{value}'")
                        break
                
                if not has_existing_intended:
                    updated_extracted_data['Intended for final users'] = 'Yes'
                    logger.info("Created new intended users field for single row with 'Yes'")
                else:
                    logger.info("Skipped creating intended users field - existing data found")
            
            # Update session
            session['extracted_data'] = updated_extracted_data
        
        return jsonify({
            'success': True,
            'eppo_codes_added': len([code for code in eppo_codes if code]),
            'commodity_options': commodity_options,
            'is_array_format': is_array_format,
            'message': f'IPAFFS pre-fill completed. Added {len([code for code in eppo_codes if code])} EPPO codes.'
        })
    
    except Exception as e:
        logger.error(f"Error pre-filling IPAFFS data: {e}")
        return jsonify({'error': str(e)})

@app.route('/get_current_csv_data', methods=['GET'])
def get_current_csv_data():
    """Get the current CSV data from the session (for use after IPAFFS pre-fill)."""
    try:
        # Get current data from session
        extracted_data = session.get('extracted_data', {})
        target_columns = session.get('target_columns', [])
        sample_data = session.get('sample_data', {})
        
        logger.info(f"Getting current CSV data - extracted_data keys: {list(extracted_data.keys())}, target_columns: {len(target_columns)}")
        
        if extracted_data:
            # PDF mode - check for array of objects or single row
            array_of_objects_field = None
            for field, value in extracted_data.items():
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    array_of_objects_field = field
                    logger.info(f"Found array of objects in field '{field}' with {len(value)} objects")
                    break
            
            if array_of_objects_field:
                # Array of objects format
                objects_array = extracted_data[array_of_objects_field]
                
                # Extract all unique keys from all objects as column headers
                all_keys = set()
                for obj in objects_array:
                    if isinstance(obj, dict):
                        all_keys.update(obj.keys())
                
                headers = sorted(list(all_keys))
                
                # Create rows - each object becomes a row
                csv_data = {
                    'headers': headers,
                    'data': []
                }
                
                for obj in objects_array:
                    if isinstance(obj, dict):
                        row = {}
                        for header in headers:
                            value = obj.get(header, '')
                            if isinstance(value, (dict, list)):
                                value = json.dumps(value)
                            elif value is None:
                                value = ''
                            row[header] = str(value)
                        csv_data['data'].append(row)
                
                logger.info(f"Returning array of objects CSV data with {len(headers)} columns and {len(csv_data['data'])} rows")
                return jsonify({
                    'success': True,
                    'csv_data': csv_data,
                    'format': 'array_of_objects'
                })
            else:
                # Single row PDF format
                headers = list(extracted_data.keys())
                csv_data = {
                    'headers': headers,
                    'data': []
                }
                
                # Create a single row
                row = {}
                for field in headers:
                    value = extracted_data.get(field, '')
                    if isinstance(value, (list, dict)):
                        value = json.dumps(value)
                    elif value is None:
                        value = ''
                    row[field] = str(value)
                
                csv_data['data'].append(row)
                
                logger.info(f"Returning single row CSV data with {len(headers)} columns")
                return jsonify({
                    'success': True,
                    'csv_data': csv_data,
                    'format': 'single_row'
                })
        elif target_columns and sample_data:
            # Multi-row format (either Excel mode or converted PDF)
            headers = target_columns
            
            # Determine the maximum number of rows
            max_rows = 0
            for col in headers:
                col_data = sample_data.get(col, [])
                max_rows = max(max_rows, len(col_data))
            
            # Create rows
            csv_data = {
                'headers': headers,
                'data': []
            }
            
            for row_idx in range(max_rows):
                row = {}
                for col in headers:
                    col_data = sample_data.get(col, [])
                    row[col] = col_data[row_idx] if row_idx < len(col_data) else ''
                csv_data['data'].append(row)
            
            logger.info(f"Returning multi-row CSV data with {len(headers)} columns and {len(csv_data['data'])} rows")
            return jsonify({
                'success': True,
                'csv_data': csv_data,
                'format': 'multi_row'
            })
        else:
            return jsonify({'success': False, 'error': 'No data found in session'})
    
    except Exception as e:
        logger.error(f"Error getting current CSV data: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/update_commodity_selection', methods=['POST'])
def update_commodity_selection():
    """Update the commodity code selection for a specific row."""
    try:
        data = request.json
        row_index = data.get('row_index')
        commodity_code = data.get('commodity_code')
        display_text = data.get('display_text', '')
        
        if row_index is None:
            return jsonify({'error': 'Row index not provided'})
        
        # Get or create commodity selections in session
        commodity_selections = session.get('commodity_selections', {})
        
        # Store the selection (use string key for JSON serialization)
        commodity_selections[str(row_index)] = {
            'code': commodity_code,
            'display_text': display_text
        }
        
        # Update session
        session['commodity_selections'] = commodity_selections
        
        logger.info(f"Updated commodity selection for row {row_index}: {commodity_code} ({display_text})")
        
        return jsonify({
            'success': True,
            'message': f'Commodity selection updated for row {row_index}'
        })
        
    except Exception as e:
        logger.error(f"Error updating commodity selection: {e}")
        return jsonify({'error': str(e)})

@app.route('/batch_update_commodity_selections', methods=['POST'])
def batch_update_commodity_selections():
    """Update multiple commodity code selections in a single request."""
    try:
        data = request.json
        selections = data.get('selections', [])
        
        if not selections:
            return jsonify({'error': 'No selections provided'})
        
        # Get or create commodity selections in session
        commodity_selections = session.get('commodity_selections', {})
        
        # Process all selections
        updated_count = 0
        for selection in selections:
            row_index = selection.get('rowIndex')
            commodity_code = selection.get('selectedValue')
            display_text = selection.get('selectedText', '')
            
            if row_index is not None:
                # Store the selection (use string key for JSON serialization)
                commodity_selections[str(row_index)] = {
                    'code': commodity_code,
                    'display_text': display_text
                }
                updated_count += 1
                logger.info(f"Batch updated commodity selection for row {row_index}: {commodity_code} ({display_text})")
        
        # Update session with all changes
        session['commodity_selections'] = commodity_selections
        
        logger.info(f"Batch updated {updated_count} commodity selections successfully")
        
        return jsonify({
            'success': True,
            'message': f'Successfully updated {updated_count} commodity selections',
            'updated_count': updated_count
        })
        
    except Exception as e:
        logger.error(f"Error batch updating commodity selections: {e}")
        return jsonify({'error': str(e)})

@app.route('/validate_commodity_selections', methods=['POST'])
def validate_commodity_selections():
    """Validate that all required commodity selections are saved."""
    try:
        # Get current data to determine expected number of rows
        extracted_data = session.get('extracted_data', {})
        commodity_selections = session.get('commodity_selections', {})
        
        if not extracted_data:
            return jsonify({'valid': False, 'error': 'No data found for validation'})
        
        # Determine number of expected rows
        expected_rows = 0
        array_of_objects_field = None
        
        for field, value in extracted_data.items():
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                array_of_objects_field = field
                expected_rows = len(value)
                logger.info(f"Found {expected_rows} expected rows in array of objects field '{field}'")
                break
        
        if expected_rows == 0:
            # Single row or no array data - no validation needed
            return jsonify({'valid': True, 'message': 'No commodity selections required for validation'})
        
        # Check if we have commodity selections for all rows
        missing_rows = []
        for row_index in range(expected_rows):
            if str(row_index) not in commodity_selections:
                missing_rows.append(row_index)
        
        if missing_rows:
            logger.warning(f"Missing commodity selections for rows: {missing_rows}")
            return jsonify({
                'valid': False, 
                'error': f'Missing commodity selections for {len(missing_rows)} rows',
                'missing_rows': missing_rows
            })
        
        logger.info(f"All {expected_rows} commodity selections validated successfully")
        return jsonify({
            'valid': True, 
            'message': f'All {expected_rows} commodity selections are saved'
        })
        
    except Exception as e:
        logger.error(f"Error validating commodity selections: {e}")
        return jsonify({'valid': False, 'error': str(e)})

@app.route('/update_csv_preview', methods=['POST'])
def update_csv_preview():
    """Update the session with edited CSV data."""
    try:
        # Get data from request
        data = request.json
        csv_data = data.get('csv_data', {})
        
        if not csv_data:
            return jsonify({'error': 'Missing CSV data'})
        
        # Check if we're in PDF mode or Excel mode
        extracted_data = session.get('extracted_data')
        if extracted_data:
            # PDF mode - but now handle multiple rows properly
            headers = csv_data.get('headers', [])
            data_rows = csv_data.get('data', [])
            
            logger.info(f"Updating PDF data with {len(headers)} fields and {len(data_rows)} rows")
            
            if data_rows:
                if len(data_rows) == 1:
                    # Single row - use original logic for backward compatibility
                    updated_data = {}
                    first_row = data_rows[0]
                    
                    for field in headers:
                        value = first_row.get(field, '')
                        # Try to parse JSON strings back to objects if they were originally complex
                        if isinstance(value, str) and (value.startswith('{') or value.startswith('[')):
                            try:
                                value = json.loads(value)
                            except (json.JSONDecodeError, ValueError):
                                # If parsing fails, keep as string
                                pass
                        updated_data[field] = value
                    
                    # Update the session with single row data
                    session['extracted_data'] = updated_data
                else:
                    # Multiple rows - convert to Excel-like format
                    logger.info(f"Converting PDF data to multi-row format with {len(data_rows)} rows")
                    
                    # Create sample_data format like Excel mode
                    new_sample_data = {}
                    for col in headers:
                        column_data = []
                        for row in data_rows:
                            value = row.get(col, '')
                            # Try to parse JSON strings back to objects if they were originally complex
                            if isinstance(value, str) and (value.startswith('{') or value.startswith('[')):
                                try:
                                    value = json.loads(value)
                                except (json.JSONDecodeError, ValueError):
                                    pass
                            column_data.append(value)
                        new_sample_data[col] = column_data
                    
                    # Switch to Excel-like mode for multi-row data
                    session['target_columns'] = headers
                    session['sample_data'] = new_sample_data
                    
                    # Clear the single-row extracted_data since we're now in multi-row mode
                    session['extracted_data'] = {}
                    
                    logger.info(f"PDF data converted to Excel-like format with {len(new_sample_data)} columns and {len(data_rows)} rows")
            
            return jsonify({
                'success': True,
                'message': 'PDF data updated successfully'
            })
        else:
            # Excel mode - use existing logic
            target_columns = session.get('target_columns', [])
            sample_data = session.get('sample_data', {})
            suggested_data = session.get('suggested_data', {})
            
            if not target_columns:
                return jsonify({'error': 'No active analysis session found'})
            
            # Convert edited CSV data to sample_data format
            headers = csv_data.get('headers', [])
            data_rows = csv_data.get('data', [])
            
            # Log the state of data before update
            logger.info(f"Updating CSV preview with {len(headers)} columns and {len(data_rows)} rows")
            logger.debug(f"Headers before update: {list(sample_data.keys())}")
            
            # Create a completely new sample_data dictionary, effectively replacing
            # the old one rather than merging with it. This ensures renamed columns
            # don't appear twice.
            new_sample_data = {}
            
            for col in headers:
                column_data = []
                for row in data_rows:
                    if col in row:
                        column_data.append(row[col])
                new_sample_data[col] = column_data
                logger.debug(f"Column {col} updated with {len(column_data)} values")
            
            # Replace target_columns list with the new headers to make sure
            # everything stays in sync when columns are renamed
            session['target_columns'] = headers
            
            # Update session data with edited values - completely replacing old data
            session['sample_data'] = new_sample_data
            logger.info(f"Session sample_data updated successfully with {len(new_sample_data)} columns")
            
            return jsonify({
                'success': True,
                'message': 'CSV preview updated successfully'
            })
    
    except Exception as e:
        logger.error(f"Error updating CSV preview: {e}")
        return jsonify({'error': str(e)})


@app.teardown_appcontext
def cleanup_temp_files(exception=None):
    """Clean up temporary files when the app context is torn down."""
    try:
        # Only try to access session if we're in a request context
        from flask import has_request_context, session
        if has_request_context():
            temp_file_path = session.get('temp_file_path')
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except Exception as e:
                    logger.error(f"Error cleaning up temporary file: {e}")
    except Exception as e:
        logger.error(f"Error in cleanup_temp_files: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
