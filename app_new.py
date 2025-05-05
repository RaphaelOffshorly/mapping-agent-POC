import os
import json
import logging
import subprocess
import time
import numpy as np
from langchain_core.messages import AIMessage
from agents.csv_edit_agent import CSVEditAgent
import tempfile
import os
from langchain_core.messages import HumanMessage
from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
import tempfile
from dotenv import load_dotenv
import pandas as pd

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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/pdf_upload')
def pdf_upload():
    """Route for the PDF upload page"""
    return render_template('pdf_upload.html')

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

@app.route('/save_schema', methods=['POST'])
def save_schema():
    """API endpoint to save an edited schema"""
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
        # Get the schema path from session
        schema_filepath = session.get('schema_filepath')
        if not schema_filepath or not os.path.exists(schema_filepath):
            return jsonify({'error': 'No schema found. Please generate a schema first.'})

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

        # Store the extracted data in session
        session['filename'] = pdf_filename
        session['pdf_filepath'] = pdf_filepath
        session['extracted_data'] = extracted_data
        session['schema_filepath'] = schema_filepath

        # Get the schema content
        with open(schema_filepath, 'r') as f:
            schema_content = json.load(f)
        
        # Generate column descriptions from schema
        column_descriptions = {}
        if 'properties' in schema_content:
            for field, props in schema_content['properties'].items():
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
            sheet_name = excel_file.sheet_names[0]
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=None)

        # Try to find the header row
        from utils.common import infer_header_row
        header_index = infer_header_row(df)
        if header_index is not None:
            # Extract headers from the inferred header row
            headers = df.iloc[header_index].astype(str).tolist()
            target_columns = [h.strip() for h in headers if h.strip()]
        else:
            # If no header row found, use the first row
            headers = df.iloc[0].astype(str).tolist()
            target_columns = [h.strip() for h in headers if h.strip()]

        # Clean up
        os.remove(filepath)

        return jsonify({'target_columns': target_columns})
    except Exception as e:
        logger.error(f"Error getting target columns: {e}")
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
                
                # Process the single-sheet file
                results = run_workflow(temp_sheet_path, target_columns_list, skip_suggestion=True)
                
                # Clean up
                os.remove(temp_sheet_path)
            else:
                return jsonify({'error': f'Sheet "{sheet_name}" not found in the Excel file'})
        else:
            # Process the entire file
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
        
        # Run the workflow for just this target column
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
        
        # Run the workflow for just this target column
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
        
        # Run the workflow for just this target column
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
        
        # Run the workflow
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
            # PDF mode - export the extracted data
            from io import StringIO
            import csv
            
            output = StringIO()
            writer = csv.writer(output)
            
            # Write header row with field names
            fields = list(extracted_data.keys())
            writer.writerow(fields)
            
            # Write a single row with all values
            row_data = []
            for field in fields:
                value = extracted_data.get(field, '')
                # Convert complex values to string
                if isinstance(value, (list, dict)):
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
        
        # Get data from session
        target_columns = session.get('target_columns', [])
        sample_data = session.get('sample_data', {})
        suggested_data = session.get('suggested_data', {})
        
        if not target_columns:
            return jsonify({'error': 'No active analysis session found'})
        
        logger.info(f"Getting CSV data for {len(target_columns)} columns")
        
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
    
    except Exception as e:
        logger.error(f"Error getting CSV data: {e}")
        return jsonify({'error': str(e)})

@app.route('/chat_with_csv_editor', methods=['POST'])
def chat_with_csv_editor():
    """Handle chatbot interactions for CSV editing."""
    try:
        # Get data from request
        data = request.json
        message = data.get('message', '')
        csv_data = data.get('csv_data', {})
        source_data = data.get('source_data', {})
        print(csv_data)
        if not source_data:
            source_data = {}
            logger.info("No source data provided, using empty dict")
        
        if not message or not csv_data:
            logger.error("Missing required parameters in chat_with_csv_editor")
            return jsonify({'success': False, 'error': 'Missing required parameters'})
        
        # Initialize logging
        logger.info(f"Chat message received: {message[:50]}...")
        
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
            # Create a message
            from langchain_core.messages import HumanMessage
            user_message = HumanMessage(content=message)
            
            # Convert csv_data to DataFrame and save to temp file
            import pandas as pd
            import tempfile
            import os
            import json
            
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
            
            # Prepare the initial state
            state = {
                'messages': [user_message],
                'csv_file_path': csv_file_path,
                'source_data': source_data
            }
            
            # Run the agent
            logger.info("Running CSV Edit Supervisor Agent")
            result = agent.run(state)
            logger.info("CSV Edit Supervisor Agent completed")
            
            # Process the response by extracting messages from named agents
            from langchain_core.messages import AIMessage
            
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
            
            # Clean up the temporary file
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
                'csv_data_changed': csv_data_changed
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

@app.route('/update_csv_preview', methods=['POST'])
def update_csv_preview():
    """Update the session with edited CSV data."""
    try:
        # Get data from request
        data = request.json
        csv_data = data.get('csv_data', {})
        
        if not csv_data:
            return jsonify({'error': 'Missing CSV data'})
        
        # Get current session data
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
        
        new_sample_data = {}
        
        for col in headers:
            column_data = []
            for row in data_rows:
                if col in row:
                    column_data.append(row[col])
            new_sample_data[col] = column_data
            logger.debug(f"Column {col} updated with {len(column_data)} values")
        
        # Update session data with edited values
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
