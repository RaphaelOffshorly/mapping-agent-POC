import os
import json
import logging
from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
import tempfile
from dotenv import load_dotenv
import pandas as pd

from config.config import Config
from utils.excel import get_excel_preview
from workflow import run_workflow

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
    
    # Construct results
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
        # Get data directly from session keys
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
