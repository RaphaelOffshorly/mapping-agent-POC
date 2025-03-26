import os
import json
import pandas as pd
from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
from langchain_anthropic import ChatAnthropic
import tempfile
from dotenv import load_dotenv
import logging

from utils.common import infer_header_row

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default-secret-key')
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['SESSION_TYPE'] = 'filesystem'  # Use filesystem session storage
app.config['SESSION_FILE_DIR'] = os.path.join(tempfile.gettempdir(), 'flask_sessions')
app.config['SESSION_PERMANENT'] = False
app.config['PERMANENT_SESSION_LIFETIME'] = 3600  # 1 hour

# Create session directory if it doesn't exist
os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)

# Initialize session extension
from flask_session import Session
Session(app)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Make sure you have your Anthropic API key in your .env file
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')


class ExcelHeaderMatcher:
    def __init__(self, model_name="claude-3-sonnet-20240229", header_scan_rows=10, cell_scan_rows=30, cell_scan_cols=10):
        self.model = ChatAnthropic(model=model_name, api_key=ANTHROPIC_API_KEY)
        self.header_scan_rows = header_scan_rows  # Number of rows to consider when inferring header
        self.cell_scan_rows = cell_scan_rows      # Number of rows to scan for cell-level heuristics
        self.cell_scan_cols = cell_scan_cols      # Number of columns to scan for cell-level heuristics

    def _infer_header_row(self, df):
        """
        Infer the most likely header row index in the DataFrame by computing the ratio of non-numeric,
        non-empty cells for the first few rows.
        """
        best_row = None
        best_score = 0
        for i in range(min(self.header_scan_rows, len(df))):
            row = df.iloc[i, :]
            # Count cells that are strings and not purely numeric or empty
            valid_cells = [val for val in row if isinstance(val, str) and val.strip() and not val.strip().isdigit()]
            score = len(valid_cells) / len(row) if len(row) > 0 else 0
            if score > best_score:
                best_score = score
                best_row = i
        return best_row

    def _extract_from_inferred_header(self, df, header_index):
        """
        Extract headers from the inferred header row.
        """
        if header_index is not None:
            header_values = df.iloc[header_index].astype(str).tolist()
            return [val.strip() for val in header_values if val.strip()]
        return []

    def extract_all_potential_headers(self, file_path):
        """
        Comprehensive extraction of potential headers from any Excel format.
        This method now uses a single read (without header) and then applies heuristics.
        """
        all_texts = []

        try:
            excel_file = pd.ExcelFile(file_path)
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                logging.info(f"Processing sheet: {sheet_name}")

                # Heuristic 1: Infer header row based on string density
                inferred_index = self._infer_header_row(df)
                inferred_headers = self._extract_from_inferred_header(df, inferred_index)
                all_texts.extend(inferred_headers)

                # Heuristic 2: Consider the first few rows as potential header rows
                for i in range(min(5, len(df))):
                    row_texts = [str(val).strip() for val in df.iloc[i, :] if pd.notna(val)]
                    all_texts.extend(row_texts)

                # Heuristic 3: First column might contain labels
                first_col_texts = [str(df.iloc[i, 0]).strip() for i in range(len(df)) if pd.notna(df.iloc[i, 0])]
                all_texts.extend(first_col_texts)

                # Heuristic 4: Look for cells that use a colon or equals sign as label indicators
                for i in range(min(self.cell_scan_rows, len(df))):
                    for j in range(min(self.cell_scan_cols, df.shape[1])):
                        cell_val = df.iloc[i, j]
                        if pd.notna(cell_val) and isinstance(cell_val, str):
                            text = cell_val.strip()
                            if ':' in text or '=' in text:
                                # Extract text before the colon or equals sign
                                delimiter = ':' if ':' in text else '='
                                label_part = text.split(delimiter)[0].strip()
                                if label_part:
                                    all_texts.append(label_part)

        except Exception as e:
            logging.error(f"Error processing Excel file: {e}")
            return {"error": str(e)}

        # Clean and filter extracted texts
        cleaned_texts = []
        for text in all_texts:
            text = str(text).strip()
            if (text and 2 <= len(text) <= 50 and not text.isdigit() and text.lower() != "nan"):
                cleaned_texts.append(text)

        # Remove duplicates while preserving order
        seen = set()
        unique_texts = [x for x in cleaned_texts if x not in seen and not seen.add(x)]

        return unique_texts

    def describe_target_columns(self, target_columns):
        """
        Use the Anthropic model to describe the possible data for each target column.
        Returns a dictionary with descriptions and sample data examples for each target column.
        """
        prompt = f"""
I need descriptions for these target column names that might appear in an Excel file:

{json.dumps(target_columns, indent=2)}

For each target column, provide:
1. A brief description of what kind of data this column typically contains
2. The expected data type (text, number, date, etc.)
3. 3-5 realistic sample values that might appear in this column

Return ONLY a JSON object with this structure:
{{
    "target_column_name": {{
        "description": "Brief description of what this column contains",
        "data_type": "text|number|date|boolean|etc",
        "sample_values": ["example1", "example2", "example3"]
    }},
    ...
}}
        """
        
        response = self.model.invoke(prompt)
        try:
            content = response.content
            import re
            json_pattern = r'\{[\s\S]*\}'
            json_match = re.search(json_pattern, content)
            if json_match:
                json_str = json_match.group()
                # Clean JSON string: remove trailing commas, fix unbalanced braces/quotes if necessary
                json_str = re.sub(r',\s*([\]}])', r'\1', json_str).strip()
                missing_braces = json_str.count("{") - json_str.count("}")
                if missing_braces > 0:
                    json_str += "}" * missing_braces
                if len(re.findall(r'(?<!\\)"', json_str)) % 2 != 0:
                    json_str += '"'
                return json.loads(json_str)
            else:
                logging.error("Could not find valid JSON in the response")
                return {}
        except Exception as e:
            logging.error(f"Error parsing response: {e}")
            logging.error(f"Raw response: {response.content}")
            return {}

    def match_headers(self, potential_headers, target_columns):
        """
        Use the Anthropic model to match potential headers to target columns.
        The prompt instructs the model to consider various matching criteria.
        """
        # Split target columns into smaller batches to avoid exceeding model context limits
        batch_size = 20
        target_batches = [target_columns[i:i + batch_size] for i in range(0, len(target_columns), batch_size)]
        
        all_matches = {}
        
        for batch in target_batches:
            prompt = f"""
I have extracted these potential headers or labels from an Excel file:

{json.dumps(potential_headers, indent=2)}

I need to find matches for these target column names:

{json.dumps(batch, indent=2)}

For each target column, find the best matching header from the Excel file.
Consider exact matches, semantic similarity, abbreviations, and partial matches.

Return ONLY a JSON object with this structure:
{{
    "target_column_name": {{
        "match": "best_matching_header_from_excel",
        "confidence": "high|medium|low"
    }},
    ...
}}

If no good match exists for a target column, use "No match found" as the value for "match".
            """
            response = self.model.invoke(prompt)
            try:
                content = response.content
                import re
                json_pattern = r'\{[\s\S]*\}'
                json_match = re.search(json_pattern, content)
                if json_match:
                    json_str = json_match.group()
                    # Clean JSON string: remove trailing commas, fix unbalanced braces/quotes if necessary
                    json_str = re.sub(r',\s*([\]}])', r'\1', json_str).strip()
                    missing_braces = json_str.count("{") - json_str.count("}")
                    if missing_braces > 0:
                        json_str += "}" * missing_braces
                    if len(re.findall(r'(?<!\\)"', json_str)) % 2 != 0:
                        json_str += '"'
                    batch_matches = json.loads(json_str)
                    
                    # Add batch matches to all_matches
                    all_matches.update(batch_matches)
                else:
                    logging.error("Could not find valid JSON in the response")
                    # Add default "No match found" for all columns in this batch
                    for target in batch:
                        all_matches[target] = {
                            "match": "No match found",
                            "confidence": "low"
                        }
            except Exception as e:
                logging.error(f"Error parsing response: {e}")
                logging.error(f"Raw response: {response.content}")
                # Add default "No match found" for all columns in this batch
                for target in batch:
                    all_matches[target] = {
                        "match": "No match found",
                        "confidence": "low"
                    }
        
        # Ensure all target columns have a match entry
        for target in target_columns:
            if target not in all_matches:
                all_matches[target] = {
                    "match": "No match found",
                    "confidence": "low"
                }
        
        return all_matches

    def extract_sample_data(self, file_path, header_name, max_rows=5):
        """
        Extract sample data for a given header from the Excel file.
        Returns a list of sample values.
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            samples = []
            
            # Try to find the header in each sheet
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                # First try to find the header in the inferred header row
                inferred_index = self._infer_header_row(df)
                if inferred_index is not None:
                    header_row = df.iloc[inferred_index]
                    for col_idx, col_name in enumerate(header_row):
                        if str(col_name).strip() == header_name:
                            # Extract sample data from this column
                            data_start_row = inferred_index + 1
                            if data_start_row < len(df):
                                column_data = df.iloc[data_start_row:data_start_row+max_rows, col_idx].tolist()
                                samples = [str(val) for val in column_data if pd.notna(val)]
                                if samples:
                                    return samples
                
                # If not found in header row, search the entire sheet
                for i in range(min(20, len(df))):
                    for j in range(min(20, df.shape[1])):
                        if str(df.iloc[i, j]).strip() == header_name:
                            # Found the header, extract data below or to the right
                            # Try below first (more common)
                            if i + 1 < len(df):
                                column_data = df.iloc[i+1:i+1+max_rows, j].tolist()
                                samples = [str(val) for val in column_data if pd.notna(val)]
                                if samples:
                                    return samples
                            
                            # Try to the right if no data found below
                            if not samples and j + 1 < df.shape[1]:
                                row_data = df.iloc[i, j+1:j+1+max_rows].tolist()
                                samples = [str(val) for val in row_data if pd.notna(val)]
                                if samples:
                                    return samples
            
            return samples or ["No sample data found"]
        except Exception as e:
            logging.error(f"Error extracting sample data: {e}")
            return ["Error extracting sample data"]

    def get_excel_preview(self, file_path):
        """
        Get a preview of the Excel file for display in the UI.
        Returns a dictionary with sheet data.
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            preview = {}
            
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                # Get dimensions
                rows, cols = df.shape
                
                # Extract all data without limiting rows/columns
                preview_data = []
                for i in range(rows):
                    row_data = []
                    for j in range(cols):
                        val = df.iloc[i, j]
                        # Preserve formatting better
                        if pd.isna(val):
                            row_data.append("")
                        elif isinstance(val, (int, float)) and val == int(val):
                            # Format integers without decimal points
                            row_data.append(str(int(val)))
                        else:
                            row_data.append(str(val))
                    preview_data.append(row_data)
                
                # Add row numbers for all rows
                row_numbers = [str(i+1) for i in range(rows)]
                
                preview[sheet_name] = {
                    "data": preview_data,
                    "row_numbers": row_numbers,
                    "total_rows": rows,
                    "total_cols": cols
                }
            
            return preview
        except Exception as e:
            logging.error(f"Error generating Excel preview: {e}")
            return {"error": str(e)}

    def suggest_header_for_target(self, file_path, target_column):
        """
        Use the Anthropic model to suggest a header for a specific target column based on the Excel content.
        Returns a suggested header string.
        """
        # First, extract all potential headers and text from the Excel file
        all_potential_headers = self.extract_all_potential_headers(file_path)
        
        # Get a preview of the Excel file to analyze
        excel_preview = self.get_excel_preview(file_path)
        
        # Get column description if available
        column_description = None
        try:
            column_description = self.describe_target_columns([target_column])[target_column]
        except:
            pass
        
        # Prepare data for the prompt
        prompt_data = []
        for sheet_name, sheet_data in excel_preview.items():
            # Only include the first sheet or limit to 2 sheets to avoid context length issues
            if len(prompt_data) >= 2:
                break
                
            # Get the data from this sheet
            rows = sheet_data.get('data', [])
            
            # Limit to first 30 rows to avoid context length issues
            sample_rows = rows[:30]
            
            # Add this sheet's data to the prompt
            prompt_data.append({
                'sheet_name': sheet_name,
                'rows': sample_rows
            })
        
        # Create the prompt for Claude
        prompt = f"""
I need you to select the most appropriate header from the Excel file for a target column named "{target_column}".

IMPORTANT INSTRUCTIONS:
1. You MUST select a header from the list of potential headers extracted from the Excel file
2. DO NOT invent or suggest headers that are not in the list of potential headers
3. Select the header that best matches the target column description and data patterns

Here are all the potential headers extracted from the Excel file:
{json.dumps(all_potential_headers, indent=2)}

"""
        # Add column description if available
        if column_description:
            prompt += f"""
Target column description:
- Description: {column_description.get('description', 'Not available')}
- Data type: {column_description.get('data_type', 'Not available')}
- Expected sample values: {json.dumps(column_description.get('sample_values', []), indent=2)}

"""
        
        prompt += """
Here's the Excel data:
"""
        
        # Add the Excel data to the prompt
        for sheet_data in prompt_data:
            prompt += f"\nSheet: {sheet_data['sheet_name']}\n"
            
            # Add all rows
            for i, row in enumerate(sheet_data['rows']):
                row_str = ", ".join(row)
                if len(row_str) > 1000:  # Truncate very long rows
                    row_str = row_str[:1000] + "..."
                prompt += f"Row {i+1}: {row_str}\n"
        
        prompt += f"""
Based on the Excel data shown above, the list of potential headers, and the description of the target column "{target_column}", please:

1. Select the most appropriate header from the list of potential headers
2. Choose the header that best matches the target column description and data patterns
3. If multiple headers could work, choose the one that most accurately represents the data

IMPORTANT: You MUST select a header from the list of potential headers provided. Do not suggest any header that is not in this list.

Return ONLY a single string with your selected header name. Do not include any explanations or additional text.
"""
        
        # Call the model
        response = self.model.invoke(prompt)
        
        try:
            # Extract just the suggested header name from the response
            suggested_header = response.content.strip()
            
            # Remove any quotes or formatting that might be present
            suggested_header = suggested_header.strip('"\'')
            
            return suggested_header
        except Exception as e:
            logging.error(f"Error parsing header suggestion response: {e}")
            logging.error(f"Raw response: {response.content}")
            return f"Error suggesting header for {target_column}"
    
    def suggest_headers(self, file_path):
        """
        Use the Anthropic model to suggest headers based on the Excel content.
        Returns a dictionary with suggested headers for each column.
        """
        # Get a preview of the Excel file to analyze
        excel_preview = self.get_excel_preview(file_path)
        
        # Prepare data for the prompt
        prompt_data = []
        for sheet_name, sheet_data in excel_preview.items():
            # Only include the first sheet or limit to 2 sheets to avoid context length issues
            if len(prompt_data) >= 2:
                break
                
            # Get the data from this sheet
            rows = sheet_data.get('data', [])
            
            # Limit to first 20 rows to avoid context length issues
            sample_rows = rows[:20]
            
            # Add this sheet's data to the prompt
            prompt_data.append({
                'sheet_name': sheet_name,
                'rows': sample_rows
            })
        
        # Create the prompt for Claude
        prompt = f"""
I need you to analyze this Excel data and suggest appropriate column headers based on the content.
For each column, suggest a descriptive header that accurately represents the data.

Here's the Excel data:
"""
        
        # Add the Excel data to the prompt
        for sheet_data in prompt_data:
            prompt += f"\nSheet: {sheet_data['sheet_name']}\n"
            
            # Get the number of columns from the first row
            if sheet_data['rows'] and len(sheet_data['rows']) > 0:
                num_columns = len(sheet_data['rows'][0])
                
                # Add column data
                for col_idx in range(num_columns):
                    column_values = []
                    
                    # Get values for this column from each row
                    for row in sheet_data['rows']:
                        if col_idx < len(row):
                            value = row[col_idx]
                            if value and value.strip():
                                column_values.append(value)
                    
                    # Add column data to prompt
                    prompt += f"\nColumn {col_idx + 1} values: {column_values[:10]}"
                    if len(column_values) > 10:
                        prompt += f" (and {len(column_values) - 10} more values)"
        
        prompt += """

Based on the data shown above, suggest appropriate headers for each column.
Return ONLY a JSON object with this structure:
{
    "column_1": {
        "suggested_header": "Header Name",
        "confidence": "high|medium|low",
        "reasoning": "Brief explanation of why this header fits the data"
    },
    "column_2": {
        ...
    },
    ...
}

Use the column numbers as they appear in the data (Column 1, Column 2, etc.).
"""
        
        # Call the model
        response = self.model.invoke(prompt)
        
        try:
            content = response.content
            import re
            json_pattern = r'\{[\s\S]*\}'
            json_match = re.search(json_pattern, content)
            if json_match:
                json_str = json_match.group()
                # Clean JSON string
                json_str = re.sub(r',\s*([\]}])', r'\1', json_str).strip()
                missing_braces = json_str.count("{") - json_str.count("}")
                if missing_braces > 0:
                    json_str += "}" * missing_braces
                if len(re.findall(r'(?<!\\)"', json_str)) % 2 != 0:
                    json_str += '"'
                return json.loads(json_str)
            else:
                logging.error("Could not find valid JSON in the response")
                return {}
        except Exception as e:
            logging.error(f"Error parsing response: {e}")
            logging.error(f"Raw response: {response.content}")
            return {}
    
    def process_excel_file(self, file_path, target_columns):
        """
        Process an Excel file and match headers to target columns.
        Returns both the potential headers extracted and the match results.
        """
        potential_headers = self.extract_all_potential_headers(file_path)
        if potential_headers and not isinstance(potential_headers, dict):
            # Get descriptions for target columns
            column_descriptions = self.describe_target_columns(target_columns)
            
            # Match headers to target columns
            matches = self.match_headers(potential_headers, target_columns)
            
            # Extract sample data for each matched header
            sample_data = {}
            for target, info in matches.items():
                if info["match"] != "No match found":
                    # Extract actual sample data from the file
                    sample_data[target] = self.extract_sample_data(file_path, info["match"])
                elif target in column_descriptions and "sample_values" in column_descriptions[target]:
                    # If no match found, use the AI-generated sample values as fallback
                    sample_data[target] = column_descriptions[target]["sample_values"]
            
            # Get Excel preview for the UI
            excel_preview = self.get_excel_preview(file_path)
            
            # Get AI-suggested headers
            suggested_headers = self.suggest_headers(file_path)
            
            return {
                "potential_headers": potential_headers,
                "matches": matches,
                "sample_data": sample_data,
                "column_descriptions": column_descriptions,
                "excel_preview": excel_preview,
                "suggested_headers": suggested_headers
            }
        else:
            if isinstance(potential_headers, dict) and "error" in potential_headers:
                return potential_headers
            return {"error": "No potential headers found in the Excel file"}


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
        logging.error(f"Error getting sheet names: {e}")
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
        logging.error(f"Error getting target columns: {e}")
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
            logging.error(f"Error processing target file: {e}")
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

        matcher = ExcelHeaderMatcher(
            model_name="claude-3-sonnet-20240229",
            header_scan_rows=20,    # Scan first 20 rows to find a possible header row
            cell_scan_rows=50,      # Scan first 50 rows for colon/equal heuristics
            cell_scan_cols=50       # Scan first 50 columns
        )

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
                
                # Process the single-sheet file
                results = matcher.process_excel_file(temp_sheet_path, target_columns_list)
                
                # Clean up
                os.remove(temp_sheet_path)
            else:
                return jsonify({'error': f'Sheet "{sheet_name}" not found in the Excel file'})
        else:
            # Process the entire file
            results = matcher.process_excel_file(filepath, target_columns_list)

        # Store the uploaded file temporarily for re-analysis
        temp_file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{filename}")
        file.seek(0)  # Reset file pointer to beginning
        file.save(temp_file_path)
        
        # Store only essential data in session to avoid large cookie size
        session['filename'] = file.filename
        session['target_columns'] = target_columns_list
        session['temp_file_path'] = temp_file_path
        session['potential_headers'] = results['potential_headers']
        session['matches'] = results['matches']
        session['sample_data'] = results.get('sample_data', {})
        session['column_descriptions'] = results.get('column_descriptions', {})
        session['suggested_headers'] = results.get('suggested_headers', {})
        # Don't store excel_preview in session as it's too large

        return jsonify({
            'success': True,
            'redirect': '/results'
        })
    except Exception as e:
        logging.error(f"Error during file processing: {e}")
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
    
    # Get AI-suggested headers if not already in session
    suggested_headers = {}
    temp_file_path = session.get('temp_file_path')
    
    if temp_file_path and os.path.exists(temp_file_path):
        # Check if we need to generate suggested headers
        if 'suggested_headers' not in session or not session['suggested_headers']:
            try:
                # Create a matcher instance and get suggested headers
                matcher = ExcelHeaderMatcher(model_name="claude-3-sonnet-20240229")
                suggested_headers = matcher.suggest_headers(temp_file_path)
                
                # Store in session for future use
                session['suggested_headers'] = suggested_headers
            except Exception as e:
                logging.error(f"Error generating suggested headers: {e}")
        else:
            # Use cached suggested headers from session
            suggested_headers = session['suggested_headers']
    
    # Construct results without the excel_preview
    results = {
        'potential_headers': potential_headers,
        'matches': matches,
        'sample_data': sample_data,
        'column_descriptions': column_descriptions,
        'suggested_headers': suggested_headers
    }
    
    return render_template(
        'results.html',
        results=results,
        filename=filename,
        target_columns=target_columns
    )


@app.route('/get_excel_preview', methods=['GET'])
def get_excel_preview():
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
        logging.error(f"Error generating Excel preview: {e}")
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
        logging.error(f"Error adding header: {e}")
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
        
        # Re-match only the specified target column
        matcher = ExcelHeaderMatcher(model_name="claude-3-sonnet-20240229")
        
        # Create a list with just the one target column
        single_target = [target_column]
        
        # Match just this target against all potential headers
        match_result = matcher.match_headers(potential_headers, single_target)
        
        if 'error' in match_result:
            return jsonify({'error': match_result['error']})
        
        # Update just this target in the session
        matches[target_column] = match_result[target_column]
        session['matches'] = matches
        
        return jsonify({'success': True})
    
    except Exception as e:
        logging.error(f"Error re-matching: {e}")
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
        
        if not temp_file_path:
            return jsonify({'error': 'No active analysis session found'})
        
        if not os.path.exists(temp_file_path):
            return jsonify({'error': 'Temporary file no longer available'})
        
        # Create a matcher instance
        matcher = ExcelHeaderMatcher()
        
        # Get AI-suggested header for this target column
        suggested_header = matcher.suggest_header_for_target(temp_file_path, target_column)
        
        # Store the suggested header in the session
        ai_suggested_headers = session.get('ai_suggested_headers', {})
        ai_suggested_headers[target_column] = suggested_header
        session['ai_suggested_headers'] = ai_suggested_headers
        
        return jsonify({
            'success': True,
            'suggested_header': suggested_header,
            'target_column': target_column
        })
    
    except Exception as e:
        logging.error(f"Error suggesting header: {e}")
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
        
        # Re-match all target columns with the current potential headers
        matcher = ExcelHeaderMatcher(model_name="claude-3-sonnet-20240229")
        
        # Get new column descriptions
        column_descriptions = matcher.describe_target_columns(target_columns)
        
        # Match headers to target columns
        match_result = matcher.match_headers(potential_headers, target_columns)
        
        if 'error' in match_result:
            return jsonify({'error': match_result['error']})
        
        # Update the session with new matches and descriptions
        session['matches'] = match_result
        session['column_descriptions'] = column_descriptions
        
        return jsonify({'success': True})
    
    except Exception as e:
        logging.error(f"Error re-analyzing: {e}")
        return jsonify({'error': str(e)})


@app.route('/suggest_sample_data', methods=['POST'])
def suggest_sample_data():
    try:
        data = request.json
        target_column = data.get('target_column')
        
        if not target_column:
            return jsonify({'error': 'Target column not specified'})
        
        # Get current data from session
        matches = session.get('matches', {})
        potential_headers = session.get('potential_headers', [])
        temp_file_path = session.get('temp_file_path')
        column_descriptions = session.get('column_descriptions', {})
        
        if not matches or not potential_headers or not temp_file_path:
            return jsonify({'error': 'No active analysis session found'})
        
        if not os.path.exists(temp_file_path):
            return jsonify({'error': 'Temporary file no longer available'})
        
        # Check if this target column has a match
        if target_column not in matches or matches[target_column].get('match') == "No match found":
            return jsonify({'error': 'This column has no matching header. Please use "Re-match" first or manually select data.'})
        
        # Get the matched header for this target column
        matched_header = matches[target_column].get('match')
        
        # Create a matcher instance
        matcher = ExcelHeaderMatcher()
        
        # Get the column description if available, or generate one on-the-fly
        column_description = None
        if target_column in column_descriptions:
            column_description = column_descriptions[target_column]
        else:
            # Generate a column description on-the-fly
            logging.info(f"Generating column description on-the-fly for {target_column}")
            try:
                # Create a prompt for the LLM to generate a description
                description_prompt = f"""
I need a description for this target column name that might appear in an Excel file:

"{target_column}"

Provide:
1. A brief description of what kind of data this column typically contains
2. The expected data type (text, number, date, etc.)
3. 3-5 realistic sample values that might appear in this column

Return ONLY a JSON object with this structure:
{{
    "{target_column}": {{
        "description": "Brief description of what this column contains",
        "data_type": "text|number|date|boolean|etc",
        "sample_values": ["example1", "example2", "example3"]
    }}
}}
                """
                
                description_response = matcher.model.invoke(description_prompt)
                
                # Extract the JSON object from the response
                import re
                json_pattern = r'\{[\s\S]*\}'
                json_match = re.search(json_pattern, description_response.content)
                
                if json_match:
                    json_str = json_match.group()
                    json_str = re.sub(r',\s*([\]}])', r'\1', json_str).strip()
                    missing_braces = json_str.count("{") - json_str.count("}")
                    if missing_braces > 0:
                        json_str += "}" * missing_braces
                    if len(re.findall(r'(?<!\\)"', json_str)) % 2 != 0:
                        json_str += '"'
                    
                    generated_descriptions = json.loads(json_str)
                    if target_column in generated_descriptions:
                        column_description = generated_descriptions[target_column]
                        logging.info(f"Successfully generated description for {target_column}")
                    else:
                        logging.warning(f"Generated description doesn't contain the target column {target_column}")
                        # Create a default description
                        column_description = {
                            "description": f"Data related to {target_column}",
                            "data_type": "text",
                            "sample_values": ["Example 1", "Example 2", "Example 3"]
                        }
                else:
                    logging.warning(f"Could not extract JSON from description response for {target_column}")
                    # Create a default description
                    column_description = {
                        "description": f"Data related to {target_column}",
                        "data_type": "text",
                        "sample_values": ["Example 1", "Example 2", "Example 3"]
                    }
            except Exception as e:
                logging.error(f"Error generating description for {target_column}: {e}")
                # Create a default description
                column_description = {
                    "description": f"Data related to {target_column}",
                    "data_type": "text",
                    "sample_values": ["Example 1", "Example 2", "Example 3"]
                }
        
        # Get the full Excel data
        excel_file = pd.ExcelFile(temp_file_path)
        excel_data = {}
        
        # Read all sheets and all data
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(temp_file_path, sheet_name=sheet_name, header=None)
            
            # Get dimensions
            rows, cols = df.shape
            
            # Extract all data
            sheet_data = []
            for i in range(rows):
                row_data = []
                for j in range(cols):
                    val = df.iloc[i, j]
                    # Format the value
                    if pd.isna(val):
                        row_data.append("")
                    elif isinstance(val, (int, float)) and val == int(val):
                        row_data.append(str(int(val)))
                    else:
                        row_data.append(str(val))
                sheet_data.append(row_data)
            
            excel_data[sheet_name] = {
                'data': sheet_data,
                'total_rows': rows,
                'total_cols': cols
            }
        
        # Use the LLM to find appropriate data based on the column description and matched header
        try:
            # Construct a prompt for the LLM
            prompt = f"""
I need to find appropriate sample data from an Excel file for a column named "{target_column}".

The matched header in the Excel file is: "{matched_header}"

Column description:
- Description: {column_description.get('description', 'Not available')}
- Data type: {column_description.get('data_type', 'Not available')}
- Expected sample values: {json.dumps(column_description.get('sample_values', []), indent=2)}

Here's the Excel file data:
"""
            
            # Add Excel data - we need to be careful about the prompt size
            # Include full data for smaller sheets, but limit larger sheets
            for sheet_name, sheet_data in excel_data.items():
                rows = sheet_data['total_rows']
                cols = sheet_data['total_cols']
                
                # For very large sheets, include a subset
                if rows > 100 or cols > 50:
                    max_rows = min(100, rows)
                    max_cols = min(50, cols)
                    
                    prompt += f"\nSheet: {sheet_name} (showing {max_rows} of {rows} rows and {max_cols} of {cols} columns)\n"
                    
                    # Include header rows and some data rows
                    for i in range(min(max_rows, len(sheet_data['data']))):
                        row_str = ", ".join(sheet_data['data'][i][:max_cols])
                        if len(row_str) > 1000:  # Truncate very long rows
                            row_str = row_str[:1000] + "..."
                        prompt += f"Row {i+1}: {row_str}\n"
                        
                    if rows > max_rows:
                        prompt += f"... ({rows - max_rows} more rows)\n"
                else:
                    # For smaller sheets, include all data
                    prompt += f"\nSheet: {sheet_name} ({rows} rows, {cols} columns)\n"
                    
                    for i in range(len(sheet_data['data'])):
                        row_str = ", ".join(sheet_data['data'][i])
                        if len(row_str) > 1000:  # Truncate very long rows
                            row_str = row_str[:1000] + "..."
                        prompt += f"Row {i+1}: {row_str}\n"
            
            prompt += f"""
Based on the matched header "{matched_header}" and the column description, please:

1. Identify the most appropriate data in the Excel file that matches the target column "{target_column}"
2. Extract 5-20 sample values that best represent this data
3. If you can't find exact matches for the header, look for semantically similar columns or data patterns that match the expected data type and description
4. If multiple potential matches exist, prioritize the one that best aligns with the column description

Return ONLY a JSON array of sample values, like this:
["sample1", "sample2", "sample3", ...]

Do not include any explanations or additional text in your response, just the JSON array.
"""
            
            # Check if the prompt is too large and truncate if necessary
            if len(prompt) > 100000:
                logging.warning(f"Prompt for {target_column} is too large ({len(prompt)} chars), truncating")
                prompt = prompt[:100000] + "\n\n[Excel data truncated due to size]\n\nReturn ONLY a JSON array of sample values."
            
            # Call the LLM
            response = matcher.model.invoke(prompt)
            
            # Extract the JSON array from the response
            content = response.content
            import re
            json_pattern = r'\[[\s\S]*\]'
            json_match = re.search(json_pattern, content)
            
            if json_match:
                json_str = json_match.group()
                # Clean JSON string if needed
                json_str = re.sub(r',\s*([\]}])', r'\1', json_str).strip()
                sample_data = json.loads(json_str)
                
                # Ensure we have at least some sample data
                if not sample_data:
                    # Fall back to the column description's sample values
                    sample_data = column_description.get('sample_values', ["No sample data found"])
                
                # Limit to 20 samples
                sample_data = sample_data[:20]
                
                # Update the session with the new sample data
                sample_data_dict = session.get('sample_data', {})
                sample_data_dict[target_column] = sample_data
                session['sample_data'] = sample_data_dict
                
                return jsonify({
                    'success': True,
                    'sample_data': sample_data,
                    'has_match': True,
                    'target_column': target_column
                })
            else:
                # If we couldn't extract a JSON array, try to fall back to traditional extraction
                logging.warning("Could not extract JSON array from LLM response, falling back to traditional extraction")
                
                # Try traditional extraction as a fallback
                excel_file = pd.ExcelFile(temp_file_path)
                all_samples = []
                
                # Try to find the header in each sheet
                for sheet_name in excel_file.sheet_names:
                    df = pd.read_excel(temp_file_path, sheet_name=sheet_name, header=None)
                    
                    # First try to find the header in the inferred header row
                    inferred_index = matcher._infer_header_row(df)
                    if inferred_index is not None:
                        header_row = df.iloc[inferred_index]
                        for col_idx, col_name in enumerate(header_row):
                            if str(col_name).strip().lower() == matched_header.lower():
                                # Extract sample data from this column
                                data_start_row = inferred_index + 1
                                if data_start_row < len(df):
                                    column_data = df.iloc[data_start_row:data_start_row+20, col_idx].tolist()
                                    samples = [str(val) for val in column_data if pd.notna(val)]
                                    if samples:
                                        all_samples.extend(samples)
                    
                    # If not found in header row, search the entire sheet
                    if not all_samples:
                        for i in range(min(30, len(df))):
                            for j in range(min(30, df.shape[1])):
                                if str(df.iloc[i, j]).strip().lower() == matched_header.lower():
                                    # Found the header, extract data below or to the right
                                    # Try below first (more common)
                                    if i + 1 < len(df):
                                        column_data = df.iloc[i+1:i+1+20, j].tolist()
                                        samples = [str(val) for val in column_data if pd.notna(val)]
                                        if samples:
                                            all_samples.extend(samples)
                                    
                                    # Try to the right if no data found below
                                    if not all_samples and j + 1 < df.shape[1]:
                                        row_data = df.iloc[i, j+1:j+1+20].tolist()
                                        samples = [str(val) for val in row_data if pd.notna(val)]
                                        if samples:
                                            all_samples.extend(samples)
                
                # If we found samples, use them
                if all_samples:
                    # Remove duplicates while preserving order
                    seen = set()
                    unique_samples = [x for x in all_samples if x not in seen and not seen.add(x)]
                    sample_data = unique_samples[:20]  # Limit to 20 samples
                    
                    # Update the session with the new sample data
                    sample_data_dict = session.get('sample_data', {})
                    sample_data_dict[target_column] = sample_data
                    session['sample_data'] = sample_data_dict
                    
                    return jsonify({
                        'success': True,
                        'sample_data': sample_data,
                        'has_match': True,
                        'target_column': target_column
                    })
                else:
                    # If all else fails, use the column description's sample values
                    sample_data = column_description.get('sample_values', ["No sample data found"])
                    
                    # Update the session with the new sample data
                    sample_data_dict = session.get('sample_data', {})
                    sample_data_dict[target_column] = sample_data
                    session['sample_data'] = sample_data_dict
                    
                    return jsonify({
                        'success': True,
                        'sample_data': sample_data,
                        'has_match': True,
                        'target_column': target_column
                    })
        
        except Exception as e:
            logging.error(f"Error using LLM to suggest sample data: {e}")
            logging.error(f"Raw response: {response.content if 'response' in locals() else 'No response'}")
            return jsonify({'error': f'Error suggesting sample data: {str(e)}'})
    
    except Exception as e:
        logging.error(f"Error suggesting sample data: {e}")
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
            # Try to get sample data from the results object for backward compatibility
            results = session.get('results', {})
            if results and 'sample_data' in results:
                sample_data = results['sample_data']
            else:
                return jsonify({'error': 'No active analysis session found'})
        
        # Update the sample data for this target column
        sample_data[target_column] = selected_data
        
        # Update both the direct session variable and the results object for backward compatibility
        session['sample_data'] = sample_data
        
        # Also update the results object if it exists
        results = session.get('results', {})
        if results:
            if 'sample_data' not in results:
                results['sample_data'] = {}
            results['sample_data'][target_column] = selected_data
            session['results'] = results
        
        # Get match information for this target column
        matches = session.get('matches', {})
        has_match = target_column in matches and matches[target_column].get('match') != "No match found"
        
        return jsonify({
            'success': True,
            'message': f'Sample data updated for {target_column}',
            'sample_data': selected_data,  # Return the updated sample data
            'has_match': has_match,  # Return whether this target column has a match
            'target_column': target_column  # Return the target column name
        })
    
    except Exception as e:
        logging.error(f"Error updating sample data: {e}")
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
                target_data = data.get('ai_data', {}).get(target, [])
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
                    target_data = data.get('ai_data', {}).get(target, [])
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
        logging.error(f"Error exporting CSV: {e}")
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
        logging.error(f"Error downloading CSV: {e}")
        return jsonify({'error': str(e)})


# @app.teardown_appcontext
# def cleanup_temp_files(exception=None):
#     """Clean up temporary files when the app context is torn down."""
#     try:
#         # Only try to access session if we're in a request context
#         from flask import has_request_context, session
#         if has_request_context():
#             temp_file_path = session.get('temp_file_path')
#             if temp_file_path and os.path.exists(temp_file_path):
#                 try:
