import os
import tempfile
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for the application."""
    
    # API Keys
    ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
    
    # Flask configuration
    SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'default-secret-key')
    UPLOAD_FOLDER = tempfile.gettempdir()
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    SESSION_TYPE = 'filesystem'  # Use filesystem session storage
    SESSION_FILE_DIR = os.path.join(tempfile.gettempdir(), 'flask_sessions')
    SESSION_PERMANENT = False
    PERMANENT_SESSION_LIFETIME = 3600  # 1 hour
    
    # LLM configuration
    DEFAULT_MODEL = "claude-3-7-sonnet-latest"
    
    # Excel processing configuration
    HEADER_SCAN_ROWS = 20  # Number of rows to consider when inferring header
    CELL_SCAN_ROWS = 50    # Number of rows to scan for cell-level heuristics
    CELL_SCAN_COLS = 50    # Number of columns to scan for cell-level heuristics
    
    # Static messages
    OUT_OF_SCOPE_MESSAGE = """
    We're sorry, but your request is outside the scope of what this Excel header matcher tool can process.
    
    This tool is specifically designed to handle CSV file editing operations such as:
    - Adding, removing, or modifying columns
    - Updating cell values
    - Filtering data
    - Basic calculations
    
    For other tasks like creating presentations, generating complex visualizations, or 
    performing operations on non-CSV files, please use an appropriate specialized tool.
    
    If you believe this is an error, please try rephrasing your request to focus specifically 
    on CSV editing operations.
    """
