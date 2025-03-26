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
