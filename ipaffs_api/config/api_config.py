import os
import tempfile
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class APIConfig:
    """Configuration class for the IPAFFS REST API."""
    
    # API Keys
    ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
    
    # Flask configuration
    SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'ipaffs-api-secret-key')
    UPLOAD_FOLDER = tempfile.gettempdir()
    MAX_CONTENT_LENGTH = 32 * 1024 * 1024  # 32MB max file size for API
    
    # API configuration
    API_VERSION = "v1"
    API_PREFIX = f"/api/{API_VERSION}"
    
    # Session management (for stateless operations)
    SESSION_TIMEOUT = 3600  # 1 hour
    MAX_SESSIONS = 1000
    
    # LLM configuration
    DEFAULT_MODEL = "claude-3-7-sonnet-latest"
    
    # Excel processing configuration
    HEADER_SCAN_ROWS = 20
    CELL_SCAN_ROWS = 50
    CELL_SCAN_COLS = 50
    
    # IPAFFS specific configuration
    IPAFFS_SCHEMA_PATH = "ipaffs_schema.json"
    
    # CORS configuration
    CORS_ORIGINS = ["*"]  # Configure as needed for production
    
    # Response format configuration
    DEFAULT_RESPONSE_FORMAT = "json"
    INCLUDE_CSV_IN_RESPONSE = True
    
    # Error messages
    GENERIC_ERROR_MESSAGE = "An error occurred while processing your request."
    VALIDATION_ERROR_MESSAGE = "The provided data does not meet IPAFFS requirements."
    OUT_OF_SCOPE_MESSAGE = """
    We're sorry, but your request is outside the scope of what this IPAFFS tool can process.
    
    This tool is specifically designed to handle IPAFFS-related operations such as:
    - PDF data extraction using IPAFFS schema
    - EPPO database lookups and pre-filling
    - CSV data validation and manipulation for IPAFFS compliance
    - Commodity code selection and validation
    
    For other tasks, please use an appropriate specialized tool.
    """
