# IPAFFS REST API

A REST API for IPAFFS (Import of Plants, Plant Products and Food and Feed) data processing, built on Flask. This API provides JSON-only endpoints for PDF extraction, EPPO database lookups, CSV manipulation, and AI-powered data editing.

## Features

- **PDF Data Extraction**: Extract structured data from PDFs using IPAFFS schema
- **EPPO Database Integration**: Automatic EPPO code lookup and commodity pre-filling
- **IPAFFS Compatibility Checking**: Validate data against IPAFFS requirements
- **AI-Powered CSV Editing**: Chat-based CSV manipulation with human-in-the-loop support
- **Session Management**: Stateless operations with session tracking
- **Consistent JSON Responses**: All endpoints return structured JSON with CSV data
- **CORS Support**: Ready for frontend integration

## Quick Start

### Prerequisites

- Python 3.8+
- All dependencies from the main project installed
- Environment variables configured (see main project's `.env`)

### Installation

1. Install additional API dependencies:
```bash
cd ipaffs_api
pip install -r requirements.txt
```

2. Run the API server:
```bash
python app.py
```

The API will start on `http://localhost:5001` (different port from main app).

## API Endpoints

### Health & Schema

- `GET /api/v1/health` - Health check and service status
- `GET /api/v1/ipaffs/schema` - Get IPAFFS schema definition

### PDF Processing

- `POST /api/v1/ipaffs/extract-pdf` - Extract data from PDF using IPAFFS schema
  - Form data: `pdf_file` (file), `session_id` (optional)
  - Returns: Extracted data + CSV format

### Data Validation & Processing

- `POST /api/v1/ipaffs/check-compatibility` - Check IPAFFS compatibility
  - JSON: `{"csv_data": {...}, "session_id": "..."}`
  - Returns: Compatibility results + matched/missing headers

- `POST /api/v1/ipaffs/prefill-eppo` - Pre-fill data using EPPO database
  - JSON: `{"csv_data": {...}, "session_id": "..."}`
  - Returns: Updated CSV with EPPO codes + commodity options

### Session Management

- `GET /api/v1/ipaffs/csv-data/<session_id>` - Get CSV data for session
- `POST /api/v1/ipaffs/csv-data/<session_id>` - Update CSV data
- `DELETE /api/v1/ipaffs/csv-data/<session_id>` - Delete session

### AI Chat Integration

- `POST /api/v1/ipaffs/chat` - Chat with AI CSV editor
  - JSON: `{"message": "...", "csv_data": {...}, "session_id": "..."}`
  - Returns: AI response + modified CSV data
  - Supports conversation resumption with `thread_id`

### Commodity Management

- `POST /api/v1/ipaffs/commodity-selections` - Update commodity selections
  - JSON: `{"selections": [...], "session_id": "..."}`

### Export

- `POST /api/v1/ipaffs/export-csv` - Export CSV data
  - JSON: `{"csv_data": {...}, "session_id": "..."}`
  - Returns: CSV content as string

## Response Format

All endpoints return consistent JSON responses:

```json
{
  "success": boolean,
  "data": object,
  "csv_data": {
    "headers": ["Column1", "Column2", ...],
    "data": [
      {"Column1": "value1", "Column2": "value2"},
      ...
    ]
  },
  "meta": {
    "session_id": "uuid",
    "timestamp": "2025-01-14T12:00:00Z",
    "format": "array_of_objects|single_row|multi_row"
  },
  "error": null
}
```

## CSV Data Formats

The API supports three CSV data formats:

1. **Single Row**: One object in data array
2. **Multi Row**: Multiple objects, each representing a row
3. **Array of Objects**: Complex nested structure from PDF extraction

## Example Usage

### 1. Extract PDF Data

```bash
curl -X POST http://localhost:5001/api/v1/ipaffs/extract-pdf \
  -F "pdf_file=@document.pdf"
```

### 2. Check IPAFFS Compatibility

```bash
curl -X POST http://localhost:5001/api/v1/ipaffs/check-compatibility \
  -H "Content-Type: application/json" \
  -d '{
    "csv_data": {
      "headers": ["Genus and Species", "Commodity code"],
      "data": [{"Genus and Species": "Rosa hybrid", "Commodity code": ""}]
    }
  }'
```

### 3. Pre-fill EPPO Data

```bash
curl -X POST http://localhost:5001/api/v1/ipaffs/prefill-eppo \
  -H "Content-Type: application/json" \
  -d '{
    "csv_data": {
      "headers": ["Genus and Species"],
      "data": [{"Genus and Species": "Rosa hybrid"}]
    },
    "session_id": "session-uuid"
  }'
```

### 4. Chat with AI Editor

```bash
curl -X POST http://localhost:5001/api/v1/ipaffs/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Add a new column called Quantity with value 100 for all rows",
    "csv_data": {
      "headers": ["Genus and Species"],
      "data": [{"Genus and Species": "Rosa hybrid"}]
    }
  }'
```

## Error Handling

Errors return structured responses with appropriate HTTP status codes:

```json
{
  "success": false,
  "data": null,
  "csv_data": null,
  "meta": {
    "session_id": null,
    "timestamp": "2025-01-14T12:00:00Z",
    "format": null,
    "error_code": "VALIDATION_ERROR"
  },
  "error": "The provided data does not meet IPAFFS requirements."
}
```

## Configuration

Key configuration options in `config/api_config.py`:

- `API_VERSION`: API version (default: "v1")
- `MAX_CONTENT_LENGTH`: Max file upload size (default: 32MB)
- `SESSION_TIMEOUT`: Session expiration (default: 1 hour)
- `CORS_ORIGINS`: Allowed CORS origins
- `IPAFFS_SCHEMA_PATH`: Path to IPAFFS schema file

## Development

### Project Structure

```
ipaffs_api/
├── app.py                 # Main Flask application
├── config/
│   └── api_config.py     # API configuration
├── models/
│   └── ipaffs_models.py  # Pydantic models
├── services/
│   └── pdf_service.py    # PDF processing service
├── utils/
│   ├── response_helpers.py  # Response formatting
│   └── session_manager.py   # Session management
└── requirements.txt      # API dependencies
```

### Adding New Endpoints

1. Define request/response models in `models/ipaffs_models.py`
2. Create service functions if needed
3. Add route handler in `app.py`
4. Use `ResponseFormatter` for consistent responses
5. Update this documentation

## Integration Notes

- **Stateless**: Uses session IDs instead of server-side sessions
- **Thread-Safe**: Session manager supports concurrent requests
- **Error Resilient**: Comprehensive error handling and cleanup
- **Extensible**: Easy to add new IPAFFS-specific endpoints

## Differences from Main App

- **No HTML Templates**: Pure JSON API, no web interface
- **Session Management**: Custom session manager vs Flask sessions
- **Response Format**: Consistent JSON structure for all endpoints
- **CORS Enabled**: Ready for cross-origin requests
- **Error Handling**: Structured error responses with codes
- **File Handling**: Automatic cleanup of temporary files
