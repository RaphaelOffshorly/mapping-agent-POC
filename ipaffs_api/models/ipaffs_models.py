from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel, Field, validator
from datetime import datetime

class CSVData(BaseModel):
    """Model for CSV data structure."""
    headers: List[str] = Field(..., description="Column headers")
    data: List[Dict[str, Any]] = Field(..., description="Row data as list of dictionaries")
    
    class Config:
        schema_extra = {
            "example": {
                "headers": ["Genus and Species", "Commodity code", "EPPO code"],
                "data": [
                    {
                        "Genus and Species": "Rosa hybrid",
                        "Commodity code": "0603110000",
                        "EPPO code": "ROSHY"
                    }
                ]
            }
        }

class ExtractPDFRequest(BaseModel):
    """Request model for PDF extraction."""
    pdf_file: bytes = Field(..., description="PDF file content")
    session_id: Optional[str] = Field(None, description="Optional session ID to continue")
    
class ExtractPDFResponse(BaseModel):
    """Response model for PDF extraction."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: Optional[str] = None
    message: Optional[str] = None

class CompatibilityCheckRequest(BaseModel):
    """Request model for IPAFFS compatibility check."""
    csv_data: CSVData = Field(..., description="CSV data to check")
    session_id: Optional[str] = Field(None, description="Session ID")

class CompatibilityCheckResponse(BaseModel):
    """Response model for IPAFFS compatibility check."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: Optional[str] = None
    compatible: bool = Field(..., description="Whether data is IPAFFS compatible")
    matched_headers: Dict[str, str] = Field(..., description="Matched IPAFFS headers")
    missing_headers: List[str] = Field(..., description="Missing required headers")

class CommodityOption(BaseModel):
    """Model for commodity code options."""
    code: str = Field(..., description="Commodity code")
    description: str = Field(..., description="Commodity description")
    display: str = Field(..., description="Display text for UI")

class PrefillEPPORequest(BaseModel):
    """Request model for EPPO pre-filling."""
    csv_data: CSVData = Field(..., description="CSV data to pre-fill")
    session_id: Optional[str] = Field(None, description="Session ID")

class PrefillEPPOResponse(BaseModel):
    """Response model for EPPO pre-filling."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: Optional[str] = None
    eppo_codes_added: int = Field(..., description="Number of EPPO codes added")
    commodity_options: List[List[CommodityOption]] = Field(
        ..., description="Commodity options for each row"
    )
    is_array_format: bool = Field(..., description="Whether data is in array format")

class CommoditySelection(BaseModel):
    """Model for commodity code selection."""
    row_index: int = Field(..., description="Row index for selection")
    commodity_code: str = Field(..., description="Selected commodity code")
    display_text: str = Field(..., description="Display text for the selection")

class UpdateCommodityRequest(BaseModel):
    """Request model for updating commodity selections."""
    selections: List[CommoditySelection] = Field(..., description="Commodity selections")
    session_id: str = Field(..., description="Session ID")

class UpdateCommodityResponse(BaseModel):
    """Response model for updating commodity selections."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: Optional[str] = None
    updated_count: int = Field(..., description="Number of selections updated")

class ChatRequest(BaseModel):
    """Request model for AI chat interactions."""
    message: str = Field(..., description="User message")
    csv_data: CSVData = Field(..., description="Current CSV data")
    session_id: Optional[str] = Field(None, description="Session ID for conversation")
    thread_id: Optional[str] = Field(None, description="Thread ID for resuming conversation")
    source_data: Optional[Dict[str, Any]] = Field(None, description="Additional source data")

class ChatResponse(BaseModel):
    """Response model for AI chat interactions."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: Optional[str] = None
    response: str = Field(..., description="AI response message")
    csv_data_changed: bool = Field(..., description="Whether CSV data was modified")
    needs_input: bool = Field(False, description="Whether human input is needed")
    interrupt_message: Optional[str] = Field(None, description="Message for human interruption")
    thread_id: Optional[str] = Field(None, description="Thread ID for conversation continuation")

class ValidationRequest(BaseModel):
    """Request model for data validation."""
    csv_data: CSVData = Field(..., description="CSV data to validate")
    session_id: Optional[str] = Field(None, description="Session ID")
    validation_type: str = Field("ipaffs", description="Type of validation to perform")

class ValidationResponse(BaseModel):
    """Response model for data validation."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: Optional[str] = None
    valid: bool = Field(..., description="Whether data is valid")
    validation_errors: List[str] = Field(..., description="List of validation errors")
    validation_warnings: List[str] = Field([], description="List of validation warnings")

class ExportCSVRequest(BaseModel):
    """Request model for CSV export."""
    csv_data: CSVData = Field(..., description="CSV data to export")
    session_id: Optional[str] = Field(None, description="Session ID")
    export_format: str = Field("csv", description="Export format")
    include_commodity_selections: bool = Field(True, description="Include commodity selections")

class ExportCSVResponse(BaseModel):
    """Response model for CSV export."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: Optional[str] = None
    csv_content: str = Field(..., description="CSV content as string")
    filename: str = Field(..., description="Suggested filename")

class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str = Field(..., description="Service status")
    timestamp: str = Field(..., description="Current timestamp")
    version: str = Field(..., description="API version")
    active_sessions: int = Field(..., description="Number of active sessions")

class SessionDataRequest(BaseModel):
    """Request model for session data operations."""
    csv_data: Optional[CSVData] = Field(None, description="CSV data to store")
    additional_data: Optional[Dict[str, Any]] = Field(None, description="Additional data")

class SessionDataResponse(BaseModel):
    """Response model for session data operations."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: Optional[str] = None

class ErrorResponse(BaseModel):
    """Standard error response model."""
    success: bool = False
    data: Optional[Any] = None
    csv_data: Optional[CSVData] = None
    meta: Dict[str, Any]
    error: str = Field(..., description="Error message")
    
    class Config:
        schema_extra = {
            "example": {
                "success": False,
                "data": None,
                "csv_data": None,
                "meta": {
                    "session_id": "abc123",
                    "timestamp": "2025-01-14T12:00:00Z",
                    "format": None,
                    "error_code": "VALIDATION_ERROR"
                },
                "error": "The provided data does not meet IPAFFS requirements."
            }
        }

# Schema validation helpers
def validate_ipaffs_headers(headers: List[str]) -> Dict[str, Any]:
    """Validate headers against IPAFFS requirements."""
    required_headers = [
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
    
    normalized_headers = [h.lower().strip() for h in headers]
    matched_headers = {}
    missing_headers = []
    
    for required in required_headers:
        found = False
        for header, normalized in zip(headers, normalized_headers):
            if normalized == required.lower():
                matched_headers[required] = header
                found = True
                break
        
        if not found:
            missing_headers.append(required)
    
    return {
        "matched_headers": matched_headers,
        "missing_headers": missing_headers,
        "total_matched": len(matched_headers),
        "total_required": len(required_headers),
        "compatible": len(matched_headers) >= len(required_headers) * 0.6  # 60% match threshold
    }
