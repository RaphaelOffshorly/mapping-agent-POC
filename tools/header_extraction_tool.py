import logging
import pandas as pd
from typing import Dict, List, Any, Optional

from tools.base_tool import BaseTool
from utils.common import infer_header_row, extract_from_inferred_header
from utils.excel import read_excel_file
from config.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HeaderExtractionTool(BaseTool[str, List[str]]):
    """Tool for extracting potential headers from Excel files."""
    
    def __init__(self):
        """Initialize the header extraction tool."""
        super().__init__(
            name="header_extractor",
            description="Extracts potential headers from Excel files"
        )
        self.header_scan_rows = Config.HEADER_SCAN_ROWS
        self.cell_scan_rows = Config.CELL_SCAN_ROWS
        self.cell_scan_cols = Config.CELL_SCAN_COLS
    
    def run(self, file_path: str) -> List[str]:
        """
        Extract potential headers from an Excel file.
        
        Args:
            file_path: The path to the Excel file
            
        Returns:
            A list of potential headers
        """
        try:
            all_texts = []
            
            # Read all sheets in the Excel file
            sheet_dfs = read_excel_file(file_path)
            
            for sheet_name, df in sheet_dfs.items():
                logger.info(f"Processing sheet: {sheet_name}")
                
                # Heuristic 1: Infer header row based on string density
                inferred_index = infer_header_row(df, self.header_scan_rows)
                inferred_headers = extract_from_inferred_header(df, inferred_index)
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
        
        except Exception as e:
            logger.error(f"Error extracting headers: {e}")
            return []
