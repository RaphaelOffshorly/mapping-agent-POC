"""
Commodity Code Filtering Utility

Provides functions to filter commodity codes against a hardcoded reference list
while preserving descriptions from the EPPO database.
"""

import logging
from typing import Set, List, Tuple

logger = logging.getLogger(__name__)

# Hardcoded list of valid commodity codes (preserves exact formatting with leading zeros)
VALID_COMMODITY_CODES = {
    "06012010", "06012030", "06012090", "0601209010", "0601209090",
    "06021010", "06021090", "0602109010", "0602109090", "06022010",
    "06022020", "06022030", "06022080", "0602300010", "0602300090",
    "06024000", "0602400010", "0602400090", "06029020", "06029030",
    "06029041", "06029045", "06029046", "06029047", "06029048",
    "06029050", "0602905010", "0602905090", "06029070", "06029091",
    "0602909110", "0602909190", "06029099", "0602909910", "0602909990"
}

class CommodityCodeFilter:
    """Class for filtering commodity codes against a hardcoded reference list."""
    
    def __init__(self, csv_path: str = None):
        """
        Initialize the commodity code filter.
        
        Args:
            csv_path: Ignored - kept for backward compatibility
        """
        self._valid_codes = VALID_COMMODITY_CODES.copy()
        logger.info(f"Initialized commodity filter with {len(self._valid_codes)} hardcoded valid codes")
        
        # Log a few examples for verification
        examples = list(self._valid_codes)[:5]
        logger.info(f"Example valid codes: {examples}")
    
    def get_valid_codes(self) -> Set[str]:
        """
        Get the set of valid commodity codes.
        
        Returns:
            Set of valid commodity code strings
        """
        if self._valid_codes is None:
            self._load_valid_codes()
        return self._valid_codes.copy()
    
    def is_valid_code(self, commodity_code) -> bool:
        """
        Check if a commodity code is valid.
        
        Args:
            commodity_code: The commodity code to check (can be string, int, float, etc.)
            
        Returns:
            True if the code is valid, False otherwise
        """
        if self._valid_codes is None:
            self._load_valid_codes()
        
        if commodity_code is None:
            return False
        
        # Convert to string for comparison (handles int, float, string types)
        try:
            cleaned_code = str(commodity_code).strip()
        except (ValueError, TypeError):
            return False
        
        # Also check if it's empty after conversion
        if not cleaned_code:
            return False
        
        return cleaned_code in self._valid_codes
    
    def filter_eppo_results(self, eppo_results: List[Tuple]) -> List[Tuple]:
        """
        Filter EPPO lookup results to only include valid commodity codes.
        
        Args:
            eppo_results: List of tuples from EPPO lookup 
                         (commodity_name, eppo_code, commodity_code, description)
            
        Returns:
            Filtered list of tuples with only valid commodity codes
        """
        if not eppo_results:
            return []
        
        if self._valid_codes is None:
            self._load_valid_codes()
        
        # If no valid codes are loaded, return empty list (fail-safe)
        if not self._valid_codes:
            logger.warning("No valid commodity codes loaded - returning empty results")
            return []
        
        filtered_results = []
        
        for result in eppo_results:
            # Expected format: (commodity_name, eppo_code, commodity_code, description)
            if len(result) >= 3:
                commodity_code = result[2]  # Third element is commodity_code
                
                if self.is_valid_code(commodity_code):
                    filtered_results.append(result)
                    logger.debug(f"Included commodity code: {commodity_code}")
                else:
                    logger.debug(f"Filtered out commodity code: {commodity_code}")
        
        logger.info(f"Filtered EPPO results: {len(eppo_results)} -> {len(filtered_results)} valid codes")
        
        return filtered_results
    
    def filter_commodity_options(self, commodity_options: List[List[dict]]) -> List[List[dict]]:
        """
        Filter commodity options for dropdown menus.
        
        Args:
            commodity_options: List of lists of dictionaries with 'code', 'description', 'display' keys
            
        Returns:
            Filtered commodity options with only valid codes
        """
        if not commodity_options:
            return []
        
        filtered_options = []
        
        for row_options in commodity_options:
            if not row_options:
                filtered_options.append([])
                continue
            
            filtered_row = []
            for option in row_options:
                if isinstance(option, dict) and 'code' in option:
                    if self.is_valid_code(option['code']):
                        filtered_row.append(option)
                        logger.debug(f"Included option: {option['code']}")
                    else:
                        logger.debug(f"Filtered out option: {option['code']}")
            
            filtered_options.append(filtered_row)
        
        # Log summary
        original_count = sum(len(row) for row in commodity_options)
        filtered_count = sum(len(row) for row in filtered_options)
        logger.info(f"Filtered commodity options: {original_count} -> {filtered_count} valid options")
        
        return filtered_options
    
    def reload_codes(self):
        """Reload commodity codes from the CSV file."""
        self._valid_codes = None
        self._load_valid_codes()

# Global instance for easy access
_commodity_filter = None

def get_commodity_filter() -> CommodityCodeFilter:
    """Get the global commodity code filter instance."""
    global _commodity_filter
    if _commodity_filter is None:
        _commodity_filter = CommodityCodeFilter()
    return _commodity_filter

def filter_eppo_results(eppo_results: List[Tuple]) -> List[Tuple]:
    """
    Convenience function to filter EPPO results.
    
    Args:
        eppo_results: List of tuples from EPPO lookup
        
    Returns:
        Filtered list of tuples with only valid commodity codes
    """
    filter_instance = get_commodity_filter()
    return filter_instance.filter_eppo_results(eppo_results)

def is_valid_commodity_code(commodity_code: str) -> bool:
    """
    Convenience function to check if a commodity code is valid.
    
    Args:
        commodity_code: The commodity code to check
        
    Returns:
        True if the code is valid, False otherwise
    """
    filter_instance = get_commodity_filter()
    return filter_instance.is_valid_code(commodity_code)
