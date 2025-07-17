"""
Genus and Species Validation Filter

Provides functions to validate and filter genus and species entries using
proper taxonomic validation methods and external APIs.
"""

import re
import requests
import logging
from typing import List, Set, Optional, Tuple, Dict
from functools import lru_cache
import time

logger = logging.getLogger(__name__)

class GenusSpeciesFilter:
    """Class for validating and filtering genus and species entries."""
    
    def __init__(self, enable_api_validation: bool = True):
        """
        Initialize the genus and species filter.
        
        Args:
            enable_api_validation: Whether to enable external API validation
        """
        self.enable_api_validation = enable_api_validation
        self.api_cache = {}
        self.api_timeout = 5  # seconds
        
        # ITIS API endpoints
        self.itis_search_url = "https://www.itis.gov/ITISWebService/services/ITISService/searchByScientificName"
        self.itis_tsn_url = "https://www.itis.gov/ITISWebService/services/ITISService/getAcceptedNamesFromTSN"
        
        # GBIF API endpoints (as fallback)
        self.gbif_species_url = "https://api.gbif.org/v1/species/match"
        
        logger.info(f"Initialized GenusSpeciesFilter with API validation: {enable_api_validation}")
    
    def normalize_genus_species(self, genus_species: str) -> Optional[str]:
        """
        Normalize and clean genus and species name to proper format.
        
        Handles:
        - Wrong capitalization (rosa damascena → Rosa damascena)
        - Parentheses with country info (Rosa damascena (France) → Rosa damascena)
        - Taxonomic synonyms (Rhyncospermum (=Trachelospermum) jasminoides → Trachelospermum jasminoides)
        - Cultivar names in quotes (Rosa damascena "Red Robin" → Rosa damascena)
        - Cultivar names without quotes (Rosa damascena Red Robin → Rosa damascena)
        - Synonym notation (Chamaerops excelsa (=Trachycarpus fortunei) → Trachycarpus fortunei)
        
        Args:
            genus_species: The genus and species string to normalize
            
        Returns:
            Normalized genus and species string, or None if cannot be normalized
        """
        if not genus_species or not isinstance(genus_species, str):
            return None
        
        # Clean the string
        cleaned = genus_species.strip()
        
        # Check for obvious non-scientific patterns first
        if self._contains_invalid_patterns(cleaned):
            return None
        
        # Handle taxonomic synonyms: "Rhyncospermum (=Trachelospermum) jasminoides"
        synonym_match = re.search(r'(\w+)\s*\(=(\w+)\)\s+(\w+)', cleaned)
        if synonym_match:
            old_genus, new_genus, species = synonym_match.groups()
            # Use the synonym genus (the one in parentheses)
            cleaned = f"{new_genus} {species}"
        else:
            # Handle other synonym patterns: "Chamaerops excelsa (=Trachycarpus fortunei)"
            synonym_match2 = re.search(r'(\w+)\s+(\w+)\s*\(=(\w+)\s+(\w+)\)', cleaned)
            if synonym_match2:
                old_genus, old_species, new_genus, new_species = synonym_match2.groups()
                # Use the synonym name (the one in parentheses)
                cleaned = f"{new_genus} {new_species}"
        
        # Remove cultivar names in quotes: "Red Robin", "Pyramidalis", etc.
        cleaned = re.sub(r'\s*"[^"]*"\s*', ' ', cleaned)
        
        # Remove any remaining parentheses and their contents (country info, etc.)
        cleaned = re.sub(r'\s*\([^)]*\)\s*', ' ', cleaned)
        
        # Clean up multiple spaces
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Split into words
        words = cleaned.split()
        
        # Must have at least two words, take only first two (genus species)
        if len(words) < 2:
            return None
        
        genus, species = words[0], words[1]
        
        # Both words must be alphabetic
        if not genus.isalpha() or not species.isalpha():
            return None
        
        # Length checks (reasonable bounds)
        if len(genus) < 2 or len(genus) > 25:
            return None
        if len(species) < 2 or len(species) > 30:
            return None
        
        # Normalize capitalization: Genus capitalized, species lowercase
        normalized_genus = genus.capitalize()
        normalized_species = species.lower()
        
        return f"{normalized_genus} {normalized_species}"
    
    def is_valid_binomial_format(self, genus_species: str) -> bool:
        """
        Check if the string follows basic binomial nomenclature format.
        
        Rules:
        - Must contain exactly two words
        - First word (genus) must be capitalized
        - Second word (species) must be lowercase
        - Both words must contain only alphabetic characters
        - No numbers, special characters, or parentheses
        
        Args:
            genus_species: The genus and species string to validate
            
        Returns:
            True if format is valid, False otherwise
        """
        if not genus_species or not isinstance(genus_species, str):
            return False
        
        # Clean the string
        cleaned = genus_species.strip()
        
        # Check for obvious non-scientific patterns
        if self._contains_invalid_patterns(cleaned):
            return False
        
        # Split into words
        words = cleaned.split()
        
        # Must have exactly two words
        if len(words) != 2:
            return False
        
        genus, species = words
        
        # Genus must be capitalized alphabetic word
        if not genus.isalpha() or not genus[0].isupper():
            return False
        
        # Species must be lowercase alphabetic word
        if not species.isalpha() or not species.islower():
            return False
        
        # Length checks (reasonable bounds)
        if len(genus) < 2 or len(genus) > 25:
            return False
        if len(species) < 2 or len(species) > 30:
            return False
        
        return True
    
    def _contains_invalid_patterns(self, text: str) -> bool:
        """
        Check if text contains patterns that indicate it's not a scientific name.
        
        Args:
            text: The text to check
            
        Returns:
            True if invalid patterns are found, False otherwise
        """
        text_lower = text.lower()
        
        # Common words that indicate product descriptions, not scientific names
        invalid_indicators = [
            'edible', 'fruit', 'tree', 'trees', 'shrub', 'shrubs', 'container', 'pot', 'potted',
            'bare root', 'rooted', 'grafted', 'cutting', 'cuttings', 'plant', 'plants',
            'flower', 'flowering', 'ornamental', 'commercial', 'production', 'indoor', 'outdoor',
            'height', 'size', 'cm', 'mm', 'inch', 'ft', 'meter', 'grade', 'quality',
            'variety', 'varieties', 'cultivar', 'cultivars', 'hybrid', 'seed', 'seeds',
            'bulb', 'bulbs', 'tuber', 'tubers', 'rhizome', 'rhizomes', 'nursery',
            'wholesale', 'retail', 'for sale', 'package', 'packaging', 'exceed',
            'not exceed', 'less than', 'more than', 'other', 'various', 'mixed',
            'assorted', 'type', 'types', 'kind', 'kinds', 'in cvs', 'cv.', 'var.',
            'subsp.', 'f.', 'sp.', 'spp.', 'etc', 'and other', 'including',
            'excluding', 'except', 'such as', 'like', 'similar'
        ]
        
        # Check for numbers (but allow them in parentheses for now - will be cleaned)
        if any(char.isdigit() for char in text):
            return True
        
        # Check for invalid indicator words
        for indicator in invalid_indicators:
            if indicator in text_lower:
                return True
        
        # Check for excessive length (scientific names are typically concise)
        if len(text) > 100:  # More lenient since we can clean parentheses
            return True
        
        return False
    
    @lru_cache(maxsize=1000)
    def validate_with_itis(self, genus_species: str) -> bool:
        """
        Validate scientific name using ITIS API.
        
        Args:
            genus_species: The genus and species name to validate
            
        Returns:
            True if valid according to ITIS, False otherwise
        """
        if not self.enable_api_validation:
            return True
        
        try:
            # ITIS SOAP API call
            soap_envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
            <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                <soap:Body>
                    <searchByScientificName xmlns="http://itis_service.itis.usgs.gov">
                        <srchKey>{genus_species}</srchKey>
                    </searchByScientificName>
                </soap:Body>
            </soap:Envelope>"""
            
            headers = {
                'Content-Type': 'text/xml; charset=utf-8',
                'SOAPAction': 'searchByScientificName'
            }
            
            response = requests.post(
                self.itis_search_url,
                data=soap_envelope,
                headers=headers,
                timeout=self.api_timeout
            )
            
            if response.status_code == 200:
                # Simple check - if we get a response with TSN, it's likely valid
                return 'tsn' in response.text.lower()
            
        except Exception as e:
            logger.debug(f"ITIS API validation failed for '{genus_species}': {e}")
        
        return False
    
    @lru_cache(maxsize=1000)
    def validate_with_gbif(self, genus_species: str) -> bool:
        """
        Validate scientific name using GBIF API.
        
        Args:
            genus_species: The genus and species name to validate
            
        Returns:
            True if valid according to GBIF, False otherwise
        """
        if not self.enable_api_validation:
            return True
        
        try:
            words = genus_species.split()
            if len(words) != 2:
                return False
            
            genus, species = words
            
            params = {
                'name': genus_species,
                'verbose': 'false'
            }
            
            response = requests.get(
                self.gbif_species_url,
                params=params,
                timeout=self.api_timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                # Check if we got a valid match
                return (data.get('matchType') in ['EXACT', 'FUZZY'] and 
                       data.get('species') is not None)
            
        except Exception as e:
            logger.debug(f"GBIF API validation failed for '{genus_species}': {e}")
        
        return False
    
    def is_valid_genus_species(self, genus_species: str) -> bool:
        """
        Comprehensive validation of genus and species name.
        
        Args:
            genus_species: The genus and species name to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Step 1: Try to normalize the name first
        normalized = self.normalize_genus_species(genus_species)
        if not normalized:
            logger.debug(f"Cannot normalize genus/species: '{genus_species}'")
            return False
        
        # Step 2: Basic format validation on normalized name
        if not self.is_valid_binomial_format(normalized):
            logger.debug(f"Invalid binomial format after normalization: '{normalized}'")
            return False
        
        # Step 3: API validation (if enabled)
        if self.enable_api_validation:
            # Try GBIF first (faster REST API)
            if self.validate_with_gbif(normalized):
                logger.debug(f"Valid according to GBIF: '{normalized}' (from '{genus_species}')")
                return True
            
            # Try ITIS as fallback
            if self.validate_with_itis(normalized):
                logger.debug(f"Valid according to ITIS: '{normalized}' (from '{genus_species}')")
                return True
            
            logger.debug(f"Not found in taxonomic databases: '{normalized}' (from '{genus_species}')")
            return False
        
        # If API validation is disabled, rely on format validation
        return True
    
    def filter_genus_species_list(self, genus_species_list: List[str]) -> List[str]:
        """
        Filter a list of genus and species names, keeping only valid ones.
        Returns normalized versions of valid names.
        
        Args:
            genus_species_list: List of genus and species names to filter
            
        Returns:
            List of valid and normalized genus and species names
        """
        if not genus_species_list:
            return []
        
        valid_entries = []
        
        for entry in genus_species_list:
            if not entry or not entry.strip():
                continue
            
            cleaned_entry = entry.strip()
            
            # Try to normalize the entry
            normalized = self.normalize_genus_species(cleaned_entry)
            if normalized and self.is_valid_genus_species(cleaned_entry):
                valid_entries.append(normalized)
                if normalized != cleaned_entry:
                    logger.debug(f"Normalized genus/species: '{cleaned_entry}' -> '{normalized}'")
                else:
                    logger.debug(f"Valid genus/species: '{cleaned_entry}'")
            else:
                logger.debug(f"Invalid genus/species filtered out: '{cleaned_entry}'")
        
        logger.info(f"Filtered genus/species list: {len(genus_species_list)} -> {len(valid_entries)} valid entries")
        return valid_entries
    
    def get_validation_stats(self) -> Dict[str, int]:
        """Get validation statistics."""
        gbif_cache_info = self.validate_with_gbif.cache_info()
        itis_cache_info = self.validate_with_itis.cache_info()
        
        return {
            'gbif_cache_hits': gbif_cache_info.hits,
            'gbif_cache_misses': gbif_cache_info.misses,
            'itis_cache_hits': itis_cache_info.hits,
            'itis_cache_misses': itis_cache_info.misses,
            'total_api_calls': gbif_cache_info.misses + itis_cache_info.misses
        }
    
    def clear_cache(self):
        """Clear the validation cache."""
        self.validate_with_gbif.cache_clear()
        self.validate_with_itis.cache_clear()
        self.api_cache.clear()
        logger.info("Validation cache cleared")

# Global instance for easy access
_genus_species_filter = None

def get_genus_species_filter(enable_api_validation: bool = True) -> GenusSpeciesFilter:
    """Get the global genus species filter instance."""
    global _genus_species_filter
    if _genus_species_filter is None:
        _genus_species_filter = GenusSpeciesFilter(enable_api_validation=enable_api_validation)
    return _genus_species_filter

def is_valid_genus_species(genus_species: str, enable_api_validation: bool = True) -> bool:
    """
    Convenience function to validate a genus and species name.
    
    Args:
        genus_species: The genus and species name to validate
        enable_api_validation: Whether to use external API validation
        
    Returns:
        True if valid, False otherwise
    """
    filter_instance = get_genus_species_filter(enable_api_validation)
    return filter_instance.is_valid_genus_species(genus_species)

def filter_genus_species_list(genus_species_list: List[str], enable_api_validation: bool = True) -> List[str]:
    """
    Convenience function to filter a list of genus and species names.
    
    Args:
        genus_species_list: List of genus and species names to filter
        enable_api_validation: Whether to use external API validation
        
    Returns:
        List of valid genus and species names
    """
    filter_instance = get_genus_species_filter(enable_api_validation)
    return filter_instance.filter_genus_species_list(genus_species_list)

def main():
    """Main function for command-line testing."""
    import sys
    
    if len(sys.argv) < 2:
        print("Genus Species Filter Utility")
        print("Usage:")
        print("  python genus_species_filter.py <genus_species>     - Validate single name")
        print("  python genus_species_filter.py test                - Run test cases")
        return
    
    filter_instance = GenusSpeciesFilter(enable_api_validation=True)
    
    if sys.argv[1] == "test":
        # Test cases including examples from sample data
        test_cases = [
            # Basic valid cases
            "Rosa damascena",  # Valid
            "Quercus robur",   # Valid
            "Lithodora diffusa",  # Valid
            "Acer palmatum",   # Valid
            "Prunus serrulata",  # Valid
            
            # Cases from sample data (should all be valid)
            "Amelanchier lamarckii",  # Valid
            "Chamaerops excelsa (=Trachycarpus fortunei)",  # Valid (synonym notation)
            "Cordyline australis",  # Valid
            'Cupressus sempervirens "Pyramidalis"',  # Valid (cultivar in quotes)
            "Laurus nobilis",  # Valid
            "Olea europea",  # Valid
            'Phormium tenax "Purpureum"',  # Valid (cultivar in quotes)
            'Photinia serratifolia "Red Robin"',  # Valid (cultivar in quotes)
            "Photinia serratifolia Red Robin compatta",  # Valid (cultivar without quotes)
            'Pittosporum tobira "Nana"',  # Valid (cultivar in quotes)
            "Rhyncospermum (=Trachelospermum) jasminoides",  # Valid (synonym handling)
            
            # Capitalization tests
            "rosa damascena",  # Should be valid (wrong capitalization, gets normalized)
            "ROSA DAMASCENA",  # Should be valid (wrong capitalization, gets normalized)
            "Rosa damascena (France)",  # Should be valid (parentheses cleaned)
            "Olea europea (Spain)",  # Should be valid (parentheses cleaned)
            
            # Invalid cases
            "Edible fruit trees and shrubs in container or RB not bare root (Olea europea in cvs)",  # Invalid
            "Container plants for commercial production",  # Invalid
            "Various ornamental flowering plants",  # Invalid
            "Mixed variety pack",  # Invalid
            "Rosa",  # Invalid (only one word)
            "Rosa damasc3na",  # Invalid (contains number)
        ]
        
        print("Testing genus/species validation:")
        print("=" * 50)
        
        for test_case in test_cases:
            is_valid = filter_instance.is_valid_genus_species(test_case)
            status = "✓ VALID" if is_valid else "✗ INVALID"
            print(f"{status:10} | {test_case}")
        
        print("\nValidation Statistics:")
        stats = filter_instance.get_validation_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    else:
        # Validate single name
        genus_species = " ".join(sys.argv[1:])
        is_valid = filter_instance.is_valid_genus_species(genus_species)
        
        print(f"Input: '{genus_species}'")
        print(f"Valid: {is_valid}")
        
        if not is_valid:
            format_valid = filter_instance.is_valid_binomial_format(genus_species)
            print(f"Format valid: {format_valid}")

if __name__ == "__main__":
    main()
