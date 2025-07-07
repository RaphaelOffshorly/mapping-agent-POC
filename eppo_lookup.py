#!/usr/bin/env python3
"""
EPPO Code Lookup Utility

Provides functions to lookup EPPO codes by commodity name from the SQLite database.
"""

import psycopg2
import psycopg2.extras
import psycopg2.pool
import os
import re
from typing import List, Tuple, Optional, Dict
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Hardcoded commodity codes dictionary from commodity_code.csv
COMMODITY_CODES = {
    "06012010": "Chicory plants and roots",
    "06012030": "Orchids, hyacinths, narcissi and tulips",
    "06012090": "Other",
    "0601209010": "Plants (rhizomes in flower) of Colocasia Schott",
    "0601209090": "Other",
    "06021010": "Of vines",
    "06021090": "Other",
    "0602109010": "Mormodica L., Solanum Melogena L. and Trichosantes L.",
    "0602109090": "Other",
    "06022010": "Vine slips, grafted or rooted",
    "06022020": "With bare roots",
    "06022030": "Citrus",
    "06022080": "Other",
    "0602300010": "Rhododendrons",
    "0602300090": "Azaleas",
    "06024000": "Roses, grafted or not",
    "0602400010": "Cuttings",
    "0602400090": "Other",
    "06029020": "Pineapple plants",
    "06029030": "Vegetable and strawberry plants",
    "06029041": "Forest trees",
    "06029045": "Rooted cuttings and young plants",
    "06029046": "With bare roots",
    "06029047": "Conifers and evergreens",
    "06029048": "Other",
    "06029050": "Other outdoor plants",
    "0602905010": "Mormodica L., Solanum Melogena L. and Trichosantes L.",
    "0602905090": "Other",
    "06029070": "Rooted cuttings and young plants, excluding cacti",
    "06029091": "Flowering plants with buds or flowers, excluding cacti",
    "0602909110": "Potted plants not exceeding 1m in height",
    "0602909190": "Other",
    "06029099": "Other",
    "0602909910": "Potted plants not exceeding 1m in height",
    "0602909990": "Other"
}

class EPPOLookup:
    """Class for looking up EPPO codes from the PostgreSQL database."""
    
    # Class-level connection pool
    _connection_pool = None
    _pool_lock = None
    
    def __init__(self, db_url: str = None, use_pool: bool = True):
        """
        Initialize the EPPO lookup with database URL.
        
        Args:
            db_url: PostgreSQL connection URL (defaults to environment variable)
            use_pool: Whether to use connection pooling (recommended for web apps)
        """
        # Get database URL from environment variable if not provided
        self.db_url = db_url or os.getenv('POSTGRESQL_URL')
        if not self.db_url:
            raise ValueError("PostgreSQL URL not provided and POSTGRESQL_URL environment variable not set")
        
        self.db_params = self._parse_postgresql_url(self.db_url)
        self.use_pool = use_pool
        
        # Initialize connection pool if enabled and not already created
        if self.use_pool:
            self._init_connection_pool()
    
    def _init_connection_pool(self):
        """Initialize the connection pool (thread-safe)."""
        import threading
        
        # Initialize lock if not already done
        if EPPOLookup._pool_lock is None:
            EPPOLookup._pool_lock = threading.Lock()
        
        # Only create pool if it doesn't exist
        with EPPOLookup._pool_lock:
            if EPPOLookup._connection_pool is None:
                try:
                    EPPOLookup._connection_pool = psycopg2.pool.SimpleConnectionPool(
                        1,  # minimum connections
                        20,  # maximum connections
                        **self.db_params
                    )
                    print("PostgreSQL connection pool initialized successfully")
                except Exception as e:
                    print(f"Failed to initialize connection pool: {e}")
                    # Fall back to non-pooled connections
                    self.use_pool = False
    
    def _parse_postgresql_url(self, url: str) -> dict:
        """Parse PostgreSQL URL and return connection parameters."""
        parsed = urlparse(url)
        return {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path[1:],  # Remove leading slash
            'user': parsed.username,
            'password': parsed.password
        }
    
    def clean_genus_species(self, genus_species: str) -> str:
        """
        Clean genus and species data by removing country information in parentheses.
        
        Examples:
            "Lithodora diffusa (NETHERLANDS)" -> "Lithodora diffusa"
            "Rosa alba (FRANCE)" -> "Rosa alba"
            "Quercus robur" -> "Quercus robur" (unchanged)
        
        Args:
            genus_species: The genus and species string that may contain country info
            
        Returns:
            Cleaned genus and species string with country info removed
        """
        if not genus_species:
            return genus_species
        
        # Remove anything in parentheses (country information) and extra whitespace
        cleaned = re.sub(r'\s*\([^)]*\)\s*', '', genus_species)
        
        # Clean up any multiple spaces and strip
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get a database connection from pool or create new one."""
        if self.use_pool and EPPOLookup._connection_pool is not None:
            try:
                return EPPOLookup._connection_pool.getconn()
            except Exception as e:
                print(f"Failed to get connection from pool: {e}")
                # Fall back to direct connection
                return psycopg2.connect(**self.db_params)
        else:
            return psycopg2.connect(**self.db_params)
    
    def _return_connection(self, conn):
        """Return a connection to the pool."""
        if self.use_pool and EPPOLookup._connection_pool is not None:
            try:
                EPPOLookup._connection_pool.putconn(conn)
            except Exception as e:
                print(f"Failed to return connection to pool: {e}")
                # Close the connection instead
                try:
                    conn.close()
                except:
                    pass
        else:
            conn.close()
    
    def lookup_by_commodity_name(self, commodity_name: str, exact_match: bool = True) -> List[Tuple]:
        """
        Lookup EPPO codes by commodity name.
        
        Args:
            commodity_name: The commodity name to search for (will be cleaned of country info)
            exact_match: If True, searches for exact match. If False, searches for partial matches.
        
        Returns:
            List of tuples containing (commodity_name, eppo_code, commodity_code, description)
        """
        # Clean the commodity name to remove country information
        cleaned_name = self.clean_genus_species(commodity_name)
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if exact_match:
                query = """
                    SELECT commodity_name, eppo_code, commodity_code, commodity_code_description
                    FROM eppo_codes 
                    WHERE LOWER(commodity_name) = LOWER(%s)
                    ORDER BY commodity_name
                """
                cursor.execute(query, (cleaned_name,))
            else:
                query = """
                    SELECT commodity_name, eppo_code, commodity_code, commodity_code_description
                    FROM eppo_codes 
                    WHERE LOWER(commodity_name) LIKE LOWER(%s)
                    ORDER BY commodity_name
                """
                cursor.execute(query, (f"%{cleaned_name}%",))
            
            results = cursor.fetchall()
            return results
            
        finally:
            self._return_connection(conn)
    
    def lookup_by_eppo_code(self, eppo_code: str) -> List[Tuple]:
        """
        Lookup commodity information by EPPO code.
        
        Args:
            eppo_code: The EPPO code to search for
        
        Returns:
            List of tuples containing (commodity_name, eppo_code, commodity_code, description)
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT commodity_name, eppo_code, commodity_code, commodity_code_description
                FROM eppo_codes 
                WHERE UPPER(eppo_code) = UPPER(%s)
                ORDER BY commodity_name
            """
            cursor.execute(query, (eppo_code,))
            results = cursor.fetchall()
            return results
            
        finally:
            self._return_connection(conn)
    
    def search_commodities(self, search_term: str, limit: int = 50) -> List[Tuple]:
        """
        Search for commodities containing the search term.
        
        Args:
            search_term: Term to search for in commodity names (will be cleaned of country info)
            limit: Maximum number of results to return
        
        Returns:
            List of tuples containing (commodity_name, eppo_code, commodity_code, description)
        """
        # Clean the search term to remove country information
        cleaned_term = self.clean_genus_species(search_term)
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT commodity_name, eppo_code, commodity_code, commodity_code_description
                FROM eppo_codes 
                WHERE LOWER(commodity_name) LIKE LOWER(%s)
                   OR LOWER(commodity_code_description) LIKE LOWER(%s)
                ORDER BY commodity_name
                LIMIT %s
            """
            search_pattern = f"%{cleaned_term}%"
            cursor.execute(query, (search_pattern, search_pattern, limit))
            results = cursor.fetchall()
            return results
            
        finally:
            self._return_connection(conn)
    
    def get_stats(self) -> dict:
        """
        Get database statistics.
        
        Returns:
            Dictionary with database statistics
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Total records
            cursor.execute("SELECT COUNT(*) FROM eppo_codes")
            total_records = cursor.fetchone()[0]
            
            # Unique commodity names
            cursor.execute("SELECT COUNT(DISTINCT commodity_name) FROM eppo_codes")
            unique_commodities = cursor.fetchone()[0]
            
            # Unique EPPO codes
            cursor.execute("SELECT COUNT(DISTINCT eppo_code) FROM eppo_codes")
            unique_eppo_codes = cursor.fetchone()[0]
            
            return {
                'total_records': total_records,
                'unique_commodities': unique_commodities,
                'unique_eppo_codes': unique_eppo_codes,
                'database_type': 'PostgreSQL'
            }
            
        finally:
            self._return_connection(conn)
    
    def lookup_by_first_word_ipaffs(self, genus_species: str) -> Tuple[str, List[Tuple]]:
        """
        IPAFFS guide approach: Search using first word only, then construct EPPO code.
        
        Process:
        1. Clean input data (remove country info in parentheses)
        2. Search by first word (genus) e.g., "Lithodora"
        3. Get EPPO codes from results and extract the prefix e.g., "LTD" from "LTDDI"
        4. Take first 2 letters of species e.g., "DI" from "diffusa"
        5. Construct final EPPO code e.g., "LTDDI"
        6. Lookup all results with that EPPO code
        7. If no exact match, return constructed EPPO code with all valid commodity codes
        
        Args:
            genus_species: Full genus and species name (e.g., "Lithodora diffusa (NETHERLANDS)")
        
        Returns:
            Tuple of (constructed_eppo_code, matching_results) where:
            - constructed_eppo_code: The EPPO code following the discovered pattern
            - matching_results: List of tuples with all results for that EPPO code or all valid commodity codes
        """
        # Clean the input to remove country information
        cleaned_genus_species = self.clean_genus_species(genus_species)
        
        words = cleaned_genus_species.strip().split()
        if len(words) < 2:
            return "", []
        
        first_word = words[0]  # Genus
        second_word = words[1]  # Species
        
        # Search using only the first word (genus)
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Find all results that start with the first word
            query = """
                SELECT commodity_name, eppo_code, commodity_code, commodity_code_description
                FROM eppo_codes 
                WHERE LOWER(commodity_name) LIKE LOWER(%s)
                ORDER BY commodity_name
            """
            cursor.execute(query, (f"{first_word}%",))
            first_word_results = cursor.fetchall()
            
            if not first_word_results:
                return "", []
            
            # Extract EPPO code prefixes from the results
            # Most EPPO codes for plants are 5 characters, so prefix is first 3
            eppo_prefixes = set()
            for result in first_word_results:
                eppo_code = result[1]  # eppo_code is at index 1
                if len(eppo_code) >= 3:
                    prefix = eppo_code[:3].upper()
                    eppo_prefixes.add(prefix)
            
            if not eppo_prefixes:
                return "", []
            
            # Prioritize EPPO codes that start with the first letter of the genus
            first_letter = first_word[0].upper()
            prioritized_prefixes = [prefix for prefix in eppo_prefixes if prefix.startswith(first_letter)]
            
            if prioritized_prefixes:
                # Use the first prioritized prefix (starts with first letter of genus)
                eppo_prefix = prioritized_prefixes[0]
                print(f"Prioritized EPPO prefix '{eppo_prefix}' (starts with '{first_letter}' from '{first_word}')")
            else:
                # No prefixes start with first letter, proceed with current flow
                eppo_prefix = list(eppo_prefixes)[0]
                print(f"No EPPO prefix starts with '{first_letter}', using '{eppo_prefix}'")
            
            # Get first 2 letters of species
            species_part = second_word[:2].upper()
            
            # Construct the final EPPO code
            constructed_eppo = eppo_prefix + species_part
            
            # Lookup all results with this constructed EPPO code
            exact_eppo_results = self.lookup_by_eppo_code(constructed_eppo)
            
            if exact_eppo_results:
                return constructed_eppo, exact_eppo_results
            
            # If no exact match found, return constructed EPPO code with all valid commodity codes
            # Convert hardcoded commodity codes to tuples format for consistency
            commodity_tuples = []
            for code, description in COMMODITY_CODES.items():
                # Format: (commodity_name, eppo_code, commodity_code, description)
                commodity_tuples.append((genus_species, constructed_eppo, code, description))
            
            return constructed_eppo, commodity_tuples
            
        finally:
            self._return_connection(conn)
    
    def enhanced_lookup_ipaffs(self, genus_species: str) -> Tuple[str, List[Tuple]]:
        """
        Enhanced IPAFFS lookup with fallback strategy.
        
        1. First try exact match
        2. If no exact match, try partial match
        3. If still no match, try first word only approach (IPAFFS guide)
        
        Args:
            genus_species: Full genus and species name
        
        Returns:
            Tuple of (eppo_code, results) where results have valid commodity codes
        """
        # Step 1: Try exact match
        exact_results = self.lookup_by_commodity_name(genus_species, exact_match=True)
        if exact_results:
            return exact_results[0][1], exact_results
        
        # Step 2: Try partial match
        partial_results = self.lookup_by_commodity_name(genus_species, exact_match=False)
        if partial_results:
            # Find the best match (contains both words)
            words = genus_species.lower().split()
            if len(words) >= 2:
                for result in partial_results:
                    commodity_name = result[0].lower()
                    if all(word in commodity_name for word in words):
                        return result[1], [result]
            # If no perfect partial match, return first result
            return partial_results[0][1], partial_results
        
        # Step 3: Try first word only approach (IPAFFS guide)
        return self.lookup_by_first_word_ipaffs(genus_species)

def print_results(results: List[Tuple], title: str = "Results"):
    """Print search results in a formatted way."""
    if not results:
        print("No results found.")
        return
    
    print(f"\n{title} ({len(results)} found):")
    print("-" * 80)
    
    for i, (commodity_name, eppo_code, commodity_code, description) in enumerate(results, 1):
        print(f"{i:3}. Commodity: {commodity_name}")
        print(f"     EPPO Code: {eppo_code}")
        print(f"     Commodity Code: {commodity_code}")
        print(f"     Description: {description[:60]}...")
        print()

def main():
    """Main function for command-line usage."""
    import sys
    
    if len(sys.argv) < 2:
        print("EPPO Code Lookup Utility")
        print("Usage:")
        print("  python eppo_lookup.py <commodity_name>     - Exact lookup")
        print("  python eppo_lookup.py search <term>        - Search in names and descriptions")
        print("  python eppo_lookup.py eppo <eppo_code>     - Lookup by EPPO code")
        print("  python eppo_lookup.py stats                - Show database statistics")
        return
    
    try:
        lookup = EPPOLookup()
        
        if sys.argv[1] == "stats":
            stats = lookup.get_stats()
            print("Database Statistics:")
            print(f"  Total records: {stats['total_records']:,}")
            print(f"  Unique commodities: {stats['unique_commodities']:,}")
            print(f"  Unique EPPO codes: {stats['unique_eppo_codes']:,}")
            print(f"  Database type: {stats['database_type']}")
            
        elif sys.argv[1] == "search" and len(sys.argv) > 2:
            search_term = " ".join(sys.argv[2:])
            results = lookup.search_commodities(search_term)
            print_results(results, f"Search results for '{search_term}'")
            
        elif sys.argv[1] == "eppo" and len(sys.argv) > 2:
            eppo_code = sys.argv[2]
            results = lookup.lookup_by_eppo_code(eppo_code)
            print_results(results, f"Results for EPPO code '{eppo_code}'")
            
        else:
            # Exact commodity name lookup
            commodity_name = " ".join(sys.argv[1:])
            results = lookup.lookup_by_commodity_name(commodity_name, exact_match=True)
            
            if not results:
                # Try partial match
                print(f"No exact match found for '{commodity_name}'. Trying partial match...")
                results = lookup.lookup_by_commodity_name(commodity_name, exact_match=False)
            
            print_results(results, f"Results for '{commodity_name}'")
    
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the PostgreSQL database is accessible and contains the EPPO data.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
