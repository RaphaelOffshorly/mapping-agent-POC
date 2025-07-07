#!/usr/bin/env python3
"""
Example usage of the EPPO Code Lookup functionality.

This script demonstrates how to use the EPPOLookup class programmatically
to find EPPO codes by commodity names.
"""

from eppo_lookup import EPPOLookup

def main():
    """Example usage of the EPPO lookup functionality."""
    
    # Initialize the lookup utility
    lookup = EPPOLookup()
    
    print("EPPO Code Lookup Examples")
    print("=" * 50)
    
    # Example 1: Exact commodity lookup
    print("\n1. Exact Commodity Name Lookup:")
    print("-" * 40)
    
    commodity_name = "x Aranthera"
    results = lookup.lookup_by_commodity_name(commodity_name, exact_match=True)
    
    if results:
        print(f"Found {len(results)} result(s) for '{commodity_name}':")
        for commodity, eppo_code, commodity_code, description in results:
            print(f"  - EPPO Code: {eppo_code}")
            print(f"    Commodity Code: {commodity_code}")
            print(f"    Description: {description[:60]}...")
    else:
        print(f"No exact match found for '{commodity_name}'")
    
    # Example 2: Partial search
    print("\n2. Partial Search:")
    print("-" * 40)
    
    search_term = "rose"
    results = lookup.search_commodities(search_term, limit=5)
    
    if results:
        print(f"Found {len(results)} result(s) containing '{search_term}' (showing first 5):")
        for commodity, eppo_code, commodity_code, description in results:
            print(f"  - {commodity} → {eppo_code}")
    else:
        print(f"No results found containing '{search_term}'")
    
    # Example 3: Lookup by EPPO code
    print("\n3. Reverse Lookup by EPPO Code:")
    print("-" * 40)
    
    eppo_code = "1AABG"
    results = lookup.lookup_by_eppo_code(eppo_code)
    
    if results:
        print(f"Found {len(results)} result(s) for EPPO code '{eppo_code}':")
        for commodity, eppo, commodity_code, description in results:
            print(f"  - {commodity}")
            print(f"    Commodity Code: {commodity_code}")
    else:
        print(f"No results found for EPPO code '{eppo_code}'")
    
    # Example 4: Database statistics
    print("\n4. Database Statistics:")
    print("-" * 40)
    
    stats = lookup.get_stats()
    print(f"Total records: {stats['total_records']:,}")
    print(f"Unique commodities: {stats['unique_commodities']:,}")
    print(f"Unique EPPO codes: {stats['unique_eppo_codes']:,}")
    print(f"Database size: {stats['database_size_mb']} MB")
    
    # Example 5: Programmatic usage for batch processing
    print("\n5. Batch Processing Example:")
    print("-" * 40)
    
    commodity_list = ["x Aranthera", "rose", "apple", "wheat"]
    
    for commodity in commodity_list:
        results = lookup.lookup_by_commodity_name(commodity, exact_match=False)
        if results:
            # Get the first result
            first_result = results[0]
            print(f"{commodity} → {first_result[1]} (found {len(results)} matches)")
        else:
            print(f"{commodity} → No matches found")

if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError:
        print("Error: Database not found!")
        print("Please run 'python csv_to_sqlite_eppo.py' first to create the database.")
    except Exception as e:
        print(f"Error: {e}")
