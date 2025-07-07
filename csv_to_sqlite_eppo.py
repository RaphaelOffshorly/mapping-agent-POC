#!/usr/bin/env python3
"""
EPPO Codes CSV to SQLite Converter

Converts the large EPPO Codes.csv file to a SQLite database with optimized indexing
for commodity name lookups.
"""

import sqlite3
import csv
import os
import time
from pathlib import Path

def create_database_schema(db_path):
    """Create the SQLite database schema with proper table structure."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create the main table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS eppo_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_name TEXT NOT NULL,
            eppo_code TEXT NOT NULL,
            commodity_code TEXT,
            commodity_code_description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    return conn

def create_indexes(conn):
    """Create optimized indexes after data insertion for better performance."""
    cursor = conn.cursor()
    
    print("Creating indexes...")
    
    # Primary index for commodity name lookups (main use case)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_commodity_name ON eppo_codes(commodity_name)")
    
    # Secondary indexes for other potential lookups
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_eppo_code ON eppo_codes(eppo_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_commodity_code ON eppo_codes(commodity_code)")
    
    # Composite index for commodity name + eppo code queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_commodity_eppo ON eppo_codes(commodity_name, eppo_code)")
    
    conn.commit()
    print("Indexes created successfully!")

def convert_csv_to_sqlite(csv_file_path, db_path, chunk_size=10000):
    """
    Convert CSV to SQLite database with chunked processing for large files.
    
    Args:
        csv_file_path: Path to the EPPO Codes.csv file
        db_path: Path where the SQLite database will be created
        chunk_size: Number of rows to process at once
    """
    # Ensure database directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed existing database: {db_path}")
    
    print(f"Creating database: {db_path}")
    conn = create_database_schema(db_path)
    cursor = conn.cursor()
    
    print(f"Starting conversion of {csv_file_path}")
    start_time = time.time()
    
    total_rows = 0
    chunk_data = []
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8', newline='') as csvfile:
            # Create CSV reader
            csv_reader = csv.reader(csvfile)
            
            for row_num, row in enumerate(csv_reader, 1):
                if len(row) != 4:
                    print(f"Warning: Row {row_num} has {len(row)} columns instead of 4, skipping...")
                    continue
                
                # Clean and prepare data
                commodity_name = row[0].strip()
                eppo_code = row[1].strip()
                commodity_code = row[2].strip() if row[2] else None
                commodity_code_description = row[3].strip() if row[3] else None
                
                chunk_data.append((commodity_name, eppo_code, commodity_code, commodity_code_description))
                
                # Process in chunks for better memory management
                if len(chunk_data) >= chunk_size:
                    cursor.executemany("""
                        INSERT INTO eppo_codes 
                        (commodity_name, eppo_code, commodity_code, commodity_code_description)
                        VALUES (?, ?, ?, ?)
                    """, chunk_data)
                    
                    total_rows += len(chunk_data)
                    chunk_data = []
                    
                    if total_rows % 50000 == 0:
                        print(f"Processed {total_rows:,} rows...")
                        conn.commit()  # Commit periodically
            
            # Process remaining data
            if chunk_data:
                cursor.executemany("""
                    INSERT INTO eppo_codes 
                    (commodity_name, eppo_code, commodity_code, commodity_code_description)
                    VALUES (?, ?, ?, ?)
                """, chunk_data)
                total_rows += len(chunk_data)
        
        # Final commit
        conn.commit()
        
        # Create indexes after all data is inserted
        create_indexes(conn)
        
        # Optimize database
        print("Optimizing database...")
        cursor.execute("VACUUM")
        conn.commit()
        
        elapsed_time = time.time() - start_time
        
        print(f"\nConversion completed successfully!")
        print(f"Total rows processed: {total_rows:,}")
        print(f"Time elapsed: {elapsed_time:.2f} seconds")
        print(f"Database created: {db_path}")
        
        # Get database size
        db_size = os.path.getsize(db_path) / (1024 * 1024)  # MB
        print(f"Database size: {db_size:.2f} MB")
        
    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_file_path}")
        return False
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        return False
    finally:
        conn.close()
    
    return True

def verify_conversion(db_path):
    """Verify the conversion was successful by checking row counts and sample data."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check total rows
        cursor.execute("SELECT COUNT(*) FROM eppo_codes")
        total_rows = cursor.fetchone()[0]
        print(f"\nVerification Results:")
        print(f"Total rows in database: {total_rows:,}")
        
        # Check for any null values in critical fields
        cursor.execute("SELECT COUNT(*) FROM eppo_codes WHERE commodity_name IS NULL OR commodity_name = ''")
        null_names = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM eppo_codes WHERE eppo_code IS NULL OR eppo_code = ''")
        null_eppo = cursor.fetchone()[0]
        
        print(f"Rows with empty commodity names: {null_names}")
        print(f"Rows with empty EPPO codes: {null_eppo}")
        
        # Show sample data
        cursor.execute("SELECT commodity_name, eppo_code, commodity_code, commodity_code_description FROM eppo_codes LIMIT 5")
        sample_data = cursor.fetchall()
        
        print(f"\nSample data (first 5 rows):")
        for i, row in enumerate(sample_data, 1):
            print(f"{i}. {row[0]} | {row[1]} | {row[2]} | {row[3][:50]}...")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error during verification: {str(e)}")
        return False

if __name__ == "__main__":
    # File paths
    csv_file = "EPPO Codes.csv"
    db_file = "database/eppo_codes.db"
    
    print("EPPO Codes CSV to SQLite Converter")
    print("=" * 50)
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found!")
        exit(1)
    
    # Convert CSV to SQLite
    success = convert_csv_to_sqlite(csv_file, db_file)
    
    if success:
        # Verify the conversion
        verify_conversion(db_file)
        print(f"\nConversion complete! Database ready at: {db_file}")
    else:
        print("Conversion failed!")
        exit(1)
