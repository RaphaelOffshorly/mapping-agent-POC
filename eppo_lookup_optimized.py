#!/usr/bin/env python3
"""
Optimized EPPO Code Lookup Utility

This module provides optimized functions for EPPO code lookups using the redesigned database structure.
Specifically optimized for IPAFFS prefill batch operations.
"""

import psycopg2
import psycopg2.extras
import psycopg2.pool
import os
import time
from typing import List, Tuple, Optional, Dict, Union
from urllib.parse import urlparse
from dotenv import load_dotenv
from contextlib import contextmanager
from functools import lru_cache
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EPPOLookupOptimized:
    """Optimized EPPO lookup class using the redesigned database structure."""
    
    # Class-level connection pool
    _connection_pool = None
    _pool_lock = None
    
    def __init__(self, db_url: str = None, use_pool: bool = True, table_name: str = 'eppo_codes_optimized'):
        """
        Initialize the optimized EPPO lookup.
        
        Args:
            db_url: PostgreSQL connection URL
            use_pool: Whether to use connection pooling
            table_name: Name of the table to use (default: eppo_codes_optimized)
        """
        self.db_url = db_url or os.getenv('POSTGRESQL_URL')
        if not self.db_url:
            raise ValueError("PostgreSQL URL not provided and POSTGRESQL_URL environment variable not set")
        
        self.db_params = self._parse_postgresql_url(self.db_url)
        self.use_pool = use_pool
        self.table_name = table_name
        
        if self.use_pool:
            self._init_connection_pool()
    
    def _init_connection_pool(self):
        """Initialize the connection pool (thread-safe)."""
        import threading
        
        if EPPOLookupOptimized._pool_lock is None:
            EPPOLookupOptimized._pool_lock = threading.Lock()
        
        with EPPOLookupOptimized._pool_lock:
            if EPPOLookupOptimized._connection_pool is None:
                try:
                    EPPOLookupOptimized._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                        2,   # minimum connections
                        25,  # maximum connections
                        **self.db_params
                    )
                    logger.info("PostgreSQL connection pool initialized successfully")
                except Exception as e:
                    logger.error(f"Failed to initialize connection pool: {e}")
                    self.use_pool = False
    
    def _parse_postgresql_url(self, url: str) -> dict:
        """Parse PostgreSQL URL and return connection parameters."""
        parsed = urlparse(url)
        return {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path[1:],
            'user': parsed.username,
            'password': parsed.password
        }
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        if self.use_pool and EPPOLookupOptimized._connection_pool is not None:
            try:
                conn = EPPOLookupOptimized._connection_pool.getconn()
                try:
                    yield conn
                finally:
                    EPPOLookupOptimized._connection_pool.putconn(conn)
            except Exception as e:
                logger.error(f"Failed to get connection from pool: {e}")
                # Fallback to direct connection
                conn = psycopg2.connect(**self.db_params)
                try:
                    yield conn
                finally:
                    conn.close()
        else:
            conn = psycopg2.connect(**self.db_params)
            try:
                yield conn
            finally:
                conn.close()
    
    def exact_lookup(self, commodity_name: str) -> List[Tuple]:
        """
        Optimized exact lookup using hash index.
        
        Args:
            commodity_name: The commodity name to search for
            
        Returns:
            List of tuples containing (commodity_name, eppo_code, commodity_code, description)
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT commodity_name, eppo_code, commodity_code, commodity_code_description
                    FROM {self.table_name}
                    WHERE commodity_name_normalized = normalize_commodity_name(%s)
                    ORDER BY commodity_name
                """
                cursor.execute(query, (commodity_name,))
                return cursor.fetchall()
    
    def partial_lookup(self, commodity_name: str, limit: int = 50) -> List[Tuple]:
        """
        Optimized partial lookup using GIN index.
        
        Args:
            commodity_name: The commodity name to search for
            limit: Maximum number of results
            
        Returns:
            List of tuples containing (commodity_name, eppo_code, commodity_code, description)
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT commodity_name, eppo_code, commodity_code, commodity_code_description
                    FROM {self.table_name}
                    WHERE commodity_name_normalized LIKE '%' || normalize_commodity_name(%s) || '%'
                    ORDER BY commodity_name
                    LIMIT %s
                """
                cursor.execute(query, (commodity_name, limit))
                return cursor.fetchall()
    
    def genus_lookup(self, genus: str) -> List[Tuple]:
        """
        Optimized genus-based lookup for IPAFFS pattern.
        
        Args:
            genus: The genus name to search for
            
        Returns:
            List of tuples containing (commodity_name, eppo_code, commodity_code, description)
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT commodity_name, eppo_code, commodity_code, commodity_code_description
                    FROM {self.table_name}
                    WHERE genus = lower(%s)
                    ORDER BY commodity_name
                """
                cursor.execute(query, (genus,))
                return cursor.fetchall()
    
    def batch_exact_lookup(self, commodity_names: List[str]) -> Dict[str, List[Tuple]]:
        """
        Optimized batch exact lookup using the stored function.
        
        Args:
            commodity_names: List of commodity names to search for
            
        Returns:
            Dictionary mapping commodity names to their lookup results
        """
        if not commodity_names:
            return {}
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT * FROM batch_lookup_commodities(%s, TRUE)"
                cursor.execute(query, (commodity_names,))
                results = cursor.fetchall()
                
                # Organize results by input name
                result_dict = {name: [] for name in commodity_names}
                
                for result in results:
                    input_name = result[0]
                    if result[1] is not None:  # Only add if there's a match
                        match_result = result[1:]  # Remove input_name from result
                        result_dict[input_name].append(match_result)
                
                return result_dict
    
    def batch_partial_lookup(self, commodity_names: List[str]) -> Dict[str, List[Tuple]]:
        """
        Optimized batch partial lookup using the stored function.
        
        Args:
            commodity_names: List of commodity names to search for
            
        Returns:
            Dictionary mapping commodity names to their lookup results
        """
        if not commodity_names:
            return {}
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT * FROM batch_lookup_commodities(%s, FALSE)"
                cursor.execute(query, (commodity_names,))
                results = cursor.fetchall()
                
                # Organize results by input name
                result_dict = {name: [] for name in commodity_names}
                
                for result in results:
                    input_name = result[0]
                    if result[1] is not None:  # Only add if there's a match
                        match_result = result[1:]  # Remove input_name from result
                        result_dict[input_name].append(match_result)
                
                return result_dict
    
    def batch_ipaffs_lookup(self, commodity_names: List[str]) -> Dict[str, Tuple[str, List[Tuple]]]:
        """
        Optimized batch IPAFFS lookup using the specialized stored function.
        
        Args:
            commodity_names: List of commodity names to search for
            
        Returns:
            Dictionary mapping commodity names to (eppo_code, results) tuples
        """
        if not commodity_names:
            return {}
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT * FROM batch_ipaffs_lookup(%s)"
                cursor.execute(query, (commodity_names,))
                results = cursor.fetchall()
                
                # Organize results by input name
                result_dict = {}
                
                for result in results:
                    input_name = result[0]
                    eppo_code = result[1]
                    commodity_name = result[2]
                    commodity_code = result[3]
                    commodity_code_description = result[4]
                    lookup_method = result[5]
                    
                    if input_name not in result_dict:
                        result_dict[input_name] = (eppo_code, [])
                    
                    # Add to results list
                    result_dict[input_name][1].append((
                        commodity_name, eppo_code, commodity_code, commodity_code_description
                    ))
                
                return result_dict
    
    def enhanced_lookup_ipaffs(self, genus_species: str) -> Tuple[str, List[Tuple]]:
        """
        Enhanced single lookup for IPAFFS (wrapper around batch function).
        
        Args:
            genus_species: The genus and species name
            
        Returns:
            Tuple of (eppo_code, results)
        """
        batch_results = self.batch_ipaffs_lookup([genus_species])
        if genus_species in batch_results:
            return batch_results[genus_species]
        else:
            return "", []
    
    def get_stats(self) -> Dict[str, Union[int, str]]:
        """
        Get optimized database statistics.
        
        Returns:
            Dictionary with database statistics
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Total records
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                total_records = cursor.fetchone()[0]
                
                # Unique commodity names
                cursor.execute(f"SELECT COUNT(DISTINCT commodity_name_normalized) FROM {self.table_name}")
                unique_commodities = cursor.fetchone()[0]
                
                # Unique EPPO codes
                cursor.execute(f"SELECT COUNT(DISTINCT eppo_code) FROM {self.table_name}")
                unique_eppo_codes = cursor.fetchone()[0]
                
                # Unique genera
                cursor.execute(f"SELECT COUNT(DISTINCT genus) FROM {self.table_name}")
                unique_genera = cursor.fetchone()[0]
                
                return {
                    'total_records': total_records,
                    'unique_commodities': unique_commodities,
                    'unique_eppo_codes': unique_eppo_codes,
                    'unique_genera': unique_genera,
                    'database_type': 'PostgreSQL Optimized',
                    'table_name': self.table_name
                }
    
    def get_performance_stats(self) -> Dict[str, List[Tuple]]:
        """
        Get performance statistics from monitoring views.
        
        Returns:
            Dictionary with performance statistics
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Index usage stats
                cursor.execute("SELECT * FROM eppo_index_usage")
                index_usage = cursor.fetchall()
                
                # Query performance stats (if pg_stat_statements is available)
                try:
                    cursor.execute("SELECT * FROM eppo_query_stats LIMIT 10")
                    query_stats = cursor.fetchall()
                except:
                    query_stats = []
                
                return {
                    'index_usage': index_usage,
                    'query_stats': query_stats
                }
    
    def refresh_materialized_view(self):
        """Refresh the materialized view for frequently accessed data."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT refresh_eppo_frequent_lookups()")
                conn.commit()
    
    def update_statistics(self):
        """Update database statistics for optimal query planning."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT update_eppo_statistics()")
                conn.commit()
    
    def benchmark_performance(self, test_names: List[str], iterations: int = 10) -> Dict[str, float]:
        """
        Benchmark the optimized lookup methods.
        
        Args:
            test_names: List of commodity names to test with
            iterations: Number of iterations to run
            
        Returns:
            Dictionary with performance metrics
        """
        results = {}
        
        # Test individual exact lookups
        start_time = time.time()
        for _ in range(iterations):
            for name in test_names:
                self.exact_lookup(name)
        individual_time = (time.time() - start_time) / iterations
        results['individual_exact'] = individual_time
        
        # Test batch exact lookups
        start_time = time.time()
        for _ in range(iterations):
            self.batch_exact_lookup(test_names)
        batch_time = (time.time() - start_time) / iterations
        results['batch_exact'] = batch_time
        
        # Test IPAFFS batch lookups
        start_time = time.time()
        for _ in range(iterations):
            self.batch_ipaffs_lookup(test_names)
        ipaffs_time = (time.time() - start_time) / iterations
        results['batch_ipaffs'] = ipaffs_time
        
        return results
    
    def health_check(self) -> Dict[str, Union[bool, str]]:
        """
        Perform a health check on the database and connection.
        
        Returns:
            Dictionary with health check results
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Test basic connectivity
                    cursor.execute("SELECT 1")
                    
                    # Test table existence
                    cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = %s
                        )
                    """, (self.table_name,))
                    table_exists = cursor.fetchone()[0]
                    
                    # Test function existence
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.routines 
                            WHERE routine_name = 'batch_ipaffs_lookup'
                        )
                    """)
                    function_exists = cursor.fetchone()[0]
                    
                    return {
                        'connection': True,
                        'table_exists': table_exists,
                        'functions_exist': function_exists,
                        'status': 'healthy' if table_exists and function_exists else 'degraded'
                    }
        except Exception as e:
            return {
                'connection': False,
                'error': str(e),
                'status': 'unhealthy'
            }

# Convenience function for backward compatibility
def get_optimized_lookup(db_url: str = None) -> EPPOLookupOptimized:
    """
    Get an optimized EPPO lookup instance.
    
    Args:
        db_url: PostgreSQL connection URL
        
    Returns:
        EPPOLookupOptimized instance
    """
    return EPPOLookupOptimized(db_url)

# Performance testing utilities
def run_performance_comparison(original_lookup, optimized_lookup, test_names: List[str]):
    """
    Compare performance between original and optimized lookups.
    
    Args:
        original_lookup: Original EPPOLookup instance
        optimized_lookup: Optimized EPPOLookupOptimized instance
        test_names: List of commodity names to test
    """
    print("Performance Comparison: Original vs Optimized")
    print("=" * 60)
    
    # Test original batch lookup
    start_time = time.time()
    original_results = original_lookup.batch_lookup_by_commodity_names(test_names)
    original_time = time.time() - start_time
    
    # Test optimized batch lookup
    start_time = time.time()
    optimized_results = optimized_lookup.batch_exact_lookup(test_names)
    optimized_time = time.time() - start_time
    
    print(f"Original batch lookup: {original_time:.4f} seconds")
    print(f"Optimized batch lookup: {optimized_time:.4f} seconds")
    
    if original_time > 0:
        speedup = original_time / optimized_time
        print(f"Speedup: {speedup:.2f}x faster")
    
    # Test IPAFFS specific lookup
    start_time = time.time()
    ipaffs_results = optimized_lookup.batch_ipaffs_lookup(test_names)
    ipaffs_time = time.time() - start_time
    
    print(f"IPAFFS optimized lookup: {ipaffs_time:.4f} seconds")
    
    if original_time > 0:
        ipaffs_speedup = original_time / ipaffs_time
        print(f"IPAFFS speedup: {ipaffs_speedup:.2f}x faster")

def main():
    """Main function for command-line usage."""
    import sys
    
    if len(sys.argv) < 2:
        print("Optimized EPPO Code Lookup Utility")
        print("Usage:")
        print("  python eppo_lookup_optimized.py <commodity_name>     - Exact lookup")
        print("  python eppo_lookup_optimized.py batch <name1> <name2> ... - Batch lookup")
        print("  python eppo_lookup_optimized.py ipaffs <name1> <name2> ... - IPAFFS batch lookup")
        print("  python eppo_lookup_optimized.py stats                - Database statistics")
        print("  python eppo_lookup_optimized.py health               - Health check")
        print("  python eppo_lookup_optimized.py perf <commodity_name> - Performance test")
        return
    
    try:
        lookup = EPPOLookupOptimized()
        
        if sys.argv[1] == "stats":
            stats = lookup.get_stats()
            print("Optimized Database Statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value:,}" if isinstance(value, int) else f"  {key}: {value}")
        
        elif sys.argv[1] == "health":
            health = lookup.health_check()
            print("Health Check Results:")
            for key, value in health.items():
                print(f"  {key}: {value}")
        
        elif sys.argv[1] == "perf" and len(sys.argv) > 2:
            test_name = sys.argv[2]
            print(f"Performance test for '{test_name}':")
            
            # Test different lookup methods
            start_time = time.time()
            exact_results = lookup.exact_lookup(test_name)
            exact_time = time.time() - start_time
            
            start_time = time.time()
            ipaffs_result = lookup.enhanced_lookup_ipaffs(test_name)
            ipaffs_time = time.time() - start_time
            
            print(f"  Exact lookup: {exact_time:.4f}s ({len(exact_results)} results)")
            print(f"  IPAFFS lookup: {ipaffs_time:.4f}s (EPPO: {ipaffs_result[0]}, {len(ipaffs_result[1])} results)")
        
        elif sys.argv[1] == "batch" and len(sys.argv) > 2:
            commodity_names = sys.argv[2:]
            print(f"Batch lookup for {len(commodity_names)} commodities:")
            
            start_time = time.time()
            results = lookup.batch_exact_lookup(commodity_names)
            elapsed_time = time.time() - start_time
            
            print(f"  Completed in {elapsed_time:.4f} seconds")
            for name, matches in results.items():
                print(f"  {name}: {len(matches)} matches")
        
        elif sys.argv[1] == "ipaffs" and len(sys.argv) > 2:
            commodity_names = sys.argv[2:]
            print(f"IPAFFS batch lookup for {len(commodity_names)} commodities:")
            
            start_time = time.time()
            results = lookup.batch_ipaffs_lookup(commodity_names)
            elapsed_time = time.time() - start_time
            
            print(f"  Completed in {elapsed_time:.4f} seconds")
            for name, (eppo_code, matches) in results.items():
                print(f"  {name}: EPPO={eppo_code}, {len(matches)} matches")
        
        else:
            # Single exact lookup
            commodity_name = " ".join(sys.argv[1:])
            results = lookup.exact_lookup(commodity_name)
            
            if results:
                print(f"Exact matches for '{commodity_name}':")
                for i, (name, eppo, code, desc) in enumerate(results, 1):
                    print(f"  {i}. {name} -> {eppo} ({code})")
            else:
                print(f"No exact matches for '{commodity_name}'. Trying IPAFFS lookup...")
                eppo_code, results = lookup.enhanced_lookup_ipaffs(commodity_name)
                print(f"IPAFFS result: EPPO={eppo_code}, {len(results)} matches")
    
    except Exception as e:
        print(f"Error: {e}")
        logger.exception("Error in main function")

if __name__ == "__main__":
    main()
