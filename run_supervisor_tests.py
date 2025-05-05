#!/usr/bin/env python3
"""
Runner script for CSV Edit Supervisor Agent tests.
This script provides a more flexible way to run tests with better output formatting.
"""

import argparse
import unittest
import sys
import logging
from test_csv_edit_supervisor import TestCSVEditSupervisor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    """Run the CSV Edit Supervisor Agent tests with flexible options."""
    parser = argparse.ArgumentParser(description='Run CSV Edit Supervisor Agent tests')
    
    # Add command-line arguments
    parser.add_argument(
        '--test', '-t', 
        help='Specific test to run (e.g., "test_1_add_new_row")', 
        default=None
    )
    parser.add_argument(
        '--category', '-c', 
        help='Test category to run (e.g., "crud", "excel", "cleaning", "advanced")',
        choices=['crud', 'cleaning', 'excel', 'advanced'],
        default=None
    )
    parser.add_argument(
        '--list', '-l', 
        help='List available tests', 
        action='store_true'
    )
    parser.add_argument(
        '--verbose', '-v', 
        help='Increase output verbosity', 
        action='store_true'
    )
    
    args = parser.parse_args()
    
    # Get all test methods
    test_loader = unittest.TestLoader()
    test_methods = [method for method in dir(TestCSVEditSupervisor) if method.startswith('test_')]
    
    # Create test categories mapping
    test_categories = {
        'crud': [method for method in test_methods if any(x in method for x in ['add_', 'update_', 'delete_'])],
        'cleaning': [method for method in test_methods if any(x in method for x in ['remove_duplicates', 'outliers'])],
        'excel': [method for method in test_methods if any(x in method for x in [
            'summation', 'concatenation', 'conditional', 'string_operations', 
            'statistical', 'pivot', 'aggregate'
        ])],
        'advanced': [method for method in test_methods if any(x in method for x in [
            'multiple_operations', 'sort', 'filter', 'verification'
        ])]
    }
    
    # List tests if requested
    if args.list:
        print("\nAvailable tests:")
        for category, methods in test_categories.items():
            print(f"\n{category.upper()}:")
            for method in sorted(methods):
                test = getattr(TestCSVEditSupervisor, method)
                print(f"  {method}: {test.__doc__}")
        return
    
    # Set up the test suite
    suite = unittest.TestSuite()
    
    # Add tests based on the command-line arguments
    if args.test:
        # Run a specific test
        if args.test in test_methods:
            test_case = test_loader.loadTestsFromName(f"test_csv_edit_supervisor.TestCSVEditSupervisor.{args.test}")
            suite.addTest(test_case)
        else:
            print(f"Error: Test '{args.test}' not found")
            return
    elif args.category:
        # Run tests in a specific category
        if args.category in test_categories:
            for method in test_categories[args.category]:
                test_case = test_loader.loadTestsFromName(f"test_csv_edit_supervisor.TestCSVEditSupervisor.{method}")
                suite.addTest(test_case)
        else:
            print(f"Error: Category '{args.category}' not found")
            return
    else:
        # Run all tests
        for method in sorted(test_methods):
            test_case = test_loader.loadTestsFromName(f"test_csv_edit_supervisor.TestCSVEditSupervisor.{method}")
            suite.addTest(test_case)
    
    # Run the tests
    verbosity = 2 if args.verbose else 1
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    
    # Return a non-zero exit code if there were failures
    sys.exit(0 if result.wasSuccessful() else 1)

if __name__ == "__main__":
    main()
