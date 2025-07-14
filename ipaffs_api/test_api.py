#!/usr/bin/env python3
"""
Simple test script for the IPAFFS REST API.
Tests basic functionality without requiring external files.
"""

import requests
import json
import time
from typing import Dict, Any

# API base URL
BASE_URL = "http://localhost:5001/api/v1"

def test_health_check():
    """Test the health check endpoint."""
    print("Testing health check...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

def test_get_schema():
    """Test getting the IPAFFS schema."""
    print("\nTesting schema retrieval...")
    try:
        response = requests.get(f"{BASE_URL}/ipaffs/schema")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            schema = response.json()
            print(f"Schema has {len(schema.get('data', {}).get('schema', {}).get('properties', {}))} properties")
            return True
        else:
            print(f"Error: {response.json()}")
            return False
    except Exception as e:
        print(f"Schema test failed: {e}")
        return False

def test_compatibility_check():
    """Test IPAFFS compatibility checking."""
    print("\nTesting compatibility check...")
    
    # Test data with some IPAFFS-compatible headers
    test_csv_data = {
        "headers": ["Genus and Species", "Commodity code", "EPPO code", "Variety"],
        "data": [
            {
                "Genus and Species": "Rosa hybrid",
                "Commodity code": "0603110000",
                "EPPO code": "ROSHY",
                "Variety": "Red Rose"
            }
        ]
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/ipaffs/check-compatibility",
            json={"csv_data": test_csv_data}
        )
        print(f"Status: {response.status_code}")
        result = response.json()
        print(f"Compatible: {result.get('data', {}).get('compatible', False)}")
        print(f"Matched headers: {result.get('data', {}).get('total_matched', 0)}")
        return response.status_code == 200
    except Exception as e:
        print(f"Compatibility check failed: {e}")
        return False

def test_csv_session_management():
    """Test CSV data session management."""
    print("\nTesting session management...")
    
    test_csv_data = {
        "headers": ["Test Column 1", "Test Column 2"],
        "data": [
            {"Test Column 1": "Value 1", "Test Column 2": "Value 2"}
        ]
    }
    
    try:
        # Test compatibility check to create a session
        response = requests.post(
            f"{BASE_URL}/ipaffs/check-compatibility",
            json={"csv_data": test_csv_data}
        )
        
        if response.status_code != 200:
            print("Failed to create session via compatibility check")
            return False
        
        session_id = response.json().get('meta', {}).get('session_id')
        if not session_id:
            print("No session ID returned")
            return False
        
        print(f"Created session: {session_id}")
        
        # Test getting CSV data
        response = requests.get(f"{BASE_URL}/ipaffs/csv-data/{session_id}")
        if response.status_code == 200:
            print("Successfully retrieved CSV data from session")
        else:
            print(f"Failed to retrieve CSV data: {response.status_code}")
            return False
        
        # Test updating CSV data
        updated_csv_data = {
            "headers": ["Updated Column"],
            "data": [{"Updated Column": "Updated Value"}]
        }
        
        response = requests.post(
            f"{BASE_URL}/ipaffs/csv-data/{session_id}",
            json={"csv_data": updated_csv_data}
        )
        
        if response.status_code == 200:
            print("Successfully updated CSV data")
        else:
            print(f"Failed to update CSV data: {response.status_code}")
            return False
        
        # Test deleting session
        response = requests.delete(f"{BASE_URL}/ipaffs/csv-data/{session_id}")
        if response.status_code == 200:
            print("Successfully deleted session")
            return True
        else:
            print(f"Failed to delete session: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Session management test failed: {e}")
        return False

def test_export_csv():
    """Test CSV export functionality."""
    print("\nTesting CSV export...")
    
    test_csv_data = {
        "headers": ["Name", "Age", "City"],
        "data": [
            {"Name": "John", "Age": "30", "City": "New York"},
            {"Name": "Jane", "Age": "25", "City": "London"}
        ]
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/ipaffs/export-csv",
            json={"csv_data": test_csv_data}
        )
        
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            csv_content = result.get('data', {}).get('csv_content', '')
            print(f"CSV export successful, content length: {len(csv_content)} characters")
            print("First few lines of CSV:")
            for line in csv_content.split('\n')[:3]:
                print(f"  {line}")
            return True
        else:
            print(f"Export failed: {response.json()}")
            return False
            
    except Exception as e:
        print(f"CSV export test failed: {e}")
        return False

def run_all_tests():
    """Run all API tests."""
    print("Starting IPAFFS REST API Tests")
    print("=" * 50)
    
    tests = [
        ("Health Check", test_health_check),
        ("Schema Retrieval", test_get_schema),
        ("Compatibility Check", test_compatibility_check),
        ("Session Management", test_csv_session_management),
        ("CSV Export", test_export_csv)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'=' * 20} {test_name} {'=' * 20}")
        success = test_func()
        results.append((test_name, success))
        print(f"Result: {'PASS' if success else 'FAIL'}")
    
    print(f"\n{'=' * 50}")
    print("Test Summary:")
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"  {test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The IPAFFS REST API is working correctly.")
    else:
        print("❌ Some tests failed. Check the API server and try again.")
    
    return passed == total

if __name__ == "__main__":
    print("IPAFFS REST API Test Suite")
    print("Make sure the API server is running on localhost:5001")
    print("Start the server with: python ipaffs_api/app.py")
    input("Press Enter to start tests...")
    
    success = run_all_tests()
    exit(0 if success else 1)
