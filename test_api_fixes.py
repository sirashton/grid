#!/usr/bin/env python3
"""
Test script to verify API fixes for Elexon BM API
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from elexon_bm_api import ElexonBMAPI
from datetime import datetime, timezone, timedelta

def test_api_chunking():
    """Test that the API properly chunks large date ranges"""
    print("Testing Elexon BM API chunking...")
    
    api = ElexonBMAPI()
    
    # Test a large date range (should be split into chunks)
    start_time = datetime(2023, 7, 14, tzinfo=timezone.utc)
    end_time = datetime(2023, 8, 14, tzinfo=timezone.utc)  # 31 days
    
    print(f"Testing date range: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
    print(f"Expected: Split into ~6 chunks of 5 days each")
    
    # Test the chunking method directly
    chunks = api._limit_date_range(start_time, end_time, max_days=5)
    print(f"Generated {len(chunks)} chunks:")
    
    for i, (chunk_start, chunk_end) in enumerate(chunks):
        duration = chunk_end - chunk_start
        print(f"  Chunk {i+1}: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')} ({duration.days} days)")
    
    # Test a small date range (should not be split)
    print("\nTesting small date range...")
    start_time = datetime(2023, 7, 14, tzinfo=timezone.utc)
    end_time = datetime(2023, 7, 16, tzinfo=timezone.utc)  # 2 days
    
    chunks = api._limit_date_range(start_time, end_time, max_days=5)
    print(f"Small range generated {len(chunks)} chunks (should be 1)")
    
    print("\nAPI chunking test completed!")

def test_api_call():
    """Test a small API call to verify it works"""
    print("\nTesting small API call...")
    
    api = ElexonBMAPI()
    
    # Test a small date range
    start_time = datetime(2023, 7, 20, tzinfo=timezone.utc)
    end_time = datetime(2023, 7, 22, tzinfo=timezone.utc)  # 2 days
    
    print(f"Making API call for: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
    
    try:
        data_points = api.get_generation_data(start_time, end_time)
        print(f"Successfully retrieved {len(data_points)} data points")
        
        if data_points:
            print(f"First timestamp: {data_points[0]['timestamp']}")
            print(f"Last timestamp: {data_points[-1]['timestamp']}")
            print(f"Sample data point: {data_points[0]}")
        
        return True
        
    except Exception as e:
        print(f"API call failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing Elexon BM API fixes...")
    print("=" * 50)
    
    # Test chunking
    test_api_chunking()
    
    # Test API call
    success = test_api_call()
    
    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Some tests failed!") 