#!/usr/bin/env python3
"""
Test the Elexon BM API client directly
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from elexon_bm_api import ElexonBMAPI
from datetime import datetime, timezone
import logging

# Set up debug logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_api_client():
    """Test the API client directly"""
    print("Testing Elexon BM API Client Directly")
    print("=" * 50)
    
    # Test the specific timestamp that failed
    target_time = datetime(2023, 7, 15, 4, 0, 0, tzinfo=timezone.utc)
    
    print(f"Testing timestamp: {target_time.isoformat()}")
    
    # Create API client
    api = ElexonBMAPI()
    
    # Call the API
    data_points = api.get_generation_data(target_time, target_time)
    
    print(f"API client returned {len(data_points)} data points")
    
    if data_points:
        print(f"First data point: {data_points[0]}")
    else:
        print("No data points returned")

if __name__ == "__main__":
    test_api_client() 