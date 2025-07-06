#!/usr/bin/env python3
"""
Test script to verify the API fix for null actual values
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from datetime import datetime, timezone
from carbon_intensity_api import CarbonIntensityAPI

def test_api_fix():
    """Test the API fix with data that has null actual values"""
    
    api = CarbonIntensityAPI()
    
    # Test with data from 2022 (has null actual values)
    start_time = datetime(2022, 3, 26, 18, 30, tzinfo=timezone.utc)
    end_time = datetime(2022, 3, 26, 23, 0, tzinfo=timezone.utc)
    
    print(f"Testing API fix with data from {start_time} to {end_time}")
    print("=" * 60)
    
    data_points = api.get_intensity_data(start_time, end_time)
    
    print(f"\nRetrieved {len(data_points)} data points")
    
    if data_points:
        print("\nFirst few data points:")
        for i, point in enumerate(data_points[:5]):
            print(f"  {i+1}. {point['timestamp']} = {point['emissions']} gCO2/kWh")
        
        print(f"\nLast few data points:")
        for i, point in enumerate(data_points[-5:]):
            print(f"  {len(data_points)-4+i}. {point['timestamp']} = {point['emissions']} gCO2/kWh")
    else:
        print("No data points retrieved - fix didn't work!")

if __name__ == "__main__":
    test_api_fix() 