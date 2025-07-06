#!/usr/bin/env python3
"""
Test script to verify the forecast/actual system
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from datetime import datetime, timezone, timedelta
from carbon_intensity_api import CarbonIntensityAPI
from database import Database

def test_forecast_system():
    """Test the forecast/actual system"""
    
    api = CarbonIntensityAPI()
    db = Database()
    
    print("Testing forecast/actual system")
    print("=" * 50)
    
    # Test 1: Fetch recent data (should have actuals)
    print("\n1. Testing recent data (should have actuals):")
    recent_start = datetime.now(timezone.utc) - timedelta(hours=2)
    recent_end = datetime.now(timezone.utc) - timedelta(hours=1)
    
    recent_data = api.get_intensity_data(recent_start, recent_end)
    print(f"Retrieved {len(recent_data)} recent data points")
    
    if recent_data:
        for i, point in enumerate(recent_data[:3]):
            data_type = "forecast" if point['is_forecast'] else "actual"
            print(f"  {i+1}. {point['timestamp']} = {point['emissions']} ({data_type})")
    
    # Test 2: Fetch old data (should have forecasts)
    print("\n2. Testing old data (should have forecasts):")
    old_start = datetime(2022, 3, 26, 18, 30, tzinfo=timezone.utc)
    old_end = datetime(2022, 3, 26, 19, 0, tzinfo=timezone.utc)
    
    old_data = api.get_intensity_data(old_start, old_end)
    print(f"Retrieved {len(old_data)} old data points")
    
    if old_data:
        for i, point in enumerate(old_data[:3]):
            data_type = "forecast" if point['is_forecast'] else "actual"
            print(f"  {i+1}. {point['timestamp']} = {point['emissions']} ({data_type})")
    
    # Test 3: Test database insertion
    print("\n3. Testing database insertion:")
    if recent_data:
        test_point = recent_data[0]
        success = db.insert_carbon_intensity_data(
            timestamp=test_point['timestamp'],
            emissions=test_point['emissions'],
            is_forecast=test_point['is_forecast']
        )
        print(f"Inserted test point: {success}")
    
    # Test 4: Test getting recent forecast records
    print("\n4. Testing recent forecast records retrieval:")
    forecast_records = db.get_recent_forecast_records(hours=24)
    print(f"Found {len(forecast_records)} recent forecast records")

    # Test 5: Known period with only forecasts (2018-11-23T15:30Z to 2018-11-24T14:00Z)
    print("\n5. Testing known period with only forecasts (2018-11-23T15:30Z to 2018-11-24T14:00Z):")
    known_start = datetime(2018, 11, 23, 15, 30, tzinfo=timezone.utc)
    known_end = datetime(2018, 11, 24, 14, 0, tzinfo=timezone.utc)
    known_data = api.get_intensity_data(known_start, known_end)
    print(f"Retrieved {len(known_data)} data points for known period")
    if known_data:
        for i, point in enumerate(known_data):
            data_type = "forecast" if point['is_forecast'] else "actual"
            print(f"  {i+1}. {point['timestamp']} = {point['emissions']} ({data_type})")
    else:
        print("No data points retrieved for known period!")

if __name__ == "__main__":
    test_forecast_system() 