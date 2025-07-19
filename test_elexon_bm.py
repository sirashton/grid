#!/usr/bin/env python3
"""
Test script for Elexon BM API integration
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from datetime import datetime, timezone, timedelta
from elexon_bm_api import ElexonBMAPI
from database import Database

def test_elexon_bm_api():
    """Test the Elexon BM API integration"""
    
    api = ElexonBMAPI()
    db = Database()
    
    print("Testing Elexon BM API Integration")
    print("=" * 50)
    
    # Test 1: API Health Check
    print("\n1. Testing API Health Check:")
    health_ok = api.check_health()
    if health_ok:
        print("✅ Elexon BM API is healthy")
    else:
        print("❌ Elexon BM API health check failed")
        return False
    
    # Test 2: Fetch Recent Data
    print("\n2. Testing Data Fetch:")
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=1)
    
    print(f"Fetching data from {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
    
    data_points = api.get_generation_data(start_time, end_time)
    
    if data_points:
        print(f"✅ Retrieved {len(data_points)} data points")
        
        # Show sample data
        print("\nSample data point:")
        sample = data_points[0]
        print(f"  Timestamp: {sample['timestamp']}")
        print(f"  Settlement Period: {sample['settlement_period']}")
        print(f"  Nuclear: {sample['nuclear']} MW")
        print(f"  Fossil Gas: {sample['fossil_gas']} MW")
        print(f"  Wind (Onshore): {sample['wind_onshore']} MW")
        print(f"  Wind (Offshore): {sample['wind_offshore']} MW")
        print(f"  Solar: {sample['solar']} MW")
        
        # Show which values are None vs 0.0
        print("\nData quality check:")
        for fuel_type, value in sample.items():
            if fuel_type not in ['timestamp', 'settlement_period']:
                if value is None:
                    print(f"  {fuel_type}: None (missing data)")
                elif value == 0.0:
                    print(f"  {fuel_type}: 0.0 (actual zero generation)")
                else:
                    print(f"  {fuel_type}: {value} MW")
    else:
        print("❌ No data points retrieved")
        return False
    
    # Test 3: Database Insertion
    print("\n3. Testing Database Insertion:")
    if data_points:
        inserted_count = 0
        for point in data_points[:5]:  # Test with first 5 points
            success = db.insert_generation_data(
                timestamp=point['timestamp'],
                settlement_period=point['settlement_period'],
                biomass=point['biomass'],
                fossil_gas=point['fossil_gas'],
                fossil_hard_coal=point['fossil_hard_coal'],
                fossil_oil=point['fossil_oil'],
                hydro_pumped_storage=point['hydro_pumped_storage'],
                hydro_run_of_river=point['hydro_run_of_river'],
                nuclear=point['nuclear'],
                other=point['other'],
                solar=point['solar'],
                wind_offshore=point['wind_offshore'],
                wind_onshore=point['wind_onshore']
            )
            if success:
                inserted_count += 1
        
        print(f"✅ Successfully inserted {inserted_count}/5 test data points")
        
        # Test retrieval
        latest_data = db.get_latest_generation_data(limit=1)
        if latest_data:
            print(f"✅ Latest record retrieved: {latest_data[0]['timestamp']}")
        else:
            print("❌ Could not retrieve latest record")
    
    # Test 4: Database Stats
    print("\n4. Testing Database Statistics:")
    stats = db.get_generation_stats()
    print(f"  Total records: {stats['total_records']}")
    print(f"  Earliest data: {stats['earliest_data']}")
    print(f"  Latest data: {stats['latest_data']}")
    print(f"  Healthy: {stats['healthy']}")
    
    print("\n🎉 Elexon BM API integration test completed successfully!")
    return True

if __name__ == "__main__":
    test_elexon_bm_api() 