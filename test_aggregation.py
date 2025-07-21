#!/usr/bin/env python3
"""
Test the generation aggregation function with real data
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from database import Database
from datetime import datetime, timezone, timedelta
import json

def test_aggregation():
    """Test the aggregation function with real data"""
    print("Testing Generation Aggregation Function")
    print("=" * 50)
    
    # Initialize database
    db = Database()
    
    # Check if we have data
    stats = db.get_generation_stats()
    print(f"Database stats: {stats}")
    
    if not stats['healthy'] or stats['total_records'] == 0:
        print("No generation data found in database. Please run data collection first.")
        return
    
    # Test 1: Basic aggregation (last 24 hours, 1-hour bins)
    print("\n" + "="*30)
    print("Test 1: Basic aggregation (last 24 hours, 1-hour bins)")
    print("="*30)
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=24)
    
    result = db.get_generation_aggregated(
        start_time=start_time,
        end_time=end_time,
        granularity_minutes=60,
        sources=['solar', 'wind_onshore', 'wind_offshore', 'nuclear']
    )
    
    print(f"Metadata: {json.dumps(result['metadata'], indent=2)}")
    print(f"Number of time bins: {len(result['data'])}")
    
    if result['data']:
        print(f"First time bin: {json.dumps(result['data'][0], indent=2)}")
        if len(result['data']) > 1:
            print(f"Last time bin: {json.dumps(result['data'][-1], indent=2)}")
    
    # Test 2: Grouped aggregation
    print("\n" + "="*30)
    print("Test 2: Grouped aggregation (renewable sources)")
    print("="*30)
    
    groups = {
        "renewable": ["solar", "wind_onshore", "wind_offshore", "biomass"],
        "low_carbon": ["solar", "wind_onshore", "wind_offshore", "nuclear", "biomass"]
    }
    
    result = db.get_generation_aggregated(
        start_time=start_time,
        end_time=end_time,
        granularity_minutes=60,
        sources=['solar', 'wind_onshore', 'wind_offshore', 'nuclear', 'biomass'],
        groups=groups
    )
    
    print(f"Metadata: {json.dumps(result['metadata'], indent=2)}")
    
    if result['data']:
        print("Groups in first time bin:", list(result['data'][0]['groups'].keys()))
        print("Full groups dict:", json.dumps(result['data'][0]['groups'], indent=2))
    
    # Test 3: Different granularity (30-minute bins)
    print("\n" + "="*30)
    print("Test 3: 30-minute granularity (last 6 hours)")
    print("="*30)
    
    start_time_6h = end_time - timedelta(hours=6)
    
    result = db.get_generation_aggregated(
        start_time=start_time_6h,
        end_time=end_time,
        granularity_minutes=30,
        sources=['solar', 'wind_onshore', 'nuclear']
    )
    
    print(f"Metadata: {json.dumps(result['metadata'], indent=2)}")
    print(f"Number of time bins: {len(result['data'])}")
    
    # Test 4: Error handling - invalid granularity
    print("\n" + "="*30)
    print("Test 4: Error handling - invalid granularity")
    print("="*30)
    
    try:
        result = db.get_generation_aggregated(
            start_time=start_time,
            end_time=end_time,
            granularity_minutes=45,  # Invalid
            sources=['solar']
        )
        print("Unexpected: Should have raised an error")
    except ValueError as e:
        print(f"Expected error: {e}")
    
    # Test 5: Error handling - invalid sources
    print("\n" + "="*30)
    print("Test 5: Error handling - invalid sources")
    print("="*30)
    
    result = db.get_generation_aggregated(
        start_time=start_time,
        end_time=end_time,
        granularity_minutes=60,
        sources=['solar', 'invalid_source', 'wind_onshore']
    )
    
    print(f"Result with invalid sources: {json.dumps(result['metadata'], indent=2)}")
    
    print("\n" + "="*50)
    print("Aggregation tests completed!")
    print("="*50)

if __name__ == "__main__":
    test_aggregation() 