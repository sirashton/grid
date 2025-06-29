#!/usr/bin/env python3
"""
Test script for gap detection functionality
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from data_gap_detector import DataGapDetector
from database import Database
from datetime import datetime, timezone, timedelta
import sqlite3

def test_gap_detection():
    """Test the gap detection functionality"""
    print("Testing Gap Detection Functionality")
    print("=" * 50)
    
    # Initialize components
    gap_detector = DataGapDetector()
    db = Database()
    overall_pass = True
    
    # Get database stats
    print("\n1. Database Statistics:")
    stats = gap_detector.get_data_stats('carbon_intensity_30min_data')
    if 'error' in stats:
        print(f"Error: {stats['error']}")
        print("❌ OVERALL TEST FAILED")
        return
    
    print(f"Table: {stats['table_name']}")
    print(f"Total records: {stats['total_records']}")
    print(f"Earliest data: {stats['earliest_data']}")
    print(f"Latest data: {stats['latest_data']}")
    print(f"Time span: {stats['time_span_hours']:.1f} hours")
    print(f"Healthy: {stats['healthy']}")
    
    if not stats['healthy']:
        print("No data to analyze")
        print("❌ OVERALL TEST FAILED")
        return
    
    # Detect gaps initially
    print("\n2. Initial Gap Detection:")
    initial_gaps = gap_detector.detect_data_gaps(
        table_name='carbon_intensity_30min_data',
        granularity_minutes=30
    )
    
    if initial_gaps:
        print(f"❌ FAILURE: Found {len(initial_gaps)} gaps initially:")
        for i, (gap_start, gap_end) in enumerate(initial_gaps[:5], 1):
            print(f"  {i}. Missing: {gap_start.isoformat()}")
        if len(initial_gaps) > 5:
            print(f"  ... and {len(initial_gaps) - 5} more gaps")
        overall_pass = False
    else:
        print("No gaps detected initially!")
    
    # Always proceed with artificial gap test
    print("\n3. Creating Artificial Gap for Testing:")
    deleted_timestamp = create_artificial_gap()
    if deleted_timestamp:
        print(f"Deleted row with timestamp: {deleted_timestamp}")
        
        # Verify gap is detected
        print("\n4. Verifying Gap Detection:")
        gaps_after_deletion = gap_detector.detect_data_gaps(
            table_name='carbon_intensity_30min_data',
            granularity_minutes=30
        )
        
        if gaps_after_deletion:
            print(f"✅ SUCCESS: Found {len(gaps_after_deletion)} gaps after deletion")
            for gap_start, gap_end in gaps_after_deletion[:3]:
                print(f"  Missing: {gap_start.isoformat()}")
            
            # Check if our deleted timestamp is in the gaps
            deleted_found = False
            for gap_start, gap_end in gaps_after_deletion:
                if gap_start.strftime('%Y-%m-%dT%H:%MZ') == deleted_timestamp:
                    deleted_found = True
                    break
            
            if deleted_found:
                print(f"✅ SUCCESS: Deleted timestamp {deleted_timestamp} was correctly detected as a gap!")
                
                # Now test gap filling
                print("\n5. Testing Gap Filling:")
                fill_success = test_gap_filling(deleted_timestamp)
                
                if fill_success:
                    # Verify gap is actually filled
                    print("\n6. Verifying Gap is Filled:")
                    gaps_after_filling = gap_detector.detect_data_gaps(
                        table_name='carbon_intensity_30min_data',
                        granularity_minutes=30
                    )
                    
                    # Check if our deleted timestamp is still in the gaps
                    deleted_still_missing = False
                    for gap_start, gap_end in gaps_after_filling:
                        if gap_start.strftime('%Y-%m-%dT%H:%MZ') == deleted_timestamp:
                            deleted_still_missing = True
                            break
                    
                    if not deleted_still_missing:
                        print(f"✅ SUCCESS: Gap {deleted_timestamp} was successfully filled!")
                    else:
                        print(f"❌ FAILURE: Gap {deleted_timestamp} is still missing after filling attempt")
                        overall_pass = False
                else:
                    print("❌ FAILURE: Gap filling failed")
                    overall_pass = False
            else:
                print(f"❌ FAILURE: Deleted timestamp {deleted_timestamp} was NOT detected as a gap")
                overall_pass = False
        else:
            print("❌ FAILURE: No gaps detected after creating artificial gap")
            overall_pass = False
    else:
        print("❌ FAILURE: Could not create artificial gap")
        overall_pass = False
    
    # Test duplicate detection
    print("\n7. Duplicate Detection:")
    try:
        with sqlite3.connect('/data/grid.db') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT timestamp, COUNT(*) as count
                FROM carbon_intensity_30min_data
                GROUP BY timestamp
                HAVING COUNT(*) > 1
                ORDER BY timestamp DESC
                LIMIT 5
            """)
            duplicates = cursor.fetchall()
            
            if duplicates:
                print(f"Found {len(duplicates)} duplicate timestamps:")
                for timestamp, count in duplicates:
                    print(f"  {timestamp}: {count} occurrences")
            else:
                print("No duplicate timestamps found")
    except Exception as e:
        print(f"Error checking duplicates: {e}")
        overall_pass = False
    
    print("\n==============================")
    if overall_pass:
        print("🎉 OVERALL TEST PASSED: Gap detection and filling works perfectly!")
    else:
        print("❌ OVERALL TEST FAILED")

def create_artificial_gap():
    """Create an artificial gap by deleting a middle row"""
    try:
        with sqlite3.connect('/data/grid.db') as conn:
            cursor = conn.cursor()
            
            # Get a middle timestamp to delete (not the first or last)
            cursor.execute("""
                SELECT timestamp 
                FROM carbon_intensity_30min_data 
                ORDER BY timestamp 
                LIMIT 1 OFFSET 10
            """)
            result = cursor.fetchone()
            
            if result:
                timestamp_to_delete = result[0]
                
                # Delete the row
                cursor.execute("""
                    DELETE FROM carbon_intensity_30min_data 
                    WHERE timestamp = ?
                """, (timestamp_to_delete,))
                
                conn.commit()
                return timestamp_to_delete
            else:
                print("No data available to delete")
                return None
                
    except Exception as e:
        print(f"Error creating artificial gap: {e}")
        return None

def test_gap_filling(target_timestamp):
    """Test the gap filling functionality"""
    try:
        # Import the main components for gap filling
        from carbon_intensity_api import CarbonIntensityAPI
        
        # Initialize components
        api = CarbonIntensityAPI()
        db = Database()
        
        # Parse the target timestamp
        if target_timestamp.endswith('Z'):
            gap_start = datetime.fromisoformat(target_timestamp.replace('Z', '+00:00'))
        else:
            gap_start = datetime.fromisoformat(target_timestamp)
        
        gap_end = gap_start  # Single point gap
        
        print(f"Attempting to fill gap: {gap_start.isoformat()}")
        
        # Fetch data from API
        data_points = api.get_intensity_data(gap_start, gap_end)
        
        if data_points:
            print(f"Retrieved {len(data_points)} data points from API")
            
            # Store the data
            inserted_count = 0
            for point in data_points:
                success = db.insert_carbon_intensity_data(
                    timestamp=point['timestamp'],
                    emissions=point['emissions']
                )
                if success:
                    inserted_count += 1
            
            print(f"Successfully inserted {inserted_count} data points")
            return inserted_count > 0
        else:
            print("No data points received from API")
            return False
            
    except Exception as e:
        print(f"Error in gap filling test: {e}")
        return False

if __name__ == "__main__":
    test_gap_detection() 