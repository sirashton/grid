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

def test_carbon_intensity_gaps():
    """Test the gap detection functionality for carbon intensity data"""
    print("Testing Carbon Intensity Gap Detection")
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
        print("❌ CARBON INTENSITY TEST FAILED")
        return False
    
    print(f"Table: {stats['table_name']}")
    print(f"Total records: {stats['total_records']}")
    print(f"Earliest data: {stats['earliest_data']}")
    print(f"Latest data: {stats['latest_data']}")
    print(f"Time span: {stats['time_span_hours']:.1f} hours")
    print(f"Healthy: {stats['healthy']}")
    
    if not stats['healthy']:
        print("No data to analyze")
        print("❌ CARBON INTENSITY TEST FAILED")
        return False
    
    # Detect gaps initially
    print("\n2. Initial Gap Detection:")
    initial_gaps = gap_detector.detect_data_gaps(
        table_name='carbon_intensity_30min_data',
        granularity_minutes=30
    )
    
    if initial_gaps:
        print(f"Found {len(initial_gaps)} gaps initially:")
        for i, (gap_start, gap_end) in enumerate(initial_gaps[:5], 1):
            print(f"  {i}. Missing: {gap_start.isoformat()}")
        if len(initial_gaps) > 5:
            print(f"  ... and {len(initial_gaps) - 5} more gaps")
    else:
        print("No gaps detected initially!")
    
    # Always proceed with artificial gap test
    print("\n3. Creating Artificial Gap for Testing:")
    deleted_timestamp = create_artificial_gap('carbon_intensity_30min_data')
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
                fill_success = test_carbon_gap_filling(deleted_timestamp)
                
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
        print("🎉 CARBON INTENSITY TEST PASSED!")
    else:
        print("❌ CARBON INTENSITY TEST FAILED")
    
    return overall_pass

def test_generation_gaps():
    """Test the gap detection functionality for generation data"""
    print("\nTesting Generation Gap Detection")
    print("=" * 50)
    
    # Initialize components
    gap_detector = DataGapDetector()
    db = Database()
    overall_pass = True
    
    # Get database stats
    print("\n1. Database Statistics:")
    stats = gap_detector.get_data_stats('generation_30min_data')
    if 'error' in stats:
        print(f"Error: {stats['error']}")
        print("❌ GENERATION TEST FAILED")
        return False
    
    print(f"Table: {stats['table_name']}")
    print(f"Total records: {stats['total_records']}")
    print(f"Earliest data: {stats['earliest_data']}")
    print(f"Latest data: {stats['latest_data']}")
    print(f"Time span: {stats['time_span_hours']:.1f} hours")
    print(f"Healthy: {stats['healthy']}")
    
    if not stats['healthy']:
        print("No data to analyze")
        print("❌ GENERATION TEST FAILED")
        return False
    
    # Detect gaps initially
    print("\n2. Initial Gap Detection:")
    initial_gaps = gap_detector.detect_data_gaps(
        table_name='generation_30min_data',
        granularity_minutes=30
    )
    
    if initial_gaps:
        print(f"Found {len(initial_gaps)} gaps initially:")
        for i, (gap_start, gap_end) in enumerate(initial_gaps[:5], 1):
            print(f"  {i}. Missing: {gap_start.isoformat()}")
        if len(initial_gaps) > 5:
            print(f"  ... and {len(initial_gaps) - 5} more gaps")
    else:
        print("No gaps detected initially!")
    
    # Always proceed with artificial gap test
    print("\n3. Creating Artificial Gap for Testing:")
    deleted_timestamp = create_artificial_gap('generation_30min_data')
    if deleted_timestamp:
        print(f"Deleted row with timestamp: {deleted_timestamp}")
        
        # Verify gap is detected
        print("\n4. Verifying Gap Detection:")
        gaps_after_deletion = gap_detector.detect_data_gaps(
            table_name='generation_30min_data',
            granularity_minutes=30
        )
        
        if gaps_after_deletion:
            print(f"✅ SUCCESS: Found {len(gaps_after_deletion)} gaps after deletion")
            for gap_start, gap_end in gaps_after_deletion[:3]:
                print(f"  Missing: {gap_start.isoformat()}")
            
            # Check if our deleted timestamp is in the gaps
            deleted_found = False
            # Normalize deleted_timestamp to remove seconds for comparison
            normalized_deleted_ts = deleted_timestamp
            if len(deleted_timestamp) > 17:  # Has seconds
                normalized_deleted_ts = deleted_timestamp[:16] + "Z"  # Remove seconds and colon, keep Z
            
            for gap_start, gap_end in gaps_after_deletion:
                if gap_start.strftime('%Y-%m-%dT%H:%MZ') == normalized_deleted_ts:
                    deleted_found = True
                    break
            
            if deleted_found:
                print(f"✅ SUCCESS: Deleted timestamp {deleted_timestamp} was correctly detected as a gap!")
                
                # Now test gap filling
                print("\n5. Testing Gap Filling:")
                fill_success = test_generation_gap_filling(deleted_timestamp)
                
                if fill_success:
                    # Verify gap is actually filled
                    print("\n6. Verifying Gap is Filled:")
                    gaps_after_filling = gap_detector.detect_data_gaps(
                        table_name='generation_30min_data',
                        granularity_minutes=30
                    )
                    
                    # Check if our deleted timestamp is still in the gaps
                    deleted_still_missing = False
                    for gap_start, gap_end in gaps_after_filling:
                        if gap_start.strftime('%Y-%m-%dT%H:%MZ') == normalized_deleted_ts:
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
                FROM generation_30min_data
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
        print("🎉 GENERATION TEST PASSED!")
    else:
        print("❌ GENERATION TEST FAILED")
    
    return overall_pass

def test_gap_detection():
    """Test the gap detection functionality for both tables"""
    print("Testing Gap Detection Functionality for Both Tables")
    print("=" * 60)
    
    # Test both tables
    carbon_pass = test_carbon_intensity_gaps()
    generation_pass = test_generation_gaps()
    
    print("\n" + "=" * 60)
    print("OVERALL TEST RESULTS")
    print("=" * 60)
    print(f"Carbon Intensity: {'✅ PASSED' if carbon_pass else '❌ FAILED'}")
    print(f"Generation: {'✅ PASSED' if generation_pass else '❌ FAILED'}")
    
    if carbon_pass and generation_pass:
        print("🎉 OVERALL TEST PASSED: Gap detection and filling works perfectly for both tables!")
    else:
        print("❌ OVERALL TEST FAILED: Some tests failed")

def create_artificial_gap(table_name):
    """Create an artificial gap by deleting a middle row"""
    try:
        with sqlite3.connect('/data/grid.db') as conn:
            cursor = conn.cursor()
            
            # First, let's find a range where we have consecutive data
            cursor.execute(f"""
                SELECT timestamp 
                FROM {table_name} 
                ORDER BY timestamp 
                LIMIT 100
            """)
            timestamps = [row[0] for row in cursor.fetchall()]
            
            if len(timestamps) < 10:
                print("Not enough data to create artificial gap")
                return None
            
            # Find a timestamp in the middle of the range where we have data
            middle_index = len(timestamps) // 2
            timestamp_to_delete = timestamps[middle_index]
            
            print(f"Found {len(timestamps)} consecutive timestamps")
            print(f"Will delete timestamp: {timestamp_to_delete}")
            
            # Verify this timestamp exists before deleting
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {table_name} 
                WHERE timestamp = ?
            """, (timestamp_to_delete,))
            
            count_before = cursor.fetchone()[0]
            print(f"Records with this timestamp before deletion: {count_before}")
            
            if count_before == 0:
                print("Timestamp doesn't exist, trying a different one")
                # Try the next one
                if middle_index + 1 < len(timestamps):
                    timestamp_to_delete = timestamps[middle_index + 1]
                    cursor.execute(f"""
                        SELECT COUNT(*) 
                        FROM {table_name} 
                        WHERE timestamp = ?
                    """, (timestamp_to_delete,))
                    count_before = cursor.fetchone()[0]
                    print(f"Trying next timestamp: {timestamp_to_delete} (count: {count_before})")
            
            if count_before > 0:
                # Delete the row
                cursor.execute(f"""
                    DELETE FROM {table_name} 
                    WHERE timestamp = ?
                """, (timestamp_to_delete,))
                
                conn.commit()
                
                # Verify deletion
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM {table_name} 
                    WHERE timestamp = ?
                """, (timestamp_to_delete,))
                count_after = cursor.fetchone()[0]
                print(f"Records with this timestamp after deletion: {count_after}")
                
                return timestamp_to_delete
            else:
                print("Could not find a valid timestamp to delete")
                return None
                
    except Exception as e:
        print(f"Error creating artificial gap: {e}")
        return None

def test_carbon_gap_filling(target_timestamp):
    """Test the carbon intensity gap filling functionality"""
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
        
        print(f"Attempting to fill carbon intensity gap: {gap_start.isoformat()}")
        
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
        print(f"Error in carbon gap filling test: {e}")
        return False

def test_generation_gap_filling(target_timestamp):
    """Test the generation gap filling functionality"""
    try:
        # Import the main components for gap filling
        from elexon_bm_api import ElexonBMAPI
        
        # Initialize components
        api = ElexonBMAPI()
        db = Database()
        
        # Parse the target timestamp
        if target_timestamp.endswith('Z'):
            gap_start = datetime.fromisoformat(target_timestamp.replace('Z', '+00:00'))
        else:
            gap_start = datetime.fromisoformat(target_timestamp)
        
        gap_end = gap_start  # Single point gap
        
        print(f"Attempting to fill generation gap: {gap_start.isoformat()}")
        print(f"API call details:")
        print(f"  Start time: {gap_start.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        print(f"  End time: {gap_end.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        print(f"  URL: {api.base_url}/generation/actual/per-type")
        print(f"  Params: from={gap_start.strftime('%Y-%m-%dT%H:%M:%SZ')}, to={gap_end.strftime('%Y-%m-%dT%H:%M:%SZ')}, format=json")
        
        # Fetch data from API
        data_points = api.get_generation_data(gap_start, gap_end)
        
        if data_points:
            print(f"Retrieved {len(data_points)} data points from API")
            
            # Store the data
            inserted_count = 0
            for point in data_points:
                success = db.insert_generation_data(
                    timestamp=point['timestamp'],
                    settlement_period=point['settlement_period'],
                    **{k: v for k, v in point.items() if k not in ['timestamp', 'settlement_period']}
                )
                if success:
                    inserted_count += 1
            
            print(f"Successfully inserted {inserted_count} data points")
            return inserted_count > 0
        else:
            print("No data points received from API")
            return False
            
    except Exception as e:
        print(f"Error in generation gap filling test: {e}")
        return False

if __name__ == "__main__":
    test_gap_detection() 