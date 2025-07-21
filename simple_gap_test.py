#!/usr/bin/env python3
"""
Simple test to verify gap detection works correctly
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from data_gap_detector import DataGapDetector
from datetime import datetime, timezone, timedelta
import sqlite3

def test_small_range():
    """Test gap detection on a small range where we know data exists"""
    print("Testing Gap Detection on Small Range")
    print("=" * 50)
    
    detector = DataGapDetector()
    
    # Test generation data on a small range
    print("\n1. Testing Generation Data:")
    
    # Find a range where we have data
    with sqlite3.connect('/data/grid.db') as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT timestamp FROM generation_30min_data 
            ORDER BY timestamp 
            LIMIT 10
        """)
        timestamps = [row[0] for row in cursor.fetchall()]
        
        if timestamps:
            start_ts = timestamps[0]
            end_ts = timestamps[-1]
            
            print(f"Testing range: {start_ts} to {end_ts}")
            
            # Parse timestamps
            if start_ts.endswith('Z'):
                start_dt = datetime.fromisoformat(start_ts.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end_ts.replace('Z', '+00:00'))
            else:
                start_dt = datetime.fromisoformat(start_ts)
                end_dt = datetime.fromisoformat(end_ts)
            
            # Test gap detection on this small range
            gaps = detector.detect_data_gaps(
                table_name='generation_30min_data',
                granularity_minutes=30,
                start_time=start_dt,
                end_time=end_dt
            )
            
            print(f"Found {len(gaps)} gaps in this range")
            
            if gaps:
                print("Gaps found:")
                for gap in gaps:
                    print(f"  {gap[0].isoformat()}")
            else:
                print("No gaps found - this is good!")
                
                # Now let's delete a record and see if it's detected
                print(f"\n2. Deleting a record to create a gap:")
                timestamp_to_delete = timestamps[len(timestamps)//2]
                print(f"Deleting: {timestamp_to_delete}")
                
                cursor.execute("""
                    DELETE FROM generation_30min_data 
                    WHERE timestamp = ?
                """, (timestamp_to_delete,))
                conn.commit()
                
                # Check if it's detected as a gap
                gaps_after = detector.detect_data_gaps(
                    table_name='generation_30min_data',
                    granularity_minutes=30,
                    start_time=start_dt,
                    end_time=end_dt
                )
                
                print(f"Found {len(gaps_after)} gaps after deletion")
                
                # Check if our deleted timestamp is in the gaps
                deleted_found = False
                for gap in gaps_after:
                    if gap[0].strftime('%Y-%m-%dT%H:%MZ') == timestamp_to_delete[:17]:  # Remove seconds
                        deleted_found = True
                        break
                
                if deleted_found:
                    print(f"✅ SUCCESS: Deleted timestamp was detected as a gap!")
                else:
                    print(f"❌ FAILURE: Deleted timestamp was NOT detected as a gap")
                    print("Available gaps:")
                    for gap in gaps_after:
                        print(f"  {gap[0].isoformat()}")
        else:
            print("No generation data found")

if __name__ == "__main__":
    test_small_range() 