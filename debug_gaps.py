#!/usr/bin/env python3
"""
Debug script to understand gap detection issues
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from data_gap_detector import DataGapDetector
import sqlite3

def debug_gaps():
    """Debug the gap detection issue"""
    print("Debugging Gap Detection")
    print("=" * 30)
    
    gap_detector = DataGapDetector()
    
    # Get actual timestamps from database
    with sqlite3.connect('/data/grid.db') as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp FROM carbon_intensity_30min_data ORDER BY timestamp LIMIT 10")
        actual_timestamps = [row[0] for row in cursor.fetchall()]
    
    print(f"Actual timestamps (first 10):")
    for ts in actual_timestamps:
        print(f"  {ts}")
    
    print(f"\nExpected timestamps (first 10):")
    # Generate expected timestamps
    from datetime import datetime, timezone, timedelta
    
    # Parse first actual timestamp to get start time
    first_ts = actual_timestamps[0]
    if first_ts.endswith('Z'):
        start_time = datetime.fromisoformat(first_ts.replace('Z', '+00:00'))
    else:
        start_time = datetime.fromisoformat(first_ts)
    
    # Generate expected timestamps
    expected = []
    current = start_time
    for i in range(10):
        expected.append(current.strftime('%Y-%m-%dT%H:%M:%SZ'))
        current += timedelta(minutes=30)
    
    for ts in expected:
        print(f"  {ts}")
    
    print(f"\nComparison:")
    actual_set = set(actual_timestamps)
    for expected_ts in expected:
        if expected_ts in actual_set:
            print(f"  ✓ {expected_ts} - FOUND")
        else:
            print(f"  ✗ {expected_ts} - MISSING")

if __name__ == "__main__":
    debug_gaps() 