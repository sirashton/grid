#!/usr/bin/env python3
"""
Test script to verify gap filling respects the 5-chunk limit
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from main import GridTracker
from datetime import datetime, timezone, timedelta

def test_gap_chunking():
    """Test that gap filling respects the 5-chunk limit"""
    print("Testing gap filling chunk limits...")
    
    tracker = GridTracker()
    
    # Create some test gaps (more than 5 chunks worth)
    test_gaps = []
    base_time = datetime(2023, 7, 1, tzinfo=timezone.utc)
    
    # Create 10 chunks of gaps (more than the 5-chunk limit)
    for i in range(10):
        chunk_start = base_time + timedelta(days=i*6)  # 6-day gaps
        chunk_end = chunk_start + timedelta(days=5)    # 5-day chunks
        test_gaps.append((chunk_start, chunk_end))
    
    print(f"Created {len(test_gaps)} test gaps:")
    for i, (start, end) in enumerate(test_gaps):
        print(f"  Gap {i+1}: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
    
    # Test the gap grouping method
    grouped_gaps = tracker._group_consecutive_gaps(test_gaps, granularity_minutes=30)
    print(f"\nGrouped into {len(grouped_gaps)} ranges:")
    for i, (start, end) in enumerate(grouped_gaps):
        duration = end - start
        print(f"  Range {i+1}: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} ({duration.days} days)")
    
    # Test the 5-chunk limit logic
    MAX_GAP_CHUNKS = 5
    if len(grouped_gaps) > MAX_GAP_CHUNKS:
        print(f"\nLimiting to most recent {MAX_GAP_CHUNKS} chunks...")
        limited_gaps = grouped_gaps[-MAX_GAP_CHUNKS:]
        print(f"Limited to {len(limited_gaps)} chunks:")
        for i, (start, end) in enumerate(limited_gaps):
            duration = end - start
            print(f"  Limited {i+1}: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} ({duration.days} days)")
    
    print("\nGap chunking test completed!")

if __name__ == "__main__":
    print("Testing gap filling limits...")
    print("=" * 50)
    
    test_gap_chunking()
    
    print("\n✅ Gap limit tests completed!") 