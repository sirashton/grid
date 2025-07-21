#!/usr/bin/env python3
"""
Generic data gap detection for grid tracker
"""

import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Dict
from pathlib import Path
from utils.timestamp_utils import normalize_timestamp, parse_timestamp, format_timestamp

logger = logging.getLogger(__name__)

class DataGapDetector:
    """Detect gaps in time-series data"""
    
    def __init__(self, db_path: str = '/data/grid.db'):
        self.db_path = db_path
    
    def detect_data_gaps(
        self,
        table_name: str,
        granularity_minutes: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Tuple[datetime, datetime]]:
        """
        Detect gaps in time-series data
        
        Args:
            table_name: Name of the table to check
            granularity_minutes: Expected interval between data points (e.g., 30 for 30-minute data)
            start_time: Start of time range to check (if None, uses earliest data in table)
            end_time: End of time range to check (if None, uses latest data in table)
            
        Returns:
            List of tuples (gap_start, gap_end) where each tuple represents a missing data point
            If gap_start == gap_end, it's a single missing point
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (table_name,))
                
                if not cursor.fetchone():
                    logger.error(f"Table '{table_name}' does not exist")
                    return []
                
                # Get data range from table if not specified
                if start_time is None or end_time is None:
                    cursor.execute(f"""
                        SELECT MIN(timestamp), MAX(timestamp)
                        FROM {table_name}
                    """)
                    result = cursor.fetchone()
                    
                    if not result or not result[0] or not result[1]:
                        logger.warning(f"No data found in table '{table_name}'")
                        return []
                    
                    min_time_str, max_time_str = result
                    
                    # Parse timestamps
                    if start_time is None:
                        start_time = self._parse_timestamp(min_time_str)
                    if end_time is None:
                        end_time = self._parse_timestamp(max_time_str)
                
                # Validate time range
                if start_time >= end_time:
                    logger.error("Start time must be before end time")
                    return []
                
                # Get all timestamps in the range
                cursor.execute(f"""
                    SELECT timestamp
                    FROM {table_name}
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                """, (start_time.strftime('%Y-%m-%dT%H:%MZ'), end_time.strftime('%Y-%m-%dT%H:%MZ')))
                
                actual_timestamps = [row[0] for row in cursor.fetchall()]
                
                # Check for duplicates
                duplicates = self._find_duplicate_timestamps(actual_timestamps)
                if duplicates:
                    logger.warning(f"Found duplicate timestamps in {table_name}: {duplicates}")
                
                # Generate expected timestamps
                expected_timestamps = self._generate_expected_timestamps(
                    start_time, end_time, granularity_minutes
                )
                
                # Find gaps
                gaps = self._find_gaps(expected_timestamps, actual_timestamps)
                
                logger.info(f"Found {len(gaps)} gaps in {table_name} between {start_time} and {end_time}")
                return gaps
                
        except Exception as e:
            logger.error(f"Error detecting gaps in {table_name}: {e}")
            return []
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object"""
        return parse_timestamp(timestamp_str)
    
    def _find_duplicate_timestamps(self, timestamps: List[str]) -> List[str]:
        """Find duplicate timestamps in the list"""
        seen = set()
        duplicates = []
        for ts in timestamps:
            if ts in seen:
                duplicates.append(ts)
            else:
                seen.add(ts)
        return duplicates
    
    def _generate_expected_timestamps(
        self, 
        start_time: datetime, 
        end_time: datetime, 
        granularity_minutes: int
    ) -> List[str]:
        """Generate list of expected timestamps based on granularity"""
        expected = []
        current = start_time
        
        while current <= end_time:
            # Format without seconds to match database format exactly
            expected.append(format_timestamp(current))
            current += timedelta(minutes=granularity_minutes)
        
        return expected
    
    def _find_gaps(
        self, 
        expected_timestamps: List[str], 
        actual_timestamps: List[str]
    ) -> List[Tuple[datetime, datetime]]:
        """Find gaps between expected and actual timestamps, consolidating consecutive gaps"""
        gaps = []
        
        # Normalize actual timestamps to remove seconds for comparison
        # This handles both formats: "2023-07-14T00:00Z" and "2023-07-14T00:00:00Z"
        normalized_actual_set = set()
        for actual_ts in actual_timestamps:
            normalized_ts = normalize_timestamp(actual_ts)
            normalized_actual_set.add(normalized_ts)
        
        # Find all missing timestamps
        missing_timestamps = []
        for expected_ts in expected_timestamps:
            normalized_expected = normalize_timestamp(expected_ts)
            if normalized_expected not in normalized_actual_set:
                missing_timestamps.append(self._parse_timestamp(expected_ts))
        
        logger.info(f"Found {len(missing_timestamps)} missing timestamps")
        for ts in missing_timestamps:
            logger.info(f"  Missing: {ts}")
        
        # Consolidate consecutive missing timestamps into gaps
        if not missing_timestamps:
            return gaps
        
        # Sort missing timestamps
        missing_timestamps.sort()
        
        # Group consecutive missing timestamps
        current_gap_start = missing_timestamps[0]
        current_gap_end = missing_timestamps[0]
        
        logger.info(f"Starting gap consolidation. First missing timestamp: {current_gap_start}")
        
        for i in range(1, len(missing_timestamps)):
            current_time = missing_timestamps[i]
            expected_previous = current_gap_end + timedelta(minutes=30)
            
            logger.info(f"Checking {current_time} against expected previous {expected_previous}")
            logger.info(f"  Are they consecutive? {current_time == expected_previous}")
            
            if current_time == expected_previous:
                # Consecutive missing timestamp, extend the gap
                current_gap_end = current_time
                logger.info(f"  Extending gap to: {current_gap_start} to {current_gap_end}")
            else:
                # Non-consecutive, save current gap and start new one
                logger.info(f"  Non-consecutive! Saving gap: {current_gap_start} to {current_gap_end}")
                gaps.append((current_gap_start, current_gap_end))
                current_gap_start = current_time
                current_gap_end = current_time
                logger.info(f"  Starting new gap at: {current_time}")
        
        # Add the last gap
        logger.info(f"Adding final gap: {current_gap_start} to {current_gap_end}")
        gaps.append((current_gap_start, current_gap_end))
        
        logger.info(f"Final result: {len(gaps)} consolidated gaps")
        for gap_start, gap_end in gaps:
            if gap_start == gap_end:
                logger.info(f"  Single point gap: {gap_start}")
            else:
                time_diff = (gap_end - gap_start).total_seconds() / (30 * 60)
                logger.info(f"  Multi-point gap: {gap_start} to {gap_end} ({time_diff:.1f} hours)")
        
        return gaps
    
    def get_data_stats(self, table_name: str) -> Dict:
        """Get statistics about data in a table"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (table_name,))
                
                if not cursor.fetchone():
                    return {'error': f"Table '{table_name}' does not exist"}
                
                # Get basic stats
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_count = cursor.fetchone()[0]
                
                cursor.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM {table_name}
                """)
                result = cursor.fetchone()
                
                if result and result[0] and result[1]:
                    min_time = self._parse_timestamp(result[0])
                    max_time = self._parse_timestamp(result[1])
                    time_span = max_time - min_time
                    
                    return {
                        'table_name': table_name,
                        'total_records': total_count,
                        'earliest_data': result[0],
                        'latest_data': result[1],
                        'time_span_hours': time_span.total_seconds() / 3600,
                        'healthy': True
                    }
                else:
                    return {
                        'table_name': table_name,
                        'total_records': 0,
                        'earliest_data': None,
                        'latest_data': None,
                        'time_span_hours': 0,
                        'healthy': False
                    }
                    
        except Exception as e:
            logger.error(f"Error getting stats for {table_name}: {e}")
            return {'error': str(e)} 