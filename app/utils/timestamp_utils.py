#!/usr/bin/env python3
"""
Timestamp utilities for handling format inconsistencies
"""

import re
from datetime import datetime, timezone
from typing import Union

def normalize_timestamp(timestamp_str: str) -> str:
    """
    Normalize timestamp string to consistent format without seconds.
    
    Handles these formats:
    - "2023-07-14T00:00:00Z" -> "2023-07-14T00:00Z"
    - "2023-07-14T00:00Z" -> "2023-07-14T00:00Z" (no change)
    - "2023-07-14T00:00:00+00:00" -> "2023-07-14T00:00Z"
    - "2023-07-14T00:00+00:00" -> "2023-07-14T00:00Z"
    """
    if not timestamp_str:
        return timestamp_str
    
    # Remove timezone info and convert to UTC Z format
    if '+' in timestamp_str:
        # Parse and convert to UTC Z format
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        timestamp_str = dt.strftime('%Y-%m-%dT%H:%M:%S')
    elif timestamp_str.endswith('Z'):
        # Already in Z format, just remove seconds if present
        pass
    else:
        # Assume UTC if no timezone info
        timestamp_str = timestamp_str + 'Z'
    
    # Remove seconds if present
    if len(timestamp_str) > 17:  # Has seconds
        # Remove seconds and colon, keep Z
        normalized = timestamp_str[:16] + "Z"
    else:
        normalized = timestamp_str
    
    return normalized

def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse timestamp string to datetime object, handling various formats.
    """
    try:
        if timestamp_str.endswith('Z'):
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        else:
            return datetime.fromisoformat(timestamp_str)
    except Exception as e:
        raise ValueError(f"Could not parse timestamp '{timestamp_str}': {e}")

def format_timestamp(dt: datetime) -> str:
    """
    Format datetime object to consistent timestamp string format.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime('%Y-%m-%dT%H:%MZ')

def timestamps_match(ts1: str, ts2: str) -> bool:
    """
    Compare two timestamp strings, normalizing both first.
    """
    return normalize_timestamp(ts1) == normalize_timestamp(ts2)

def get_timestamp_format_info(timestamp_str: str) -> dict:
    """
    Analyze a timestamp string and return format information.
    """
    info = {
        'original': timestamp_str,
        'normalized': normalize_timestamp(timestamp_str),
        'has_seconds': len(timestamp_str) > 17,
        'has_timezone': '+' in timestamp_str or timestamp_str.endswith('Z'),
        'length': len(timestamp_str)
    }
    
    # Try to parse it
    try:
        dt = parse_timestamp(timestamp_str)
        info['parsed'] = dt.isoformat()
        info['is_valid'] = True
    except Exception as e:
        info['parsed'] = None
        info['is_valid'] = False
        info['error'] = str(e)
    
    return info 