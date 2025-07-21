#!/usr/bin/env python3
"""
Script to normalize all timestamps in the database to consistent format
"""

import sys
import os

import sqlite3
import logging
from datetime import datetime

# Add the current directory to the path so we can import from utils
sys.path.insert(0, os.path.dirname(__file__))
from utils.timestamp_utils import normalize_timestamp

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def normalize_database_timestamps():
    """Normalize all timestamps in the database to consistent format"""
    db_path = '/data/grid.db'
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # First, let's see what we're working with
        logger.info("=== Analyzing Current Timestamp Formats ===")
        
        # Check carbon intensity table
        cursor.execute("""
            SELECT timestamp, COUNT(*) as count
            FROM carbon_intensity_30min_data
            GROUP BY timestamp
            ORDER BY timestamp
            LIMIT 10
        """)
        carbon_timestamps = cursor.fetchall()
        logger.info(f"Carbon intensity table: {len(carbon_timestamps)} unique timestamps")
        
        # Check generation table
        cursor.execute("""
            SELECT timestamp, COUNT(*) as count
            FROM generation_30min_data
            GROUP BY timestamp
            ORDER BY timestamp
            LIMIT 10
        """)
        generation_timestamps = cursor.fetchall()
        logger.info(f"Generation table: {len(generation_timestamps)} unique timestamps")
        
        # Show sample formats
        logger.info("Sample carbon intensity timestamps:")
        for ts, count in carbon_timestamps[:3]:
            logger.info(f"  {ts} (length: {len(ts)})")
        
        logger.info("Sample generation timestamps:")
        for ts, count in generation_timestamps[:3]:
            logger.info(f"  {ts} (length: {len(ts)})")
        
        # Now normalize the generation table (carbon intensity already seems normalized)
        logger.info("\n=== Normalizing Generation Table ===")
        
        # First, handle duplicate timestamps
        logger.info("=== Handling Duplicate Timestamps ===")
        
        # Find timestamps that would conflict after normalization
        # Get all timestamps and their normalized versions
        cursor.execute("""
            SELECT timestamp
            FROM generation_30min_data
            ORDER BY timestamp
        """)
        
        all_timestamps = cursor.fetchall()
        
        # Group by normalized timestamp
        normalized_groups = {}
        for (ts,) in all_timestamps:
            normalized_ts = normalize_timestamp(ts)
            if normalized_ts not in normalized_groups:
                normalized_groups[normalized_ts] = []
            normalized_groups[normalized_ts].append(ts)
        
        # Find duplicates
        duplicates = []
        for normalized_ts, original_timestamps in normalized_groups.items():
            if len(original_timestamps) > 1:
                duplicates.append((normalized_ts, len(original_timestamps), original_timestamps))
        
        logger.info(f"Found {len(duplicates)} normalized timestamps with duplicates")
        
        for normalized_ts, count, original_timestamps in duplicates:
            logger.info(f"  {normalized_ts}: {count} records from {original_timestamps}")
            
            # Get the actual records for this normalized timestamp
            placeholders = ','.join(['?' for _ in original_timestamps])
            cursor.execute(f"""
                SELECT id, timestamp, settlement_period, biomass, fossil_gas, fossil_hard_coal,
                       fossil_oil, hydro_pumped_storage, hydro_run_of_river, nuclear,
                       other, solar, wind_offshore, wind_onshore, created_at
                FROM generation_30min_data
                WHERE timestamp IN ({placeholders})
                ORDER BY timestamp, created_at
            """, original_timestamps)
            
            records = cursor.fetchall()
            
            # Keep the most recent record (newest created_at), delete the rest
            if len(records) > 1:
                keep_id = records[-1][0]  # Last record ID (most recent)
                delete_ids = [r[0] for r in records[:-1]]  # All other record IDs
                
                logger.info(f"    Keeping record {keep_id}, deleting {len(delete_ids)} duplicates")
                
                # Delete duplicate records
                placeholders = ','.join(['?' for _ in delete_ids])
                cursor.execute(f"""
                    DELETE FROM generation_30min_data
                    WHERE id IN ({placeholders})
                """, delete_ids)
                
                logger.info(f"    Deleted {len(delete_ids)} duplicate records")
        
        # Commit the duplicate removal
        conn.commit()
        logger.info("Duplicate removal completed")
        
        # Now proceed with normalization
        logger.info("\n=== Proceeding with Normalization ===")
        
        # Get all timestamps that need normalization (those with seconds)
        cursor.execute("""
            SELECT DISTINCT timestamp
            FROM generation_30min_data
            WHERE LENGTH(timestamp) > 17
            ORDER BY timestamp
        """)
        
        timestamps_to_normalize = cursor.fetchall()
        logger.info(f"Found {len(timestamps_to_normalize)} timestamps to normalize")
        
        if not timestamps_to_normalize:
            logger.info("No timestamps need normalization!")
            return
        
        # Show some examples of what will be changed
        logger.info("Sample timestamps that will be normalized:")
        for (ts,) in timestamps_to_normalize[:5]:
            normalized = normalize_timestamp(ts)
            logger.info(f"  {ts} -> {normalized}")
        
        # Create a mapping of old to new timestamps
        timestamp_mapping = {}
        for (old_ts,) in timestamps_to_normalize:
            new_ts = normalize_timestamp(old_ts)
            if old_ts != new_ts:
                timestamp_mapping[old_ts] = new_ts
        
        logger.info(f"Will update {len(timestamp_mapping)} unique timestamps")
        
        # Update the timestamps
        updated_count = 0
        for old_ts, new_ts in timestamp_mapping.items():
            cursor.execute("""
                UPDATE generation_30min_data
                SET timestamp = ?
                WHERE timestamp = ?
            """, (new_ts, old_ts))
            
            rows_affected = cursor.rowcount
            updated_count += rows_affected
            logger.info(f"Updated {rows_affected} rows: {old_ts} -> {new_ts}")
        
        # Commit the changes
        conn.commit()
        logger.info(f"\n=== Normalization Complete ===")
        logger.info(f"Total rows updated: {updated_count}")
        
        # Verify the changes
        logger.info("\n=== Verification ===")
        cursor.execute("""
            SELECT timestamp, COUNT(*) as count
            FROM generation_30min_data
            GROUP BY timestamp
            ORDER BY timestamp
            LIMIT 10
        """)
        
        verification_timestamps = cursor.fetchall()
        logger.info("Sample timestamps after normalization:")
        for ts, count in verification_timestamps[:5]:
            logger.info(f"  {ts} (length: {len(ts)}, count: {count})")
        
        # Check if any timestamps still have seconds
        cursor.execute("""
            SELECT COUNT(DISTINCT timestamp)
            FROM generation_30min_data
            WHERE LENGTH(timestamp) > 17
        """)
        
        remaining_with_seconds = cursor.fetchone()[0]
        logger.info(f"Timestamps still with seconds: {remaining_with_seconds}")

if __name__ == "__main__":
    normalize_database_timestamps() 