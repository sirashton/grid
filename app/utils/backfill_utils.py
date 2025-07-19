#!/usr/bin/env python3
"""
Backfill utilities for grid tracker
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Callable, Dict, List, Tuple, Optional
from utils.database_utils import get_table_stats

logger = logging.getLogger(__name__)

def backfill_table_data(
    table_name: str,
    api_function: Callable,
    config: Dict,
    db_insert_function: Callable
) -> bool:
    """
    Backfill historical data for a specific table
    
    Args:
        table_name: Name of the table to backfill
        api_function: Function to call for fetching data (takes start_time, end_time)
        config: Configuration dict with backfill parameters
        db_insert_function: Function to insert data into database
        
    Returns:
        True if backfill completed successfully, False otherwise
    """
    try:
        logger.info(f"Starting backfill for {table_name}")
        
        # Get current table stats
        stats = get_table_stats(table_name)
        if 'error' in stats:
            logger.error(f"Could not get stats for {table_name}: {stats['error']}")
            return False
        
        if not stats['has_data']:
            logger.info(f"Table {table_name} is empty, will backfill from current time")
            current_time = datetime.now(timezone.utc)
            oldest_timestamp = current_time
        else:
            # Parse the oldest timestamp
            oldest_timestamp_str = stats['earliest_timestamp']
            try:
                if oldest_timestamp_str.endswith('Z'):
                    oldest_timestamp = datetime.fromisoformat(oldest_timestamp_str.replace('Z', '+00:00'))
                else:
                    oldest_timestamp = datetime.fromisoformat(oldest_timestamp_str)
            except ValueError as e:
                logger.error(f"Could not parse oldest timestamp {oldest_timestamp_str}: {e}")
                return False
        
        # Calculate target oldest time
        target_oldest_time = datetime.now(timezone.utc) - timedelta(days=config['target_oldest_days'])
        
        # Check if we need to backfill
        if oldest_timestamp <= target_oldest_time:
            logger.info(f"Table {table_name} already has {config['target_oldest_days']} days of data")
            return True
        
        # Calculate how far back we need to go
        hours_to_backfill = int((oldest_timestamp - target_oldest_time).total_seconds() / 3600)
        logger.info(f"Need to backfill {hours_to_backfill} hours for {table_name}")
        
        # Calculate number of API calls needed
        hours_per_call = config['hours_per_call']
        max_calls = config['max_calls_per_cycle']
        calls_needed = min(max_calls, (hours_to_backfill + hours_per_call - 1) // hours_per_call)
        
        logger.info(f"Will make {calls_needed} API calls (max {max_calls} per cycle)")
        
        # Make API calls to backfill data
        total_inserted = 0
        current_start = oldest_timestamp - timedelta(hours=hours_per_call * calls_needed)
        
        for call_num in range(calls_needed):
            try:
                # Calculate time range for this call
                call_start = current_start + timedelta(hours=hours_per_call * call_num)
                call_end = call_start + timedelta(hours=hours_per_call)
                
                # Ensure we don't go beyond the oldest timestamp we already have
                if call_end >= oldest_timestamp:
                    call_end = oldest_timestamp
                
                logger.info(f"Backfill call {call_num + 1}/{calls_needed}: {call_start} to {call_end}")
                
                # Fetch data from API
                data_points = api_function(call_start, call_end)
                
                if not data_points:
                    logger.warning(f"No data received for backfill call {call_num + 1}")
                    continue
                
                # Insert data into database
                inserted_count = 0
                for point in data_points:
                    # Use table name to determine insertion method
                    if table_name == 'carbon_intensity_30min_data':
                        success = db_insert_function(
                            timestamp=point['timestamp'],
                            emissions=point['emissions'],
                            is_forecast=point.get('is_forecast', False)
                        )
                    elif table_name == 'generation_30min_data':
                        success = db_insert_function(
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
                    else:
                        # Generic approach for unknown tables
                        success = db_insert_function(**point)
                    
                    if success:
                        inserted_count += 1
                
                total_inserted += inserted_count
                logger.info(f"Backfill call {call_num + 1}: inserted {inserted_count} points")
                
            except Exception as e:
                logger.error(f"Backfill call {call_num + 1} failed: {e}")
                return False
        
        logger.info(f"Backfill for {table_name} completed: {total_inserted} total points inserted")
        return True
        
    except Exception as e:
        logger.error(f"Backfill for {table_name} failed: {e}")
        return False

def run_backfill_cycle(
    backfill_configs: Dict,
    api_functions: Dict[str, Callable],
    db_insert_functions: Dict[str, Callable]
) -> bool:
    """
    Run a complete backfill cycle for all configured tables
    
    Args:
        backfill_configs: Configuration for each table
        api_functions: Mapping of table names to API functions
        db_insert_functions: Mapping of table names to database insert functions
        
    Returns:
        True if all backfills completed successfully, False if any failed
    """
    logger.info("Starting backfill cycle")
    
    success = True
    for table_name, config in backfill_configs.items():
        if table_name not in api_functions:
            logger.error(f"No API function configured for {table_name}")
            success = False
            continue
            
        if table_name not in db_insert_functions:
            logger.error(f"No database insert function configured for {table_name}")
            success = False
            continue
        
        table_success = backfill_table_data(
            table_name=table_name,
            api_function=api_functions[table_name],
            config=config,
            db_insert_function=db_insert_functions[table_name]
        )
        
        if not table_success:
            logger.error(f"Backfill failed for {table_name}")
            success = False
    
    if success:
        logger.info("Backfill cycle completed successfully")
    else:
        logger.error("Backfill cycle failed")
    
    return success 