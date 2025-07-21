#!/usr/bin/env python3
"""
Interpolate single-point gaps in energy data

This script finds gaps where only a single data point is missing (start_time == end_time)
and fills them with interpolated values between the surrounding data points.
It also adds an 'is_interpolated' column to flag interpolated data.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Dict, Any
from data_gap_detector import DataGapDetector
from utils.timestamp_utils import normalize_timestamp

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GapInterpolator:
    """Interpolate single-point gaps in energy data"""
    
    def __init__(self, db_path: str = '/data/grid.db'):
        self.db_path = db_path
        self.gap_detector = DataGapDetector(db_path)
        
    def add_interpolation_columns(self):
        """Add is_interpolated column to both tables if it doesn't exist"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if column exists in carbon intensity table
                cursor.execute("PRAGMA table_info(carbon_intensity_30min_data)")
                columns = [col[1] for col in cursor.fetchall()]
                
                if 'is_interpolated' not in columns:
                    logger.info("Adding is_interpolated column to carbon_intensity_30min_data")
                    cursor.execute("ALTER TABLE carbon_intensity_30min_data ADD COLUMN is_interpolated BOOLEAN DEFAULT FALSE")
                
                # Check if column exists in generation table
                cursor.execute("PRAGMA table_info(generation_30min_data)")
                columns = [col[1] for col in cursor.fetchall()]
                
                if 'is_interpolated' not in columns:
                    logger.info("Adding is_interpolated column to generation_30min_data")
                    cursor.execute("ALTER TABLE generation_30min_data ADD COLUMN is_interpolated BOOLEAN DEFAULT FALSE")
                
                conn.commit()
                logger.info("Interpolation columns added successfully")
                
        except Exception as e:
            logger.error(f"Error adding interpolation columns: {e}")
            raise
    
    def find_single_point_gaps(self, table_name: str, granularity_minutes: int = 30) -> List[Tuple[datetime, datetime]]:
        """Find gaps where start_time == end_time (single point gaps)"""
        try:
            # Get all gaps
            logger.info(f"Detecting gaps in {table_name}...")
            all_gaps = self.gap_detector.detect_data_gaps(table_name, granularity_minutes)
            
            # Analyze and filter gaps
            single_gaps = []
            multi_point_gaps = []
            
            for gap_start, gap_end in all_gaps:
                # Calculate gap size in data points
                time_diff = (gap_end - gap_start).total_seconds() / (granularity_minutes * 60)
                gap_size = int(time_diff) + 1  # +1 because both start and end are missing
                
                if gap_start == gap_end:
                    # Single point gap
                    single_gaps.append((gap_start, gap_end))
                else:
                    # Multi-point gap
                    multi_point_gaps.append((gap_start, gap_end, gap_size))
            
            # Log gap analysis
            logger.info(f"Found {len(all_gaps)} total gaps in {table_name}")
            logger.info(f"  - {len(single_gaps)} single-point gaps")
            logger.info(f"  - {len(multi_point_gaps)} multi-point gaps")
            
            # Log details of single-point gaps
            if single_gaps:
                logger.info("Single-point gaps found:")
                for gap_start, gap_end in single_gaps[:10]:  # Show first 10
                    logger.info(f"  {gap_start}")
                if len(single_gaps) > 10:
                    logger.info(f"  ... and {len(single_gaps) - 10} more single-point gaps")
            
            # Log details of multi-point gaps
            if multi_point_gaps:
                logger.info("Multi-point gaps (skipping these):")
                for gap_start, gap_end, gap_size in multi_point_gaps[:10]:  # Show first 10
                    logger.info(f"  {gap_start} to {gap_end}: {gap_size} missing data points")
                if len(multi_point_gaps) > 10:
                    logger.info(f"  ... and {len(multi_point_gaps) - 10} more multi-point gaps")
            
            return single_gaps
            
        except Exception as e:
            logger.error(f"Error finding single-point gaps in {table_name}: {e}")
            return []
    
    def get_surrounding_data(self, table_name: str, gap_time: datetime, granularity_minutes: int = 30) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Get the data points before and after the gap"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Enable Row factory for dictionary-like access
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Calculate time range for surrounding data
                before_time = gap_time - timedelta(minutes=granularity_minutes)
                after_time = gap_time + timedelta(minutes=granularity_minutes)
                
                if table_name == 'carbon_intensity_30min_data':
                    # Get data before gap
                    cursor.execute("""
                        SELECT timestamp, emissions, is_forecast
                        FROM carbon_intensity_30min_data
                        WHERE timestamp = ?
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, (before_time.strftime('%Y-%m-%dT%H:%MZ'),))
                    before_row = cursor.fetchone()
                    
                    # Get data after gap
                    cursor.execute("""
                        SELECT timestamp, emissions, is_forecast
                        FROM carbon_intensity_30min_data
                        WHERE timestamp = ?
                        ORDER BY timestamp ASC
                        LIMIT 1
                    """, (after_time.strftime('%Y-%m-%dT%H:%MZ'),))
                    after_row = cursor.fetchone()
                    
                    # Convert to dict format using column names
                    before_dict = None
                    if before_row:
                        before_dict = {
                            'timestamp': before_row['timestamp'],
                            'emissions': before_row['emissions'],
                            'is_forecast': bool(before_row['is_forecast'])
                        }
                    
                    after_dict = None
                    if after_row:
                        after_dict = {
                            'timestamp': after_row['timestamp'],
                            'emissions': after_row['emissions'],
                            'is_forecast': bool(after_row['is_forecast'])
                        }
                    
                    return before_dict, after_dict
                    
                elif table_name == 'generation_30min_data':
                    # Get data before gap
                    cursor.execute("""
                        SELECT timestamp, settlement_period, biomass, fossil_gas, fossil_hard_coal, 
                               fossil_oil, hydro_pumped_storage, hydro_run_of_river, nuclear, 
                               other, solar, wind_offshore, wind_onshore
                        FROM generation_30min_data
                        WHERE timestamp = ?
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, (before_time.strftime('%Y-%m-%dT%H:%MZ'),))
                    before_row = cursor.fetchone()
                    
                    # Get data after gap
                    cursor.execute("""
                        SELECT timestamp, settlement_period, biomass, fossil_gas, fossil_hard_coal, 
                               fossil_oil, hydro_pumped_storage, hydro_run_of_river, nuclear, 
                               other, solar, wind_offshore, wind_onshore
                        FROM generation_30min_data
                        WHERE timestamp = ?
                        ORDER BY timestamp ASC
                        LIMIT 1
                    """, (after_time.strftime('%Y-%m-%dT%H:%MZ'),))
                    after_row = cursor.fetchone()
                    
                    # Convert to dict format using column names
                    before_dict = None
                    if before_row:
                        before_dict = {
                            'timestamp': before_row['timestamp'],
                            'settlement_period': before_row['settlement_period'],
                            'biomass': before_row['biomass'],
                            'fossil_gas': before_row['fossil_gas'],
                            'fossil_hard_coal': before_row['fossil_hard_coal'],
                            'fossil_oil': before_row['fossil_oil'],
                            'hydro_pumped_storage': before_row['hydro_pumped_storage'],
                            'hydro_run_of_river': before_row['hydro_run_of_river'],
                            'nuclear': before_row['nuclear'],
                            'other': before_row['other'],
                            'solar': before_row['solar'],
                            'wind_offshore': before_row['wind_offshore'],
                            'wind_onshore': before_row['wind_onshore']
                        }
                    
                    after_dict = None
                    if after_row:
                        after_dict = {
                            'timestamp': after_row['timestamp'],
                            'settlement_period': after_row['settlement_period'],
                            'biomass': after_row['biomass'],
                            'fossil_gas': after_row['fossil_gas'],
                            'fossil_hard_coal': after_row['fossil_hard_coal'],
                            'fossil_oil': after_row['fossil_oil'],
                            'hydro_pumped_storage': after_row['hydro_pumped_storage'],
                            'hydro_run_of_river': after_row['hydro_run_of_river'],
                            'nuclear': after_row['nuclear'],
                            'other': after_row['other'],
                            'solar': after_row['solar'],
                            'wind_offshore': after_row['wind_offshore'],
                            'wind_onshore': after_row['wind_onshore']
                        }
                    
                    return before_dict, after_dict
                
        except Exception as e:
            logger.error(f"Error getting surrounding data for {table_name}: {e}")
            return None, None
    
    def interpolate_carbon_intensity(self, before_data: Dict, after_data: Dict, gap_time: datetime) -> Optional[Dict]:
        """Interpolate carbon intensity data between two points"""
        try:
            if not before_data or not after_data:
                return None
            
            # Parse timestamps
            before_time = datetime.fromisoformat(before_data['timestamp'].replace('Z', '+00:00'))
            after_time = datetime.fromisoformat(after_data['timestamp'].replace('Z', '+00:00'))
            
            # Calculate interpolation factor (0 = before, 1 = after)
            total_diff = (after_time - before_time).total_seconds()
            gap_diff = (gap_time - before_time).total_seconds()
            factor = gap_diff / total_diff if total_diff > 0 else 0.5
            
            # Interpolate emissions
            before_emissions = before_data['emissions']
            after_emissions = after_data['emissions']
            interpolated_emissions = before_emissions + (after_emissions - before_emissions) * factor
            
            # Use forecast if either surrounding point is forecast
            is_forecast = before_data['is_forecast'] or after_data['is_forecast']
            
            return {
                'timestamp': gap_time.strftime('%Y-%m-%dT%H:%MZ'),
                'emissions': round(interpolated_emissions, 1),
                'is_forecast': is_forecast,
                'is_interpolated': True
            }
            
        except Exception as e:
            logger.error(f"Error interpolating carbon intensity data: {e}")
            return None
    
    def interpolate_generation(self, before_data: Dict, after_data: Dict, gap_time: datetime) -> Optional[Dict]:
        """Interpolate generation data between two points"""
        try:
            if not before_data or not after_data:
                return None
            
            # Parse timestamps
            before_time = datetime.fromisoformat(before_data['timestamp'].replace('Z', '+00:00'))
            after_time = datetime.fromisoformat(after_data['timestamp'].replace('Z', '+00:00'))
            
            # Calculate interpolation factor
            total_diff = (after_time - before_time).total_seconds()
            gap_diff = (gap_time - before_time).total_seconds()
            factor = gap_diff / total_diff if total_diff > 0 else 0.5
            
            # Interpolate each fuel type
            interpolated_data = {
                'timestamp': gap_time.strftime('%Y-%m-%dT%H:%MZ'),
                'is_interpolated': True
            }
            
            # Handle settlement period (use the one from before data)
            interpolated_data['settlement_period'] = before_data['settlement_period']
            
            # Interpolate each fuel type
            fuel_types = ['biomass', 'fossil_gas', 'fossil_hard_coal', 'fossil_oil', 
                         'hydro_pumped_storage', 'hydro_run_of_river', 'nuclear', 
                         'other', 'solar', 'wind_offshore', 'wind_onshore']
            
            for fuel_type in fuel_types:
                before_val = before_data.get(fuel_type, 0)
                after_val = after_data.get(fuel_type, 0)
                
                if before_val is not None and after_val is not None:
                    interpolated_val = before_val + (after_val - before_val) * factor
                    interpolated_data[fuel_type] = round(interpolated_val, 3)
                else:
                    interpolated_data[fuel_type] = before_val if before_val is not None else after_val
            
            return interpolated_data
            
        except Exception as e:
            logger.error(f"Error interpolating generation data: {e}")
            return None
    
    def insert_interpolated_data(self, table_name: str, data: Dict) -> bool:
        """Insert interpolated data into the database"""
        try:
            # Normalize timestamp to consistent format
            normalized_timestamp = normalize_timestamp(data['timestamp'])
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if table_name == 'carbon_intensity_30min_data':
                    cursor.execute("""
                        INSERT INTO carbon_intensity_30min_data 
                        (timestamp, emissions, is_forecast, is_interpolated)
                        VALUES (?, ?, ?, ?)
                    """, (normalized_timestamp, data['emissions'], data['is_forecast'], True))
                    
                elif table_name == 'generation_30min_data':
                    cursor.execute("""
                        INSERT INTO generation_30min_data 
                        (timestamp, settlement_period, biomass, fossil_gas, fossil_hard_coal,
                         fossil_oil, hydro_pumped_storage, hydro_run_of_river, nuclear,
                         other, solar, wind_offshore, wind_onshore, is_interpolated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        normalized_timestamp, data['settlement_period'], data['biomass'],
                        data['fossil_gas'], data['fossil_hard_coal'], data['fossil_oil'],
                        data['hydro_pumped_storage'], data['hydro_run_of_river'], data['nuclear'],
                        data['other'], data['solar'], data['wind_offshore'], data['wind_onshore'], True
                    ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error inserting interpolated data into {table_name}: {e}")
            return False
    
    def interpolate_table_gaps(self, table_name: str, granularity_minutes: int = 30) -> int:
        """Interpolate all single-point gaps in a table"""
        try:
            logger.info(f"Starting interpolation for {table_name}")
            
            # Find single-point gaps
            single_gaps = self.find_single_point_gaps(table_name, granularity_minutes)
            
            if not single_gaps:
                logger.info(f"No single-point gaps found in {table_name}")
                return 0
            
            filled_count = 0
            
            for gap_start, gap_end in single_gaps:
                try:
                    # Get surrounding data
                    before_data, after_data = self.get_surrounding_data(table_name, gap_start, granularity_minutes)
                    
                    if not before_data or not after_data:
                        logger.warning(f"Cannot interpolate {gap_start}: missing surrounding data")
                        continue
                    
                    # Interpolate data
                    if table_name == 'carbon_intensity_30min_data':
                        interpolated_data = self.interpolate_carbon_intensity(before_data, after_data, gap_start)
                    elif table_name == 'generation_30min_data':
                        interpolated_data = self.interpolate_generation(before_data, after_data, gap_start)
                    else:
                        continue
                    
                    if interpolated_data:
                        # Insert interpolated data
                        if self.insert_interpolated_data(table_name, interpolated_data):
                            filled_count += 1
                            logger.info(f"Interpolated gap at {gap_start}")
                        else:
                            logger.error(f"Failed to insert interpolated data for {gap_start}")
                    else:
                        logger.warning(f"Failed to interpolate data for {gap_start}")
                        
                except Exception as e:
                    logger.error(f"Error processing gap {gap_start}: {e}")
                    continue
            
            logger.info(f"Interpolation complete for {table_name}: {filled_count} gaps filled")
            return filled_count
            
        except Exception as e:
            logger.error(f"Error interpolating gaps in {table_name}: {e}")
            return 0
    
    def run_interpolation(self):
        """Run interpolation for both tables"""
        try:
            logger.info("Starting gap interpolation process")
            
            # Add interpolation columns
            self.add_interpolation_columns()
            
            # Interpolate carbon intensity gaps
            carbon_filled = self.interpolate_table_gaps('carbon_intensity_30min_data', 30)
            
            # Interpolate generation gaps
            generation_filled = self.interpolate_table_gaps('generation_30min_data', 30)
            
            logger.info(f"Interpolation complete!")
            logger.info(f"Carbon intensity gaps filled: {carbon_filled}")
            logger.info(f"Generation gaps filled: {generation_filled}")
            logger.info(f"Total gaps filled: {carbon_filled + generation_filled}")
            
        except Exception as e:
            logger.error(f"Error in interpolation process: {e}")
            raise

def main():
    """Main function"""
    try:
        interpolator = GapInterpolator()
        interpolator.run_interpolation()
        print("✅ Interpolation completed successfully!")
        
    except Exception as e:
        logger.error(f"Interpolation failed: {e}")
        print(f"❌ Interpolation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 