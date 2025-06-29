#!/usr/bin/env python3
"""
UK Energy Grid Tracker - Main Orchestrator
Runs the main loop with scheduled tasks for data collection and monitoring
"""

import time
import logging
import signal
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple

from config import Config
from database import Database
from carbon_intensity_api import CarbonIntensityAPI
from data_gap_detector import DataGapDetector
from utils.backfill_utils import run_backfill_cycle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/grid_tracker.log')
    ]
)
logger = logging.getLogger(__name__)

# Prevent duplicate logging
logger.propagate = False

class GridTracker:
    """Main orchestrator for the grid tracking system"""
    
    def __init__(self):
        self.config = Config()
        self.db = Database()
        self.carbon_intensity_api = CarbonIntensityAPI()
        self.gap_detector = DataGapDetector()
        
        # Track last collection times for each data source
        self.last_carbon_intensity_collection = None
        self.last_elexon_bm_reports_collection = None
        self.last_neso_data_portal_collection = None
        self.last_health_check = None
        self.last_backfill = None
        
        # Control flag for graceful shutdown
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def should_run_carbon_intensity_collection(self) -> bool:
        """Check if it's time to collect carbon intensity data"""
        if not self.last_carbon_intensity_collection:
            return True
        
        time_since_last = time.time() - self.last_carbon_intensity_collection
        return time_since_last >= self.config.CARBON_INTENSITY_COLLECTION_INTERVAL
    
    def should_run_health_check(self) -> bool:
        """Check if it's time to run health check"""
        if not self.last_health_check:
            return True
        
        time_since_last = time.time() - self.last_health_check
        return time_since_last >= self.config.HEALTH_CHECK_INTERVAL
    
    def should_run_backfill(self) -> bool:
        """Check if it's time to run backfill"""
        if not self.last_backfill:
            return True
        
        time_since_last = time.time() - self.last_backfill
        return time_since_last >= self.config.BACKFILL_INTERVAL
    
    def collect_carbon_intensity_data(self) -> bool:
        """Collect carbon intensity data with smart gap detection"""
        try:
            logger.info("Starting carbon intensity data collection...")
            print("--------------------------------")
            print("Carbon intensity data collection started")
            print("--------------------------------\n")

            # Get latest data from database
            latest_data = self.db.get_latest_carbon_intensity_data(limit=1)
            current_time = datetime.now(timezone.utc)
            
            if latest_data:
                # Parse the latest timestamp
                latest_timestamp_str = latest_data[0]['timestamp']
                
                try:
                    if latest_timestamp_str.endswith('Z'):
                        latest_timestamp = datetime.fromisoformat(latest_timestamp_str.replace('Z', '+00:00'))
                    else:
                        latest_timestamp = datetime.fromisoformat(latest_timestamp_str)
                    
                    # Check if data is fresh enough (< 60 mins old)
                    time_since_latest = current_time - latest_timestamp
                    
                    if time_since_latest.total_seconds() < 1800:  # 30 minutes
                        print(f"Carbon intensity data is fresh ({time_since_latest.total_seconds()/60:.1f} minutes old), skipping collection")
                        logger.info(f"Carbon intensity data is fresh ({time_since_latest.total_seconds()/60:.1f} minutes old), skipping collection")
                        return True
                    
                    # Data is stale, fetch from latest timestamp to current time
                    start_time = latest_timestamp
                    end_time = current_time
                    gap_hours = time_since_latest.total_seconds() / 3600
                    
                    print(f"Carbon intensity data is {gap_hours:.1f} hours old, fetching missing data")
                    logger.info(f"Carbon intensity data is {gap_hours:.1f} hours old, fetching missing data")
                    
                except ValueError as e:
                    logger.warning(f"Could not parse latest timestamp {latest_timestamp_str}: {e}")
                    # If we can't parse the timestamp, fetch last 6 hours
                    start_time = current_time - timedelta(hours=6)
                    end_time = current_time
                    gap_hours = 6
                    logger.info("Could not parse latest timestamp, fetching last 6 hours of data")
            else:
                # Database is empty, fetch last 6 hours
                start_time = current_time - timedelta(hours=6)
                end_time = current_time
                gap_hours = 6
                logger.info("Database empty, fetching last 6 hours of carbon intensity data")
            
            # Fetch data from API
            data_points = self.carbon_intensity_api.get_intensity_data(start_time, end_time)
            
            if not data_points:
                logger.warning("No carbon intensity data points received from API")
                return False
            
            # Store data in database
            inserted_count = 0
            for point in data_points:
                success = self.db.insert_carbon_intensity_data(
                    timestamp=point['timestamp'],
                    emissions=point['emissions']
                )
                if success:
                    inserted_count += 1
            
            logger.info(f"Carbon intensity collection complete: {inserted_count} points collected to fill {gap_hours:.1f} hour gap")
            return True
            
        except Exception as e:
            logger.error(f"Carbon intensity data collection failed: {e}")
            return False
    
    def run_health_check(self) -> bool:
        """Run system health check"""
        try:
            logger.info("Running health check...")
            
            # Check database health
            db_healthy = self.db.check_health()
            if db_healthy:
                stats = self.db.get_carbon_intensity_stats()
                print(f"Database healthy: {stats['total_records']} carbon intensity records")
                
                # Check for data gaps
                gaps = self.gap_detector.detect_data_gaps(
                    table_name='carbon_intensity_30min_data',
                    granularity_minutes=30
                )
                
                if gaps:
                    print(f"Found {len(gaps)} data gaps in carbon intensity data:")
                    for gap_start, gap_end in gaps[:5]:  # Show first 5 gaps
                        print(f"  Missing: {gap_start.isoformat()}")
                    if len(gaps) > 5:
                        print(f"  ... and {len(gaps) - 5} more gaps")
                    
                    # Attempt to fill the gaps
                    print("Attempting to fill gaps...")
                    gap_fill_success = self.fill_data_gaps('carbon_intensity_30min_data', 30)
                    if gap_fill_success:
                        print("Gap filling completed")
                    else:
                        print("Gap filling failed")
                else:
                    print("No data gaps detected in carbon intensity data")
                
            else:
                print("Database health check failed")
            
            # Check API health
            api_healthy = self.carbon_intensity_api.check_health()
            if api_healthy:
                print("Carbon Intensity API healthy")
            else:
                print("Carbon Intensity API health check failed")
            
            logger.info("Health check complete")
            return db_healthy and api_healthy
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def detect_and_report_gaps(self, table_name: str, granularity_minutes: int) -> List[Tuple[datetime, datetime]]:
        """Detect and report gaps in data"""
        try:
            gaps = self.gap_detector.detect_data_gaps(
                table_name=table_name,
                granularity_minutes=granularity_minutes
            )
            
            if gaps:
                logger.info(f"Found {len(gaps)} gaps in {table_name}")
                print(f"Found {len(gaps)} gaps in {table_name}:")
                for gap_start, gap_end in gaps[:10]:  # Show first 10 gaps
                    print(f"  Missing: {gap_start.isoformat()}")
                if len(gaps) > 10:
                    print(f"  ... and {len(gaps) - 10} more gaps")
            else:
                logger.info(f"No gaps found in {table_name}")
                print(f"No gaps found in {table_name}")
            
            return gaps
            
        except Exception as e:
            logger.error(f"Error detecting gaps in {table_name}: {e}")
            return []
    
    def fill_data_gaps(self, table_name: str, granularity_minutes: int) -> bool:
        """Detect and fill gaps in data"""
        try:
            logger.info(f"Checking for gaps in {table_name}...")
            
            # Detect gaps
            gaps = self.gap_detector.detect_data_gaps(
                table_name=table_name,
                granularity_minutes=granularity_minutes
            )
            
            if not gaps:
                logger.info(f"No gaps found in {table_name}")
                return True
            
            logger.info(f"Found {len(gaps)} gaps in {table_name}, attempting to fill...")
            print(f"Found {len(gaps)} gaps in carbon intensity data, attempting to fill...")
            
            # Group consecutive gaps to minimize API calls
            gap_ranges = self._group_consecutive_gaps(gaps, granularity_minutes)
            
            total_filled = 0
            for gap_start, gap_end in gap_ranges:
                try:
                    # Fetch data for this gap range
                    data_points = self.carbon_intensity_api.get_intensity_data(gap_start, gap_end)
                    
                    if data_points:
                        # Store the data
                        inserted_count = 0
                        for point in data_points:
                            success = self.db.insert_carbon_intensity_data(
                                timestamp=point['timestamp'],
                                emissions=point['emissions']
                            )
                            if success:
                                inserted_count += 1
                        
                        total_filled += inserted_count
                        logger.info(f"Filled gap {gap_start} to {gap_end}: {inserted_count} points")
                        print(f"  Filled gap {gap_start.strftime('%Y-%m-%d %H:%M')} to {gap_end.strftime('%Y-%m-%d %H:%M')}: {inserted_count} points")
                    else:
                        logger.warning(f"No data received for gap {gap_start} to {gap_end}")
                        print(f"  No data received for gap {gap_start.strftime('%Y-%m-%d %H:%M')} to {gap_end.strftime('%Y-%m-%d %H:%M')}")
                        
                except Exception as e:
                    logger.error(f"Error filling gap {gap_start} to {gap_end}: {e}")
                    print(f"  Error filling gap {gap_start.strftime('%Y-%m-%d %H:%M')} to {gap_end.strftime('%Y-%m-%d %H:%M')}: {e}")
            
            logger.info(f"Gap filling complete: {total_filled} points filled")
            print(f"Gap filling complete: {total_filled} points filled")
            return True
            
        except Exception as e:
            logger.error(f"Error in gap filling: {e}")
            return False
    
    def _group_consecutive_gaps(self, gaps: List[Tuple[datetime, datetime]], granularity_minutes: int) -> List[Tuple[datetime, datetime]]:
        """Group consecutive gaps into ranges to minimize API calls"""
        if not gaps:
            return []
        
        # Sort gaps by start time
        sorted_gaps = sorted(gaps, key=lambda x: x[0])
        
        gap_ranges = []
        current_start = sorted_gaps[0][0]
        current_end = sorted_gaps[0][1]
        
        for gap_start, gap_end in sorted_gaps[1:]:
            # Check if this gap is consecutive with the current range
            expected_next = current_end + timedelta(minutes=granularity_minutes)
            
            if gap_start == expected_next:
                # Consecutive gap, extend the range
                current_end = gap_end
            else:
                # Non-consecutive gap, save current range and start new one
                gap_ranges.append((current_start, current_end))
                current_start = gap_start
                current_end = gap_end
        
        # Add the last range
        gap_ranges.append((current_start, current_end))
        
        return gap_ranges
    
    def run_backfill(self) -> bool:
        """Run backfill for all configured data sources"""
        try:
            logger.info("Starting backfill cycle...")
            print("Starting backfill cycle...")
            
            # Set up API functions mapping
            api_functions = {
                'carbon_intensity_30min_data': self.carbon_intensity_api.get_intensity_data
            }
            
            # Set up database insert functions mapping
            db_insert_functions = {
                'carbon_intensity_30min_data': self.db.insert_carbon_intensity_data
            }
            
            # Run backfill cycle
            success = run_backfill_cycle(
                backfill_configs=self.config.BACKFILL_CONFIG,
                api_functions=api_functions,
                db_insert_functions=db_insert_functions
            )
            
            if success:
                print("Backfill cycle completed successfully")
            else:
                print("Backfill cycle failed")
            
            return success
            
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            print(f"Backfill failed: {e}")
            return False
    
    def main_loop(self):
        """Main scheduling loop"""
        logger.info("Starting Grid Tracker main loop...")
        print("Grid Tracker starting up...")
        
        # Main loop
        while self.running:
            try:
                # Check and run scheduled tasks
                if self.should_run_carbon_intensity_collection():
                    success = self.collect_carbon_intensity_data()
                    if success:
                        self.last_carbon_intensity_collection = time.time()
                        print(f"Carbon intensity collection completed at {datetime.now()}")
                    else:
                        print(f"Carbon intensity collection failed at {datetime.now()}")
                
                if self.should_run_health_check():
                    success = self.run_health_check()
                    self.last_health_check = time.time()
                    if success:
                        print(f"Health check completed at {datetime.now()}")
                    else:
                        print(f"Health check failed at {datetime.now()}")
                
                if self.should_run_backfill():
                    success = self.run_backfill()
                    self.last_backfill = time.time()
                    if success:
                        print(f"Backfill completed at {datetime.now()}")
                    else:
                        print(f"Backfill failed at {datetime.now()}")
                
                # Sleep for a short interval
                time.sleep(self.config.MAIN_LOOP_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                print(f"Error in main loop: {e}")
                time.sleep(60)  # Wait longer on error
        
        logger.info("Grid Tracker main loop stopped")
        print("Grid Tracker shutting down...")

def main():
    """Main entry point"""
    # Ensure log directory exists
    Path("/logs").mkdir(exist_ok=True)
    
    # Create and run tracker
    tracker = GridTracker()
    tracker.main_loop()

if __name__ == "__main__":
    main() 