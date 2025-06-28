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

from config import Config
from database import Database
from carbon_intensity_api import CarbonIntensityAPI

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
        
        # Track last collection times for each data source
        self.last_carbon_intensity_collection = None
        self.last_elexon_bm_reports_collection = None
        self.last_neso_data_portal_collection = None
        self.last_health_check = None
        
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
                    
                    if time_since_latest.total_seconds() < 3600:  # 60 minutes
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
                # else:
                #     print(f"Carbon intensity collection not due at {datetime.now()}")
                
                if self.should_run_health_check():
                    success = self.run_health_check()
                    self.last_health_check = time.time()
                    if success:
                        print(f"Health check completed at {datetime.now()}")
                    else:
                        print(f"Health check failed at {datetime.now()}")
                # else:
                #     print(f"Health check not due at {datetime.now()}")
                
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