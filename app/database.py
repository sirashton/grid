#!/usr/bin/env python3
"""
Database operations for the Grid Tracker
"""

import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional
from utils.timestamp_utils import normalize_timestamp
from utils.timestamp_utils import iso8601_to_sqlite_datetime

logger = logging.getLogger(__name__)

class Database:
    """Database operations for grid data"""
    
    def __init__(self, db_path: str = '/data/grid.db'):
        self.db_path = db_path
        self._ensure_database_exists()
    
    def _ensure_database_exists(self):
        """Ensure database file and tables exist"""
        # Ensure directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Create database and tables
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create carbon_intensity_30min_data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS carbon_intensity_30min_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL UNIQUE,
                    emissions INTEGER NOT NULL,
                    is_forecast BOOLEAN,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add is_forecast column if it doesn't exist (migration)
            try:
                cursor.execute("ALTER TABLE carbon_intensity_30min_data ADD COLUMN is_forecast BOOLEAN")
                logger.info("Added is_forecast column to carbon_intensity_30min_data table")
            except sqlite3.OperationalError:
                # Column already exists
                pass
            
            # Create index for efficient queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_carbon_intensity_30min_timestamp 
                ON carbon_intensity_30min_data(timestamp)
            """)
            
            # Create generation_30min_data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS generation_30min_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL UNIQUE,
                    settlement_period INTEGER,
                    biomass REAL,
                    fossil_gas REAL,
                    fossil_hard_coal REAL,
                    fossil_oil REAL,
                    hydro_pumped_storage REAL,
                    hydro_run_of_river REAL,
                    nuclear REAL,
                    other REAL,
                    solar REAL,
                    wind_offshore REAL,
                    wind_onshore REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create index for efficient queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_generation_30min_timestamp 
                ON generation_30min_data(timestamp)
            """)
            
            conn.commit()
            logger.info(f"Database initialized: {self.db_path}")
    
    def insert_carbon_intensity_data(self, timestamp: str, emissions: int, is_forecast: bool = False) -> bool:
        """
        Insert carbon intensity data
        
        Args:
            timestamp: ISO format timestamp string
            emissions: Carbon intensity in gCO2/kWh
            is_forecast: Whether this is a forecast value (True) or actual value (False)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Normalize timestamp to consistent format
            normalized_timestamp = normalize_timestamp(timestamp)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if record exists and whether it's a forecast
                cursor.execute("""
                    SELECT is_forecast FROM carbon_intensity_30min_data 
                    WHERE timestamp = ?
                """, (normalized_timestamp,))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    existing_is_forecast = existing_record[0]
                    
                    # If existing record is actual and new record is forecast, don't overwrite
                    if existing_is_forecast == 0 and is_forecast:
                        logger.debug(f"Skipping forecast update for {timestamp} - actual value already exists")
                        return True
                    
                    # If existing record is forecast and new record is actual, update
                    if existing_is_forecast == 1 and not is_forecast:
                        logger.info(f"Updating forecast to actual for {timestamp}: {emissions}")
                
                # Insert or replace the record
                cursor.execute("""
                    INSERT OR REPLACE INTO carbon_intensity_30min_data (timestamp, emissions, is_forecast)
                    VALUES (?, ?, ?)
                """, (normalized_timestamp, emissions, is_forecast))
                conn.commit()
                
                if cursor.rowcount > 0:
                    data_type = "forecast" if is_forecast else "actual"
                    logger.debug(f"Inserted/updated carbon intensity data ({data_type}): {normalized_timestamp} = {emissions}")
                    return True
                else:
                    logger.debug(f"Carbon intensity data unchanged: {normalized_timestamp}")
                    return True  # Not an error, just no change
                    
        except Exception as e:
            logger.error(f"Failed to insert carbon intensity data: {e}")
            return False
    
    def get_latest_carbon_intensity_data(self, limit: int = 1) -> List[Dict]:
        """
        Get the latest carbon intensity data points
        
        Args:
            limit: Number of records to return
            
        Returns:
            List of dictionaries with timestamp and emissions
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT timestamp, emissions
                    FROM carbon_intensity_30min_data
                    ORDER BY timestamp DESC
                    LIMIT ?
                """, (limit,))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Failed to get latest carbon intensity data: {e}")
            return []
    
    def get_carbon_intensity_history(self, hours: int = 24) -> List[Dict]:
        """
        Get historical carbon intensity data
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of dictionaries with timestamp and emissions
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT timestamp, emissions
                    FROM carbon_intensity_30min_data
                    WHERE timestamp > datetime('now', '-{} hours')
                    ORDER BY timestamp ASC
                """.format(hours))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Failed to get carbon intensity history: {e}")
            return []
    
    def get_carbon_intensity_data_count(self) -> int:
        """Get total number of carbon intensity data points"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM carbon_intensity_30min_data")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get carbon intensity data count: {e}")
            return 0
    
    def get_last_carbon_intensity_collection_time(self) -> Optional[str]:
        """Get timestamp of the most recent carbon intensity data collection"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT timestamp 
                    FROM carbon_intensity_30min_data 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """)
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get last carbon intensity collection time: {e}")
            return None
    
    def get_recent_forecast_records(self, hours: int = 24) -> List[Dict]:
        """
        Get recent forecast records that might need to be updated with actuals
        
        Args:
            hours: Number of hours to look back (default 24)
            
        Returns:
            List of dictionaries with timestamp and emissions
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT timestamp, emissions
                    FROM carbon_intensity_30min_data
                    WHERE is_forecast = 1 
                    AND timestamp > datetime('now', '-{} hours')
                    ORDER BY timestamp DESC
                """.format(hours))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Failed to get recent forecast records: {e}")
            return []
    
    def check_health(self) -> bool:
        """Check if database is healthy and accessible"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def insert_generation_data(self, **kwargs):
        """
        Insert or update a row in generation_30min_data using INSERT OR REPLACE to enforce uniqueness on timestamp and timestamp_sql.
        """
        import sqlite3
        db_path = '/data/grid.db' if not hasattr(self, 'db_path') else self.db_path
        columns = ', '.join(kwargs.keys())
        placeholders = ', '.join(['?'] * len(kwargs))
        sql = f"INSERT OR REPLACE INTO generation_30min_data ({columns}) VALUES ({placeholders})"
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, tuple(kwargs.values()))
                conn.commit()
            return True
        except Exception as e:
            print(f"[DB] Failed to insert/update generation data: {e}")
            return False
    
    def get_latest_generation_data(self, limit: int = 1) -> List[Dict]:
        """
        Get the latest generation data points
        
        Args:
            limit: Number of records to return
            
        Returns:
            List of dictionaries with timestamp and generation data
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT timestamp, settlement_period, biomass, fossil_gas, fossil_hard_coal,
                           fossil_oil, hydro_pumped_storage, hydro_run_of_river, nuclear,
                           other, solar, wind_offshore, wind_onshore
                    FROM generation_30min_data
                    ORDER BY timestamp DESC
                    LIMIT ?
                """, (limit,))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Failed to get latest generation data: {e}")
            return []
    
    def get_generation_stats(self) -> Dict:
        """Get generation database statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total records
                cursor.execute("SELECT COUNT(*) FROM generation_30min_data")
                total_records = cursor.fetchone()[0]
                
                # Date range
                cursor.execute("""
                    SELECT MIN(timestamp), MAX(timestamp) 
                    FROM generation_30min_data
                """)
                min_time, max_time = cursor.fetchone()
                
                return {
                    'total_records': total_records,
                    'earliest_data': min_time,
                    'latest_data': max_time,
                    'healthy': True
                }
                
        except Exception as e:
            logger.error(f"Failed to get generation stats: {e}")
            return {
                'total_records': 0,
                'earliest_data': None,
                'latest_data': None,
                'healthy': False
            }
    
    def get_carbon_intensity_stats(self) -> Dict:
        """Get carbon intensity database statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total records
                cursor.execute("SELECT COUNT(*) FROM carbon_intensity_30min_data")
                total_records = cursor.fetchone()[0]
                
                # Date range
                cursor.execute("""
                    SELECT MIN(timestamp), MAX(timestamp) 
                    FROM carbon_intensity_30min_data
                """)
                min_time, max_time = cursor.fetchone()
                
                return {
                    'total_records': total_records,
                    'earliest_data': min_time,
                    'latest_data': max_time,
                    'healthy': True
                }
                
        except Exception as e:
            logger.error(f"Failed to get carbon intensity stats: {e}")
            return {
                'total_records': 0,
                'earliest_data': None,
                'latest_data': None,
                'healthy': False
            }
    
    def _validate_granularity(self, granularity_minutes: int) -> bool:
        """Validate that granularity is a supported value"""
        supported_granularities = [30, 60, 120, 240, 360, 720, 1440]  # 30min to 24hr
        return granularity_minutes in supported_granularities
    
    def _get_supported_sources(self) -> List[str]:
        """Get list of supported energy sources"""
        return [
            'biomass', 'fossil_gas', 'fossil_hard_coal', 'fossil_oil',
            'hydro_pumped_storage', 'hydro_run_of_river', 'nuclear',
            'other', 'solar', 'wind_offshore', 'wind_onshore'
        ]
    
    def _validate_sources(self, sources: List[str]) -> List[str]:
        """Validate and return list of valid sources"""
        supported_sources = self._get_supported_sources()
        valid_sources = [source for source in sources if source in supported_sources]
        
        if len(valid_sources) != len(sources):
            invalid_sources = [source for source in sources if source not in supported_sources]
            logger.warning(f"Invalid sources requested: {invalid_sources}")
        
        return valid_sources
    
    def _parse_source_groups(self, groups_json: str) -> Dict[str, List[str]]:
        """Parse JSON string defining source groupings"""
        try:
            import json
            groups = json.loads(groups_json)
            
            # Validate that all sources in groups are valid
            supported_sources = self._get_supported_sources()
            validated_groups = {}
            
            for group_name, sources in groups.items():
                valid_sources = [source for source in sources if source in supported_sources]
                if valid_sources:
                    validated_groups[group_name] = valid_sources
                else:
                    logger.warning(f"Group '{group_name}' has no valid sources")
            
            return validated_groups
            
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"Failed to parse groups JSON: {e}")
            return {}
    
    # Remove get_generation_aggregated and all related aggregation/binning code

def migrate_add_timestamp_sql_column():
    """
    Add a timestamp_sql column to both tables if it doesn't exist, and backfill it for all rows.
    """
    import sqlite3
    from utils.timestamp_utils import iso8601_to_sql_datetime
    db_path = '/data/grid.db'
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        # Add column to generation_30min_data
        try:
            cursor.execute("ALTER TABLE generation_30min_data ADD COLUMN timestamp_sql DATETIME")
        except sqlite3.OperationalError:
            pass  # already exists
        # Add column to carbon_intensity_30min_data
        try:
            cursor.execute("ALTER TABLE carbon_intensity_30min_data ADD COLUMN timestamp_sql DATETIME")
        except sqlite3.OperationalError:
            pass  # already exists
        # Backfill generation_30min_data
        cursor.execute("SELECT id, timestamp FROM generation_30min_data WHERE timestamp_sql IS NULL OR timestamp_sql = ''")
        rows = cursor.fetchall()
        for row in rows:
            row_id, ts = row
            ts_sql = iso8601_to_sql_datetime(ts)
            cursor.execute("UPDATE generation_30min_data SET timestamp_sql = ? WHERE id = ?", (ts_sql, row_id))
        # Backfill carbon_intensity_30min_data
        cursor.execute("SELECT id, timestamp FROM carbon_intensity_30min_data WHERE timestamp_sql IS NULL OR timestamp_sql = ''")
        rows = cursor.fetchall()
        for row in rows:
            row_id, ts = row
            ts_sql = iso8601_to_sql_datetime(ts)
            cursor.execute("UPDATE carbon_intensity_30min_data SET timestamp_sql = ? WHERE id = ?", (ts_sql, row_id))
        conn.commit()