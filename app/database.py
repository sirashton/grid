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
    
    def insert_generation_data(self, timestamp: str, settlement_period: int, **fuel_data) -> bool:
        """
        Insert generation data by fuel type
        
        Args:
            timestamp: ISO format timestamp string
            settlement_period: Settlement period number
            **fuel_data: Keyword arguments for fuel types (biomass, fossil_gas, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Normalize timestamp to consistent format
            normalized_timestamp = normalize_timestamp(timestamp)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if record exists
                cursor.execute("""
                    SELECT id FROM generation_30min_data 
                    WHERE timestamp = ?
                """, (normalized_timestamp,))
                existing_record = cursor.fetchone()
                
                if existing_record:
                    logger.debug(f"Generation data already exists for {normalized_timestamp}")
                    return True  # Not an error, just no change
                
                # Insert the record
                cursor.execute("""
                    INSERT INTO generation_30min_data (
                        timestamp, settlement_period, biomass, fossil_gas, fossil_hard_coal,
                        fossil_oil, hydro_pumped_storage, hydro_run_of_river, nuclear,
                        other, solar, wind_offshore, wind_onshore
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    normalized_timestamp, settlement_period,
                    fuel_data.get('biomass'),
                    fuel_data.get('fossil_gas'),
                    fuel_data.get('fossil_hard_coal'),
                    fuel_data.get('fossil_oil'),
                    fuel_data.get('hydro_pumped_storage'),
                    fuel_data.get('hydro_run_of_river'),
                    fuel_data.get('nuclear'),
                    fuel_data.get('other'),
                    fuel_data.get('solar'),
                    fuel_data.get('wind_offshore'),
                    fuel_data.get('wind_onshore')
                ))
                conn.commit()
                
                if cursor.rowcount > 0:
                    logger.debug(f"Inserted generation data: {normalized_timestamp}")
                    return True
                else:
                    logger.debug(f"Generation data unchanged: {normalized_timestamp}")
                    return True  # Not an error, just no change
                    
        except Exception as e:
            logger.error(f"Failed to insert generation data: {e}")
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
    
    def get_generation_aggregated(
        self,
        start_time: datetime,
        end_time: datetime,
        granularity_minutes: int = 30,
        sources: List[str] = None,
        groups: Dict[str, List[str]] = None
    ) -> Dict:
        """
        Get aggregated generation data by time bins
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            granularity_minutes: Time bin size in minutes (30, 60, 120, 240, 360, 720, 1440)
            sources: List of energy sources to include (if None, includes all)
            groups: Dictionary of group_name -> list of sources for grouped data
            
        Returns:
            Dictionary with metadata and aggregated data
        """
        try:
            # Validate granularity
            if not self._validate_granularity(granularity_minutes):
                raise ValueError(f"Unsupported granularity: {granularity_minutes}")
            
            # Validate and filter sources
            if sources is None:
                sources = self._get_supported_sources()
            else:
                sources = self._validate_sources(sources)
            
            if not sources:
                raise ValueError("No valid sources specified")
            
            # Always get all supported sources for 'total' calculation
            all_sources = self._get_supported_sources()
            all_source_columns = []
            for source in all_sources:
                all_source_columns.extend([
                    f"AVG({source}) as {source}_avg",
                    f"MAX({source}) as {source}_max",
                    f"MIN({source}) as {source}_min",
                    f"COUNT({source}) as {source}_count"
                ])
            
            # Build SQL query for aggregation (for all sources)
            if granularity_minutes == 30:
                time_format = "strftime('%Y-%m-%dT%H:%M:00Z', timestamp)"
            elif granularity_minutes == 60:
                time_format = "strftime('%Y-%m-%dT%H:00:00Z', timestamp)"
            elif granularity_minutes == 120:
                time_format = "strftime('%Y-%m-%dT%H:00:00Z', timestamp, '+' || (strftime('%H', timestamp) / 2) * 2 || ' hours')"
            elif granularity_minutes == 240:
                time_format = "strftime('%Y-%m-%dT%H:00:00Z', timestamp, '+' || (strftime('%H', timestamp) / 4) * 4 || ' hours')"
            elif granularity_minutes == 360:
                time_format = "strftime('%Y-%m-%dT%H:00:00Z', timestamp, '+' || (strftime('%H', timestamp) / 6) * 6 || ' hours')"
            elif granularity_minutes == 720:
                time_format = "strftime('%Y-%m-%dT%H:00:00Z', timestamp, '+' || (strftime('%H', timestamp) / 12) * 12 || ' hours')"
            elif granularity_minutes == 1440:
                time_format = "strftime('%Y-%m-%dT00:00:00Z', timestamp)"
            else:
                time_format = "strftime('%Y-%m-%dT%H:%M:00Z', timestamp)"
            
            sql = f"""
                SELECT 
                    {time_format} as time_bin,
                    {', '.join(all_source_columns)}
                FROM generation_30min_data
                WHERE timestamp >= ? AND timestamp <= ?
                GROUP BY time_bin
                ORDER BY time_bin
            """
            
            # Execute query
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(sql, (
                    start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                ))
                
                rows = cursor.fetchall()
                
                # Process results
                data = []
                for row in rows:
                    time_bin = row[0]
                    row_data = row[1:]  # Skip time_bin column
                    
                    # Build sources data (only for requested sources)
                    sources_data = {}
                    # Each source has 4 columns in the SQL result: avg, max, min, count, in that order.
                    # So for source at index i, its columns are at i*4, i*4+1, i*4+2, i*4+3 in row_data.
                    for i, source in enumerate(all_sources):
                        avg_idx = i * 4
                        max_idx = i * 4 + 1
                        min_idx = i * 4 + 2
                        count_idx = i * 4 + 3
                        
                        avg_val = row_data[avg_idx]
                        max_val = row_data[max_idx]
                        min_val = row_data[min_idx]
                        count_val = row_data[count_idx]
                        
                        if source in sources:
                            if count_val > 0:
                                sources_data[source] = {
                                    "avg": round(avg_val, 2) if avg_val is not None else None,
                                    "high": round(max_val, 2) if max_val is not None else None,
                                    "low": round(min_val, 2) if min_val is not None else None,
                                    "data_points": count_val
                                }
                            else:
                                sources_data[source] = {
                                    "avg": None,
                                    "high": None,
                                    "low": None,
                                    "data_points": 0
                                }
                    
                    # Calculate groups if provided
                    groups_data = {}
                    if groups:
                        for group_name, group_sources in groups.items():
                            group_avgs = []
                            group_highs = []
                            group_lows = []
                            group_counts = []
                            
                            for source in group_sources:
                                idx = all_sources.index(source)
                                avg_idx = idx * 4
                                max_idx = idx * 4 + 1
                                min_idx = idx * 4 + 2
                                count_idx = idx * 4 + 3
                                avg_val = row_data[avg_idx]
                                max_val = row_data[max_idx]
                                min_val = row_data[min_idx]
                                count_val = row_data[count_idx]
                                if avg_val is not None:
                                    group_avgs.append(round(avg_val, 2))
                                    group_highs.append(round(max_val, 2))
                                    group_lows.append(round(min_val, 2))
                                    group_counts.append(count_val)
                            if group_avgs:
                                groups_data[group_name] = {
                                    "avg": round(sum(group_avgs), 2),
                                    "high": round(sum(group_highs), 2),
                                    "low": round(sum(group_lows), 2),
                                    "data_points": sum(group_counts)
                                }
                            else:
                                groups_data[group_name] = {
                                    "avg": None,
                                    "high": None,
                                    "low": None,
                                    "data_points": 0
                                }
                    # Always add 'total' group (sum of all sources)
                    total_avg = 0.0
                    total_high = 0.0
                    total_low = 0.0
                    total_count = 0
                    any_data = False
                    for i, source in enumerate(all_sources):
                        avg_idx = i * 4
                        max_idx = i * 4 + 1
                        min_idx = i * 4 + 2
                        count_idx = i * 4 + 3
                        avg_val = row_data[avg_idx]
                        max_val = row_data[max_idx]
                        min_val = row_data[min_idx]
                        count_val = row_data[count_idx]
                        if avg_val is not None:
                            total_avg += avg_val
                            total_high += max_val if max_val is not None else 0.0
                            total_low += min_val if min_val is not None else 0.0
                            total_count += count_val
                            any_data = True
                    if any_data:
                        groups_data["total"] = {
                            "avg": round(total_avg, 2),
                            "high": round(total_high, 2),
                            "low": round(total_low, 2),
                            "data_points": total_count
                        }
                    else:
                        groups_data["total"] = {
                            "avg": None,
                            "high": None,
                            "low": None,
                            "data_points": 0
                        }
                    
                    data.append({
                        "timestamp": time_bin,
                        "sources": sources_data,
                        "groups": groups_data
                    })
                
                # Calculate metadata
                total_bins = len(data)
                bins_with_data = sum(1 for bin_data in data if any(
                    source_data["data_points"] > 0 
                    for source_data in bin_data["sources"].values()
                ))
                
                return {
                    "metadata": {
                        "start_time": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "end_time": end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "granularity_minutes": granularity_minutes,
                        "time_bins": total_bins,
                        "data_quality": {
                            "total_expected_bins": total_bins,
                            "bins_with_data": bins_with_data,
                            "missing_bins": total_bins - bins_with_data
                        }
                    },
                    "data": data
                }
                
        except Exception as e:
            logger.error(f"Failed to get aggregated generation data: {e}")
            return {
                "metadata": {
                    "error": str(e),
                    "start_time": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "end_time": end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "granularity_minutes": granularity_minutes,
                    "time_bins": 0
                },
                "data": []
            } 