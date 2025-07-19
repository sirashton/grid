#!/usr/bin/env python3
"""
Configuration settings for the Grid Tracker
"""

import os
from datetime import timedelta

class Config:
    """Configuration settings"""
    
    # Database settings
    DATABASE_PATH = os.getenv('DATABASE_PATH', '/data/grid.db')
    
    # Timing intervals (in seconds)
    CARBON_INTENSITY_COLLECTION_INTERVAL = 60  # x seconds
    ELEXON_BM_REPORTS_COLLECTION_INTERVAL = 3600  # 1 hour
    NESO_DATA_PORTAL_COLLECTION_INTERVAL = 3600  # 1 hour
    HEALTH_CHECK_INTERVAL = 300  # 5 minutes
    MAIN_LOOP_INTERVAL = 10  # seconds
    
    # Web server settings
    WEB_SERVER_PORT = int(os.getenv('WEB_SERVER_PORT', 8000))
    WEB_SERVER_HOST = os.getenv('WEB_SERVER_HOST', '0.0.0.0')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', '/logs/grid_tracker.log')
    
    # Backfill settings
    BACKFILL_INTERVAL = 60  # X Seconds
    
    # Forecast update settings
    FORECAST_UPDATE_INTERVAL = 3600  # 1 hour
    
    # Backfill configuration for each data source
    BACKFILL_CONFIG = {
        'carbon_intensity_30min_data': {
            'target_oldest_days': 365*7,  # Target X days of historical data
            'hours_per_call': 7 * 24,  # 7 days worth of data per API call
            'max_calls_per_cycle': 10,  # Maximum API calls per backfill cycle
        },
        'generation_30min_data': {
            'target_oldest_days': 365*2,  # Target 2 years of historical data
            'hours_per_call': 7 * 24,  # 7 days worth of data per API call
            'max_calls_per_cycle': 5,  # Maximum API calls per backfill cycle
        }
        # Future data sources can be added here:
        # 'neso_data_portal': { ... },
    } 