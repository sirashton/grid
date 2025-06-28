#!/usr/bin/env python3
"""
Configuration settings for the Grid Tracker
"""

import os

class Config:
    """Configuration settings"""
    
    # Database settings
    DATABASE_PATH = os.getenv('DATABASE_PATH', '/data/grid.db')
    
    # Timing intervals (in seconds)
    CARBON_INTENSITY_COLLECTION_INTERVAL = int(os.getenv('CARBON_INTENSITY_COLLECTION_INTERVAL', 300))  # 5 minutes
    HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', 60))  # 1 minute
    MAIN_LOOP_INTERVAL = int(os.getenv('MAIN_LOOP_INTERVAL', 30))  # 30 seconds
    
    # Web server settings
    WEB_SERVER_PORT = int(os.getenv('WEB_SERVER_PORT', 8000))
    WEB_SERVER_HOST = os.getenv('WEB_SERVER_HOST', '0.0.0.0')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', '/logs/grid_tracker.log') 