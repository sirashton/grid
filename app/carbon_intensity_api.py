#!/usr/bin/env python3
"""
Carbon Intensity API client
"""

import requests
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class CarbonIntensityAPI:
    """Client for the Carbon Intensity API"""
    
    def __init__(self, base_url: str = "https://api.carbonintensity.org.uk"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'GridTracker/1.0'
        })
    
    def get_intensity_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """
        Get carbon intensity data for a specific time range
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            List of data points with timestamp and emissions
        """
        try:
            # If start and end times are the same, add a minute to end time
            # to ensure we get at least one data point from the API
            if start_time == end_time:
                end_time = end_time + timedelta(minutes=1)
            
            # Format timestamps for API
            start_str = start_time.strftime('%Y-%m-%dT%H:%MZ')
            end_str = end_time.strftime('%Y-%m-%dT%H:%MZ')
            
            # Make API request
            url = f"{self.base_url}/intensity/{start_str}/{end_str}"
            logger.debug(f"Fetching carbon intensity data from: {url}")
            print(f"Fetching carbon intensity data from: {url}")
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Debug: Print raw response structure
            # print(f"DEBUG: API Response keys: {list(data.keys())}")
            # print(f"DEBUG: Number of data entries: {len(data.get('data', []))}")
            
            # Extract and format data points
            data_points = []
            for i, entry in enumerate(data.get('data', [])):
                # print(f"DEBUG: Entry {i}: {entry}")
                timestamp = entry.get('to')  # Use 'to' timestamp as the main identifier
                intensity = entry.get('intensity', {})
                emissions = intensity.get('actual', intensity.get('forecast'))
                
                if timestamp and emissions is not None:
                    data_points.append({
                        'timestamp': timestamp,
                        'emissions': emissions
                    })
                    # print(f"DEBUG: Added data point: {timestamp} = {emissions}")
                # else:
                #     print(f"DEBUG: Skipped entry {i} - timestamp: {timestamp}, emissions: {emissions}")
            
            logger.info(f"Retrieved {len(data_points)} carbon intensity data points from API")
            print(f"Retrieved {len(data_points)} carbon intensity data points from API")
            # Print the max and min timestamps of the received data
            if data_points:
                print(f"Max timestamp: {max(data_points, key=lambda x: x['timestamp'])}")
                print(f"Min timestamp: {min(data_points, key=lambda x: x['timestamp'])}")
            else:
                print("No data points received from API")
            return data_points
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Carbon intensity API request failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Failed to process carbon intensity API response: {e}")
            return []
    
    def check_health(self) -> bool:
        """
        Check if the Carbon Intensity API is accessible
        
        Returns:
            True if API is healthy, False otherwise
        """
        try:
            # Try to get current intensity (smallest possible request)
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(minutes=30)
            
            start_str = start_time.strftime('%Y-%m-%dT%H:%MZ')
            end_str = end_time.strftime('%Y-%m-%dT%H:%MZ')
            
            url = f"{self.base_url}/intensity/{start_str}/{end_str}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            # Check if response has expected structure
            data = response.json()
            if 'data' in data:
                logger.debug("Carbon Intensity API health check passed")
                return True
            else:
                logger.warning("Carbon Intensity API response missing expected data structure")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Carbon Intensity API health check failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Carbon Intensity API health check error: {e}")
            return False 