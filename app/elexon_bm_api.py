#!/usr/bin/env python3
"""
Elexon BM Reports API client for generation data
"""

import requests
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class ElexonBMAPI:
    """Client for the Elexon BM Reports API"""
    
    def __init__(self, base_url: str = "https://data.elexon.co.uk/bmrs/api/v1"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'GridTracker/1.0',
            'accept': 'application/json'
        })
    
    def get_generation_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """
        Get generation data by fuel type for a specific time range
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            List of data points with timestamp and generation by fuel type
        """
        try:
            # Format dates for API (YYYY-MM-DD format)
            start_date = start_time.strftime('%Y-%m-%d')
            end_date = end_time.strftime('%Y-%m-%d')
            
            # Make API request
            url = f"{self.base_url}/generation/actual/per-type"
            params = {
                'from': start_date,
                'to': end_date,
                'format': 'json'
            }
            
            logger.debug(f"Fetching generation data from: {url} with params {params}")
            print(f"Fetching generation data from: {url}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract and format data points
            data_points = []
            for entry in data.get('data', []):
                timestamp = entry.get('startTime')
                settlement_period = entry.get('settlementPeriod')
                generation_data = entry.get('data', [])
                
                if timestamp and generation_data:
                    # Create a data point with all fuel types
                    point = {
                        'timestamp': timestamp,
                        'settlement_period': settlement_period,
                        'biomass': None,
                        'fossil_gas': None,
                        'fossil_hard_coal': None,
                        'fossil_oil': None,
                        'hydro_pumped_storage': None,
                        'hydro_run_of_river': None,
                        'nuclear': None,
                        'other': None,
                        'solar': None,
                        'wind_offshore': None,
                        'wind_onshore': None
                    }
                    
                    # Map PSR types to our database columns
                    for gen in generation_data:
                        psr_type = gen.get('psrType', '')
                        quantity = gen.get('quantity', None)
                        
                        if psr_type == 'Biomass':
                            point['biomass'] = quantity
                        elif psr_type == 'Fossil Gas':
                            point['fossil_gas'] = quantity
                        elif psr_type == 'Fossil Hard coal':
                            point['fossil_hard_coal'] = quantity
                        elif psr_type == 'Fossil Oil':
                            point['fossil_oil'] = quantity
                        elif psr_type == 'Hydro Pumped Storage':
                            point['hydro_pumped_storage'] = quantity
                        elif psr_type == 'Hydro Run-of-river and poundage':
                            point['hydro_run_of_river'] = quantity
                        elif psr_type == 'Nuclear':
                            point['nuclear'] = quantity
                        elif psr_type == 'Other':
                            point['other'] = quantity
                        elif psr_type == 'Solar':
                            point['solar'] = quantity
                        elif psr_type == 'Wind Offshore':
                            point['wind_offshore'] = quantity
                        elif psr_type == 'Wind Onshore':
                            point['wind_onshore'] = quantity
                    
                    data_points.append(point)
            
            logger.info(f"Retrieved {len(data_points)} generation data points from API")
            print(f"Retrieved {len(data_points)} generation data points from API")
            
            if data_points:
                print(f"Max timestamp: {max(data_points, key=lambda x: x['timestamp'])['timestamp']}")
                print(f"Min timestamp: {min(data_points, key=lambda x: x['timestamp'])['timestamp']}")
            else:
                print("No data points received from API")
                
            return data_points
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Elexon BM API request failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Failed to process Elexon BM API response: {e}")
            return []
    
    def check_health(self) -> bool:
        """
        Check if the Elexon BM API is accessible
        
        Returns:
            True if API is healthy, False otherwise
        """
        try:
            # Try to get current generation data (smallest possible request)
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=1)
            
            start_date = start_time.strftime('%Y-%m-%d')
            end_date = end_time.strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/generation/actual/per-type"
            params = {
                'from': start_date,
                'to': end_date,
                'format': 'json'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            # Check if response has expected structure
            data = response.json()
            if 'data' in data and 'metadata' in data:
                logger.debug("Elexon BM API health check passed")
                return True
            else:
                logger.warning("Elexon BM API response missing expected data structure")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Elexon BM API health check failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Elexon BM API health check error: {e}")
            return False 