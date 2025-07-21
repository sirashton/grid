#!/usr/bin/env python3
"""
Test the Elexon BM API call manually
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from elexon_bm_api import ElexonBMAPI
from datetime import datetime, timezone
import requests

def test_api_call():
    """Test the API call manually"""
    print("Testing Elexon BM API Call Manually")
    print("=" * 50)
    
    # Test the specific timestamp that failed
    target_time = datetime(2023, 7, 15, 3, 30, 0, tzinfo=timezone.utc)
    
    print(f"Testing timestamp: {target_time.isoformat()}")
    
    # Test with the exact same parameters as the API client
    url = "https://data.elexon.co.uk/bmrs/api/v1/generation/actual/per-type"
    params = {
        'from': target_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'to': target_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'format': 'json'
    }
    
    print(f"URL: {url}")
    print(f"Params: {params}")
    
    # Make the request
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'GridTracker/1.0',
        'accept': 'application/json'
    })
    
    try:
        response = session.get(url, params=params, timeout=30)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response data: {data}")
            
            # Check if there's data
            if 'data' in data:
                print(f"Number of data entries: {len(data['data'])}")
                if data['data']:
                    print(f"First entry: {data['data'][0]}")
                else:
                    print("No data entries found")
            else:
                print("No 'data' key in response")
        else:
            print(f"Error response: {response.text}")
            
    except Exception as e:
        print(f"Error making request: {e}")
    
    # Also test with a slightly larger time range
    print(f"\nTesting with a 1-hour range:")
    start_time = target_time
    end_time = target_time.replace(hour=target_time.hour + 1)
    
    params_larger = {
        'from': start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'to': end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'format': 'json'
    }
    
    print(f"Params: {params_larger}")
    
    try:
        response = session.get(url, params=params_larger, timeout=30)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data:
                print(f"Number of data entries: {len(data['data'])}")
                if data['data']:
                    print(f"First entry: {data['data'][0]}")
                else:
                    print("No data entries found")
            else:
                print("No 'data' key in response")
        else:
            print(f"Error response: {response.text}")
            
    except Exception as e:
        print(f"Error making request: {e}")

if __name__ == "__main__":
    test_api_call() 