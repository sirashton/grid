#!/usr/bin/env python3
"""
Generic script to clear all data from any table in the grid database
Use this when you need to reset tables due to schema or data format changes
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from utils.database_utils import clear_table_data, list_available_tables

def main():
    print("Grid Database Table Clearer")
    print("=" * 30)
    print("This script will clear ALL data from any table in the grid database")
    print("Use this when you need to reset due to schema or data format changes")
    print()
    
    # Check if table name was provided as argument
    if len(sys.argv) > 1:
        table_name = sys.argv[1]
        print(f"Target table: {table_name}")
    else:
        # Show available tables and ask user to choose
        print("Available tables in database:")
        list_available_tables()
        print()
        table_name = input("Enter table name to clear (or 'list' to see tables again): ").strip()
        
        if table_name.lower() == 'list':
            list_available_tables()
            print()
            table_name = input("Enter table name to clear: ").strip()
    
    if not table_name:
        print("❌ No table name provided")
        return
    
    success = clear_table_data(table_name)
    
    if success:
        print(f"\n🎉 Table '{table_name}' cleared successfully!")
        print("You can now restart the system to collect fresh data")
    else:
        print(f"\n❌ Failed to clear table '{table_name}'")

if __name__ == "__main__":
    main() 