#!/usr/bin/env python3
"""
Database utility functions for the grid tracker
"""

import sqlite3
import os
import sys
from pathlib import Path

def clear_table_data(table_name: str, db_path: str = '/data/grid.db'):
    """Clear all data from a specific table"""
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        print("Make sure you're running this from the correct directory")
        return False
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            if not cursor.fetchone():
                print(f"❌ Table '{table_name}' does not exist")
                return False
            
            # Get current record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            current_count = cursor.fetchone()[0]
            
            print(f"Found {current_count} records in {table_name} table")
            
            if current_count == 0:
                print(f"✅ Table '{table_name}' is already empty")
                return True
            
            # Confirm deletion
            print(f"\n⚠️  WARNING: This will delete ALL {current_count} records from '{table_name}'")
            print("This action cannot be undone!")
            
            # For safety, require explicit confirmation
            confirm = input(f"\nType 'DELETE {table_name}' to confirm: ")
            
            if confirm != f'DELETE {table_name}':
                print("❌ Deletion cancelled")
                return False
            
            # Delete all records
            cursor.execute(f"DELETE FROM {table_name}")
            deleted_count = cursor.rowcount
            
            # Commit the changes
            conn.commit()
            
            print(f"✅ Successfully deleted {deleted_count} records from '{table_name}'")
            
            # Verify table is empty
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            remaining_count = cursor.fetchone()[0]
            
            if remaining_count == 0:
                print(f"✅ Verification: Table '{table_name}' is now empty")
                return True
            else:
                print(f"❌ Warning: {remaining_count} records still remain in table '{table_name}'")
                return False
                
    except Exception as e:
        print(f"❌ Error clearing table '{table_name}': {e}")
        return False

def list_available_tables(db_path: str = '/data/grid.db'):
    """List all available tables in the database"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            if tables:
                print("Available tables:")
                for table in tables:
                    # Get record count for each table
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  - {table} ({count} records)")
            else:
                print("No tables found in database")
                
    except Exception as e:
        print(f"❌ Error listing tables: {e}")

def get_table_stats(table_name: str, db_path: str = '/data/grid.db'):
    """Get statistics for a specific table"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Check if table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            
            if not cursor.fetchone():
                return {'error': f"Table '{table_name}' does not exist"}
            
            # Get record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            
            # Get date range if table has timestamp column
            try:
                cursor.execute(f"""
                    SELECT MIN(timestamp), MAX(timestamp)
                    FROM {table_name}
                """)
                result = cursor.fetchone()
                
                if result and result[0] and result[1]:
                    return {
                        'table_name': table_name,
                        'record_count': count,
                        'earliest_timestamp': result[0],
                        'latest_timestamp': result[1],
                        'has_data': count > 0
                    }
                else:
                    return {
                        'table_name': table_name,
                        'record_count': count,
                        'has_data': count > 0
                    }
            except sqlite3.OperationalError:
                # Table doesn't have timestamp column
                return {
                    'table_name': table_name,
                    'record_count': count,
                    'has_data': count > 0
                }
                
    except Exception as e:
        return {'error': str(e)} 