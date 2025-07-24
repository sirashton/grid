#!/usr/bin/env python3
"""
Migration script to add and backfill timestamp_sql columns in both tables.
"""
import sqlite3
from utils.timestamp_utils import iso8601_to_sql_datetime

def migrate_add_timestamp_sql_column():
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
        # Print count of NULL/empty timestamp_sql before backfill
        cursor.execute("SELECT COUNT(*) FROM generation_30min_data WHERE timestamp_sql IS NULL OR timestamp_sql = ''")
        null_before = cursor.fetchone()[0]
        print(f'[Migration] Rows with NULL or empty timestamp_sql before backfill: {null_before}', flush=True)
        # Print only conflicts, with full details of the conflicting row and the original timestamp
        cursor.execute("SELECT id, timestamp FROM generation_30min_data WHERE timestamp_sql IS NULL OR timestamp_sql = ''")
        rows = cursor.fetchall()
        for row in rows:
            row_id, ts = row
            ts_sql = iso8601_to_sql_datetime(ts)
            cursor.execute("SELECT id, timestamp, timestamp_sql FROM generation_30min_data WHERE timestamp_sql = ? AND id != ?", (ts_sql, row_id))
            conflict = cursor.fetchone()
            if conflict:
                print(f'[Migration] CONFLICT: Row id={row_id} (timestamp={ts}) would set timestamp_sql={ts_sql}, but it already exists in row id={conflict[0]}', flush=True)
                print(f'[Migration] Conflicting row details: id={conflict[0]}, timestamp={conflict[1]}, timestamp_sql={conflict[2]}', flush=True)
        # Proceed with backfill as before
        for row in rows:
            row_id, ts = row
            ts_sql = iso8601_to_sql_datetime(ts)
            cursor.execute("UPDATE generation_30min_data SET timestamp_sql = ? WHERE id = ?", (ts_sql, row_id))
        # Print count of NULL/empty timestamp_sql after backfill
        cursor.execute("SELECT COUNT(*) FROM generation_30min_data WHERE timestamp_sql IS NULL OR timestamp_sql = ''")
        null_after = cursor.fetchone()[0]
        print(f'[Migration] Rows with NULL or empty timestamp_sql after backfill: {null_after}', flush=True)
        # Backfill carbon_intensity_30min_data
        cursor.execute("SELECT id, timestamp FROM carbon_intensity_30min_data WHERE timestamp_sql IS NULL OR timestamp_sql = ''")
        rows = cursor.fetchall()
        for row in rows:
            row_id, ts = row
            ts_sql = iso8601_to_sql_datetime(ts)
            cursor.execute("UPDATE carbon_intensity_30min_data SET timestamp_sql = ? WHERE id = ?", (ts_sql, row_id))
        conn.commit()
        print('[Migration] timestamp_sql column migration complete.', flush=True)

if __name__ == '__main__':
    migrate_add_timestamp_sql_column()