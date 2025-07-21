#!/usr/bin/env python3
"""
Migration script to add and backfill timestamp_sql columns in both tables.
"""
from database import migrate_add_timestamp_sql_column

if __name__ == "__main__":
    migrate_add_timestamp_sql_column() 