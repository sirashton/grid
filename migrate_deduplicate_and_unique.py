import sqlite3

DB_PATH = '/data/grid.db'

def deduplicate_and_add_unique():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print('--- Deduplicating generation_30min_data ---', flush=True)
    # 1. Remove duplicates for timestamp
    cursor.execute('''
        DELETE FROM generation_30min_data
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM generation_30min_data
            GROUP BY timestamp
        )
    ''')
    print('Deduplicated on timestamp.', flush=True)

    # 2. Remove duplicates for timestamp_sql
    cursor.execute('''
        DELETE FROM generation_30min_data
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM generation_30min_data
            GROUP BY timestamp_sql
        )
    ''')
    print('Deduplicated on timestamp_sql.', flush=True)

    # 3. Add unique index on timestamp (if not exists)
    try:
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_generation_timestamp_unique ON generation_30min_data(timestamp)')
        print('Unique index added on timestamp.', flush=True)
    except Exception as e:
        print(f'Could not add unique index on timestamp: {e}', flush=True)

    # 4. Add unique index on timestamp_sql (if not exists)
    try:
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_generation_timestamp_sql_unique ON generation_30min_data(timestamp_sql)')
        print('Unique index added on timestamp_sql.', flush=True)
    except Exception as e:
        print(f'Could not add unique index on timestamp_sql: {e}', flush=True)

    # 5. Print summary
    cursor.execute('SELECT COUNT(*) FROM generation_30min_data')
    total = cursor.fetchone()[0]
    print(f'Total rows after deduplication: {total}', flush=True)

    conn.commit()
    conn.close()
    print('--- Migration complete ---', flush=True)

if __name__ == '__main__':
    deduplicate_and_add_unique() 