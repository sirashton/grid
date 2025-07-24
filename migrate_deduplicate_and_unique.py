import sqlite3
from utils.timestamp_utils import normalize_timestamp

DB_PATH = '/data/grid.db' # Changed from './volumes/database/grid.db'

def deduplicate_and_add_unique():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print('--- Deduplicating generation_30min_data ---', flush=True)
    # 1. Find all timestamps and their normalized versions
    cursor.execute('SELECT id, timestamp, created_at FROM generation_30min_data')
    rows = cursor.fetchall()
    norm_map = {}
    for row_id, ts, created_at in rows:
        norm_ts = normalize_timestamp(ts)
        if norm_ts not in norm_map:
            norm_map[norm_ts] = []
        norm_map[norm_ts].append((row_id, ts, created_at))
    # 2. For each group with >1 row, keep the most recent, delete the rest
    deleted = 0
    for norm_ts, group in norm_map.items():
        if len(group) > 1:
            # Sort by created_at (or id if created_at is None)
            group_sorted = sorted(group, key=lambda x: (x[2] or '', x[0]))
            to_keep = group_sorted[-1][0]
            to_delete = [r[0] for r in group_sorted[:-1]]
            cursor.execute(f"DELETE FROM generation_30min_data WHERE id IN ({','.join(['?']*len(to_delete))})", to_delete)
            deleted += len(to_delete)
            print(f'[Normalization] For normalized timestamp {norm_ts}, kept id={to_keep}, deleted ids={to_delete}', flush=True)
    print(f'[Normalization] Deleted {deleted} rows due to timestamp normalization conflicts.', flush=True)
    # 3. Now normalize all timestamps
    cursor.execute('SELECT id, timestamp FROM generation_30min_data')
    rows = cursor.fetchall()
    updated = 0
    for row_id, ts in rows:
        norm_ts = normalize_timestamp(ts)
        if ts != norm_ts:
            cursor.execute('UPDATE generation_30min_data SET timestamp = ? WHERE id = ?', (norm_ts, row_id))
            updated += 1
    print(f'Normalized {updated} timestamp(s) to consistent format.', flush=True)

    # Print initial row count
    cursor.execute('SELECT COUNT(*) FROM generation_30min_data')
    initial_count = cursor.fetchone()[0]
    print(f'Initial row count: {initial_count}', flush=True)

    # 4. Remove duplicates for timestamp (should be none now)
    cursor.execute('''
        SELECT timestamp, COUNT(*) FROM generation_30min_data GROUP BY timestamp HAVING COUNT(*) > 1
    ''')
    timestamp_dupes = cursor.fetchall()
    print(f'Found {len(timestamp_dupes)} duplicate timestamp(s).', flush=True)
    if timestamp_dupes:
        for ts, count in timestamp_dupes:
            cursor.execute('SELECT id FROM generation_30min_data WHERE timestamp = ?', (ts,))
            ids = [r[0] for r in cursor.fetchall()]
            print(f'[Deduplication] Duplicate timestamp: {ts} (count={count}), row ids={ids}', flush=True)
        print(f'Example duplicate timestamp: {timestamp_dupes[0]}', flush=True)
    cursor.execute('''
        DELETE FROM generation_30min_data
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM generation_30min_data
            GROUP BY timestamp
        )
    ''')
    print('Deduplicated on timestamp.', flush=True)

    # 5. Remove duplicates for timestamp_sql
    cursor.execute('''
        SELECT timestamp_sql, COUNT(*) FROM generation_30min_data GROUP BY timestamp_sql HAVING COUNT(*) > 1 AND timestamp_sql IS NOT NULL
    ''')
    timestamp_sql_dupes = cursor.fetchall()
    print(f'Found {len(timestamp_sql_dupes)} duplicate timestamp_sql(s).', flush=True)
    if timestamp_sql_dupes:
        print(f'Example duplicate timestamp_sql: {timestamp_sql_dupes[0]}', flush=True)
    cursor.execute('''
        DELETE FROM generation_30min_data
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM generation_30min_data
            GROUP BY timestamp_sql
        )
    ''')
    print('Deduplicated on timestamp_sql.', flush=True)

    # Print row count after deduplication
    cursor.execute('SELECT COUNT(*) FROM generation_30min_data')
    after_dedupe_count = cursor.fetchone()[0]
    print(f'Row count after deduplication: {after_dedupe_count}', flush=True)

    # 6. Add unique index on timestamp (if not exists)
    try:
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_generation_timestamp_unique ON generation_30min_data(timestamp)')
        print('Unique index added on timestamp.', flush=True)
    except Exception as e:
        print(f'Could not add unique index on timestamp: {e}', flush=True)

    # 7. Add unique index on timestamp_sql (if not exists)
    try:
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_generation_timestamp_sql_unique ON generation_30min_data(timestamp_sql)')
        print('Unique index added on timestamp_sql.', flush=True)
    except Exception as e:
        print(f'Could not add unique index on timestamp_sql: {e}', flush=True)

    # 8. Print summary
    cursor.execute('SELECT COUNT(*) FROM generation_30min_data')
    total = cursor.fetchone()[0]
    print(f'Total rows after deduplication: {total}', flush=True)

    conn.commit()
    conn.close()
    print('--- Migration complete ---', flush=True)

if __name__ == '__main__':
    deduplicate_and_add_unique() 