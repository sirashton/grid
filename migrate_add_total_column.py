import sqlite3

DB_PATH = '/data/grid.db'

SOURCE_COLUMNS = [
    'biomass', 'fossil_gas', 'fossil_hard_coal', 'fossil_oil',
    'hydro_pumped_storage', 'hydro_run_of_river', 'nuclear',
    'other', 'solar', 'wind_offshore', 'wind_onshore'
]

def migrate_add_total_column():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print('--- Adding total column to generation_30min_data ---', flush=True)
    # 1. Add the total column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE generation_30min_data ADD COLUMN total REAL")
        print('Added total column.', flush=True)
    except sqlite3.OperationalError as e:
        if 'duplicate column name' in str(e):
            print('total column already exists.', flush=True)
        else:
            print(f'Error adding total column: {e}', flush=True)
            conn.close()
            return

    # 2. Backfill the total column for all rows
    sum_expr = ' + '.join([f'COALESCE({col}, 0)' for col in SOURCE_COLUMNS])
    update_sql = f"UPDATE generation_30min_data SET total = {sum_expr}"
    print(f'Backfilling total column with: {update_sql}', flush=True)
    cursor.execute(update_sql)
    print('Backfill complete.', flush=True)

    # 3. Print a summary
    cursor.execute('SELECT COUNT(*), SUM(total) FROM generation_30min_data')
    count, total_sum = cursor.fetchone()
    print(f'Total rows: {count}, Sum of total: {total_sum}', flush=True)

    conn.commit()
    conn.close()
    print('--- Migration complete ---', flush=True)

if __name__ == '__main__':
    migrate_add_total_column() 