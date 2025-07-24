import sys

def run_migration(name, func):
    print(f"\n--- Running migration: {name} ---", flush=True)
    try:
        func()
        print(f"--- Migration {name} complete ---\n", flush=True)
    except Exception as e:
        print(f"[ERROR] Migration {name} failed: {e}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    from migrate_add_timestamp_sql import migrate_add_timestamp_sql_column
    from migrate_add_total_column import migrate_add_total_column
    from migrate_deduplicate_and_unique import deduplicate_and_add_unique

    run_migration("Deduplicate and add unique indexes", deduplicate_and_add_unique)
    run_migration("Add timestamp_sql column", migrate_add_timestamp_sql_column)
    run_migration("Add total column", migrate_add_total_column)

    print("All migrations complete.", flush=True) 