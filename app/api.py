"""
Grid Tracker API (Sketch)

This file outlines the planned API endpoints and structure for the new, clean aggregation logic.
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import json
import sqlite3
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

API_VERSION = "0.1.3"

print(f"[Grid Tracker API] Starting up. Version: {API_VERSION}")

app = FastAPI(title="Grid Tracker API (Sketch)", version=API_VERSION)

origins = [
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://localhost",
    "http://188.166.155.100",
    "http://188.166.155.100:8080",
    "http://grid.gathered.consulting",
    "https://grid.gathered.consulting"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. Health Check Endpoint ---
# @app.get("/api/health")
# def health_check():
#     """Return API and DB health status."""
#     pass

# --- 2. Aggregated Generation Data Endpoint ---
@app.get("/api/generation/aggregated")
def get_generation_aggregated(
    start_time: str = Query(..., description="Start time (ISO 8601)"),
    end_time: str = Query(..., description="End time (ISO 8601)"),
    granularity_minutes: int = Query(30, description="Granularity in minutes (multiple of 30)"),
    sources: str = Query("solar", description="Comma-separated list of sources (default: solar)"),
    groups: Optional[str] = Query(None, description="JSON string of group definitions (optional)"),
    as_percent: bool = Query(False, description="Return values as percent of total generation (0-100)")
):
    print(f"[Grid Tracker API v{API_VERSION}] /api/generation/aggregated called with start_time={start_time}, end_time={end_time}, granularity_minutes={granularity_minutes}, sources={sources}, groups={groups}, as_percent={as_percent}")
    """
    Minimal implementation: returns binned data for 'solar' and a groups field with the group name and all values as null.
    """
    # Parse parameters
    try:
        start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
        end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid start_time or end_time format. Use ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)")
    source_list = [s.strip() for s in sources.split(",") if s.strip()]
    group_dict = {}
    if groups:
        try:
            group_dict = json.loads(groups)
        except Exception:
            group_dict = {}
    # Only support non-empty sources for now
    if not source_list:
        raise HTTPException(status_code=400, detail="At least one source must be specified.")
    # Calculate bin size in minutes
    bin_size = granularity_minutes
    # Build SQL for robust time binning
    bin_expr = f"CAST((strftime('%s', timestamp_sql) / 60) / {bin_size} AS INTEGER)"
    # Dynamically build SQL columns for requested sources
    agg_cols = []
    for src in source_list:
        if as_percent:
            # Use NULLIF to avoid division by zero
            agg_cols.extend([
                f"AVG((1.0 * {src} / NULLIF(total,0)) * 100) as {src}_avg_percent",
                f"MAX((1.0 * {src} / NULLIF(total,0)) * 100) as {src}_max_percent",
                f"MIN((1.0 * {src} / NULLIF(total,0)) * 100) as {src}_min_percent",
                f"COUNT({src}) as {src}_count"
            ])
        else:
            agg_cols.extend([
                f"AVG({src}) as {src}_avg",
                f"MAX({src}) as {src}_max",
                f"MIN({src}) as {src}_min",
                f"COUNT({src}) as {src}_count"
            ])
    sql = f'''
        WITH binned AS (
            SELECT timestamp_sql, total, {', '.join(source_list)}, {bin_expr} as bin_index
            FROM generation_30min_data
            WHERE timestamp_sql >= ? AND timestamp_sql < ?
        )
        SELECT MIN(timestamp_sql) as time_bin, {', '.join(agg_cols)}
        FROM binned
        GROUP BY bin_index
        ORDER BY time_bin
    '''
    print(f"[Grid Tracker API v{API_VERSION}] SQL query: {sql}")
    print(f"[Grid Tracker API v{API_VERSION}] SQL params: start={start_dt.strftime('%Y-%m-%d %H:%M:%S')}, end={end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    db_path = '/data/grid.db'
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        # Print count of distinct timestamps in the range
        cursor.execute("SELECT COUNT(DISTINCT timestamp_sql) FROM generation_30min_data WHERE timestamp_sql >= ? AND timestamp_sql < ?", (start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')))
        distinct_count = cursor.fetchone()[0]
        print(f"[Grid Tracker API v{API_VERSION}] Distinct timestamps in range: {distinct_count}")
        # Print total count of timestamps in the range
        cursor.execute("SELECT COUNT(timestamp_sql) FROM generation_30min_data WHERE timestamp_sql >= ? AND timestamp_sql < ?", (start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')))
        total_count = cursor.fetchone()[0]
        print(f"[Grid Tracker API v{API_VERSION}] Total timestamps in range: {total_count}")
        cursor.execute(sql, (start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')))
        rows = cursor.fetchall()
        print(f"[Grid Tracker API v{API_VERSION}] Raw SQL rows: {rows[:3]} (showing up to 3)")
    # Build response
    data = []
    for row in rows:
        time_bin = row[0]
        sources_data = {}
        for i, src in enumerate(source_list):
            base = 1 + i * 4
            avg, high, low, count = row[base], row[base+1], row[base+2], row[base+3]
            if as_percent:
                sources_data[src] = {
                    "avg_percent": round(avg, 2) if avg is not None else None,
                    "high_percent": round(high, 2) if high is not None else None,
                    "low_percent": round(low, 2) if low is not None else None,
                    "data_points": count
                }
            else:
                sources_data[src] = {
                    "avg": round(avg, 2) if avg is not None else None,
                    "high": round(high, 2) if high is not None else None,
                    "low": round(low, 2) if low is not None else None,
                    "data_points": count
                }
        groups_data = {}
        for group_name in group_dict.keys():
            groups_data[group_name] = {
                "avg": None,
                "high": None,
                "low": None,
                "data_points": 0
            }
        data.append({
            "timestamp": time_bin,
            "sources": sources_data,
            "groups": groups_data
        })
    return JSONResponse({
        "api_version": API_VERSION,
        "metadata": {
            "start_time": start_time,
            "end_time": end_time,
            "granularity_minutes": granularity_minutes,
            "time_bins": len(data),
            "as_percent": as_percent
        },
        "data": data
    })

# --- 3. Source List Endpoint ---
# @app.get("/api/generation/sources")
# def get_supported_sources():
#     """Return list of available energy sources in the database."""
#     pass

# --- 4. Example/Debug Endpoint (Optional) ---
# @app.get("/api/debug/sample-bin-query")
# def debug_sample_bin_query():
#     """Return a sample aggregation result for a fixed time window (for dev/testing)."""
#     pass
