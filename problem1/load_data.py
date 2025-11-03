#!/usr/bin/env python3
# problem1/load_data.py
import argparse, os, sys, csv, json
import psycopg2
from psycopg2.extras import execute_values

def resolve_path(datadir, base_name):
    """
    Find a data file in datadir that matches one of:
    base_name, base_name.csv, base_name.CSV
    Returns absolute path.
    """
    candidates = [base_name, base_name + ".csv", base_name + ".CSV"]
    for c in candidates:
        p = os.path.join(datadir, c)
        if os.path.isfile(p):
            return p
    raise FileNotFoundError(f"Could not find {base_name}(.csv) under {datadir}")


def connect(args):
    conn = psycopg2.connect(
        host=args.host,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        port=args.port,
    )
    conn.autocommit = False
    return conn

def run_schema(conn, schema_path):
    with conn.cursor() as cur, open(schema_path, "r", encoding="utf-8") as f:
        cur.execute(f.read())
    conn.commit()

def load_lines(conn, path):
    path = resolve_path(os.path.dirname(path), os.path.basename(path))
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((r['line_name'].strip(), r['vehicle_type'].strip()))
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO lines (line_name, vehicle_type) VALUES %s ON CONFLICT (line_name) DO NOTHING",
            rows
        )
    conn.commit()
    return len(rows)

def map_ids(conn, table, key_col, id_col):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {key_col}, {id_col} FROM {table}")
        return dict(cur.fetchall())

def load_stops(conn, path):
    path = resolve_path(os.path.dirname(path), os.path.basename(path))
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((r['stop_name'].strip(), float(r['latitude']), float(r['longitude'])))
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO stops (stop_name, latitude, longitude) VALUES %s ON CONFLICT (stop_name) DO NOTHING",
            rows
        )
    conn.commit()
    return len(rows)

def load_line_stops(conn, path):
    path = resolve_path(os.path.dirname(path), os.path.basename(path))
    # map for FK resolution
    lmap = map_ids(conn, "lines", "line_name", "line_id")
    smap = map_ids(conn, "stops", "stop_name", "stop_id")
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            ln = r['line_name'].strip()
            sn = r['stop_name'].strip()
            seq = int(r['sequence'])
            offset = int(r['time_offset'])
            rows.append((lmap[ln], smap[sn], seq, offset))
    with conn.cursor() as cur:
        execute_values(cur,
            """INSERT INTO line_stops (line_id, stop_id, sequence_number, time_offset_minutes)
               VALUES %s ON CONFLICT (line_id, sequence_number) DO NOTHING""",
            rows
        )
    conn.commit()
    return len(rows)

def load_trips(conn, path):
    path = resolve_path(os.path.dirname(path), os.path.basename(path))
    lmap = map_ids(conn, "lines", "line_name", "line_id")
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            trip_id = r['trip_id'].strip()
            line_id = lmap[r['line_name'].strip()]
            sched = r['scheduled_departure'].strip()
            vehicle = r['vehicle_id'].strip()
            rows.append((trip_id, line_id, sched, vehicle))
    with conn.cursor() as cur:
        execute_values(cur,
            """INSERT INTO trips (trip_id, line_id, scheduled_departure, vehicle_id)
               VALUES %s ON CONFLICT (trip_id) DO NOTHING""",
            rows
        )
    conn.commit()
    return len(rows)

def load_stop_events(conn, path):
    path = resolve_path(os.path.dirname(path), os.path.basename(path))
    smap = map_ids(conn, "stops", "stop_name", "stop_id")
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            trip_id = r['trip_id'].strip()
            stop_id = smap[r['stop_name'].strip()]
            scheduled = r['scheduled'].strip()
            actual = r['actual'].strip()
            on = int(r['passengers_on'])
            off = int(r['passengers_off'])
            rows.append((trip_id, stop_id, scheduled, actual, on, off))
    with conn.cursor() as cur:
        execute_values(cur,
            """INSERT INTO stop_events (trip_id, stop_id, scheduled, actual, passengers_on, passengers_off)
               VALUES %s ON CONFLICT (trip_id, stop_id) DO NOTHING""",
            rows
        )
    conn.commit()
    return len(rows)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", default=5432, type=int)
    p.add_argument("--dbname", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--datadir", default="data")
    p.add_argument("--schema", default="schema.sql")
    args = p.parse_args()

    print(f"Connected to {args.dbname}@{args.host}")
    conn = connect(args)

    print("Creating schema...")
    run_schema(conn, args.schema)
    print("Tables created: lines, stops, line_stops, trips, stop_events\n")

    total = 0
    def load_and_report(fname, loader):
        nonlocal total
        path = os.path.join(args.datadir, fname)
        n = loader(conn, path)
        total += n
        print(f"Loading {path}... {n} rows")

    load_and_report("lines", load_lines)
    load_and_report("stops", load_stops)
    load_and_report("line_stops", load_line_stops)
    load_and_report("trips", load_trips)
    load_and_report("stop_events", load_stop_events)

    print(f"\nTotal: {total} rows loaded")
    conn.close()

if __name__ == "__main__":
    main()
