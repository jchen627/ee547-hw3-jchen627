#!/usr/bin/env python3
# problem1/queries.py
import argparse, json, psycopg2
from psycopg2.extras import RealDictCursor

QUERIES = {
    "Q1": {
        "description": "List all stops on Route 20 in order",
        "sql": """
            SELECT s.stop_name, ls.sequence_number AS sequence, ls.time_offset_minutes AS time_offset
            FROM line_stops ls
            JOIN lines l ON l.line_id = ls.line_id
            JOIN stops s ON s.stop_id = ls.stop_id
            WHERE l.line_name = %s
            ORDER BY ls.sequence_number
        """,
        "params": ["Route 20"]
    },
    "Q2": {
        "description": "Trips during morning rush (7-9 AM)",
        "sql": """
            SELECT t.trip_id, l.line_name, t.scheduled_departure
            FROM trips t
            JOIN lines l ON l.line_id = t.line_id
            WHERE t.scheduled_departure::time >= time '07:00' AND t.scheduled_departure::time < time '09:00'
            ORDER BY t.scheduled_departure
        """,
        "params": []
    },
    "Q3": {
        "description": "Transfer stops (stops on 2+ routes)",
        "sql": """
            SELECT s.stop_name, COUNT(DISTINCT ls.line_id) AS line_count
            FROM line_stops ls
            JOIN stops s ON s.stop_id = ls.stop_id
            GROUP BY s.stop_id, s.stop_name
            HAVING COUNT(DISTINCT ls.line_id) >= 2
            ORDER BY line_count DESC, s.stop_name
        """,
        "params": []
    },
    "Q4": {
        "description": "Complete route for a specific trip in order",
        "sql": """
            SELECT s.stop_name, ls.sequence_number AS sequence, ls.time_offset_minutes AS time_offset
            FROM trips t
            JOIN line_stops ls ON ls.line_id = t.line_id
            JOIN stops s ON s.stop_id = ls.stop_id
            WHERE t.trip_id = %s
            ORDER BY ls.sequence_number
        """,
        "params": ["T0001"]
    },
    "Q5": {
        "description": "Routes serving both 'Wilshire / Veteran' and 'Le Conte / Broxton'",
        "sql": """
            SELECT DISTINCT l.line_name
            FROM lines l
            WHERE EXISTS (
                SELECT 1 FROM line_stops ls
                JOIN stops s ON s.stop_id = ls.stop_id
                WHERE ls.line_id = l.line_id AND s.stop_name = %s
            )
            AND EXISTS (
                SELECT 1 FROM line_stops ls2
                JOIN stops s2 ON s2.stop_id = ls2.stop_id
                WHERE ls2.line_id = l.line_id AND s2.stop_name = %s
            )
            ORDER BY l.line_name
        """,
        "params": ["Wilshire / Veteran", "Le Conte / Broxton"]
    },
    "Q6": {
        "description": "Average ridership (on+off) by line",
        "sql": """
            SELECT l.line_name, ROUND(AVG(se.passengers_on + se.passengers_off)::numeric, 2) AS avg_passengers
            FROM stop_events se
            JOIN trips t ON t.trip_id = se.trip_id
            JOIN lines l ON l.line_id = t.line_id
            GROUP BY l.line_name
            ORDER BY avg_passengers DESC, l.line_name
        """,
        "params": []
    },
    "Q7": {
        "description": "Top 10 busiest stops by total activity (on+off)",
        "sql": """
            SELECT s.stop_name, SUM(se.passengers_on + se.passengers_off) AS total_activity
            FROM stop_events se
            JOIN stops s ON s.stop_id = se.stop_id
            GROUP BY s.stop_id, s.stop_name
            ORDER BY total_activity DESC, s.stop_name
            LIMIT 10
        """,
        "params": []
    },
    "Q8": {
        "description": "Count delays by line (>2 minutes late)",
        "sql": """
            SELECT l.line_name, COUNT(*) AS delay_count
            FROM stop_events se
            JOIN trips t ON t.trip_id = se.trip_id
            JOIN lines l ON l.line_id = t.line_id
            WHERE se.actual > se.scheduled + interval '2 minutes'
            GROUP BY l.line_name
            ORDER BY delay_count DESC, l.line_name
        """,
        "params": []
    },
    "Q9": {
        "description": "Trips with 3+ delayed stops",
        "sql": """
            SELECT t.trip_id, COUNT(*) AS delayed_stop_count
            FROM stop_events se
            JOIN trips t ON t.trip_id = se.trip_id
            WHERE se.actual > se.scheduled + interval '2 minutes'
            GROUP BY t.trip_id
            HAVING COUNT(*) >= 3
            ORDER BY delayed_stop_count DESC, t.trip_id
        """,
        "params": []
    },
    "Q10": {
        "description": "Stops with above-average ridership (boardings only)",
        "sql": """
            WITH totals AS (
                SELECT s.stop_id, s.stop_name, SUM(se.passengers_on) AS total_boardings
                FROM stop_events se
                JOIN stops s ON s.stop_id = se.stop_id
                GROUP BY s.stop_id, s.stop_name
            ),
            avg_all AS (
                SELECT AVG(total_boardings) AS avg_boardings FROM totals
            )
            SELECT t.stop_name, t.total_boardings
            FROM totals t, avg_all a
            WHERE t.total_boardings > a.avg_boardings
            ORDER BY t.total_boardings DESC, t.stop_name
        """,
        "params": []
    },
}

def run_query(conn, key, fmt):
    meta = QUERIES[key]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(meta["sql"], meta["params"])
        rows = cur.fetchall()
    if fmt == "json":
        out = {
            "query": key,
            "description": meta["description"],
            "results": rows,
            "count": len(rows)
        }
        print(json.dumps(out, default=str, ensure_ascii=False, indent=2))
    else:
        # simple text output
        print(f"{key} - {meta['description']}")
        for r in rows:
            print(dict(r))
        print(f"({len(rows)} rows)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", default=5432, type=int)
    ap.add_argument("--dbname", required=True)
    ap.add_argument("--user", default="transit")
    ap.add_argument("--password", default="transit123")
    ap.add_argument("--query", choices=list(QUERIES.keys()))
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--format", choices=["text", "json"], default="text")
    args = ap.parse_args()

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.dbname,
        user=args.user, password=args.password
    )

    try:
        if args.all:
            for k in [f"Q{i}" for i in range(1, 11)]:
                run_query(conn, k, args.format)
                if args.format == "text":
                    print("-" * 40)
        else:
            run_query(conn, args.query, args.format)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
