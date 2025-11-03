#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HW3 Problem 2 — Part D: Minimal HTTP API using http.server + DynamoDB backend.
Allowed deps: boto3 + stdlib (http.server, urllib.parse, json, os, datetime)
"""

import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, unquote

import boto3
from boto3.dynamodb.conditions import Key

# ---- helpers ----
def now_ms():
    # 使用 timezone-aware 的 UTC 时间，避免 DeprecationWarning
    return int(datetime.now(timezone.utc).timestamp() * 1000)

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
TABLE_NAME = os.environ.get("ARXIV_TABLE") or "arxiv-papers"

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

# ---- query helpers (返回值已按作业 D 的格式) ----
def q_recent(category, limit=20):
    resp = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=int(limit),
    )
    items = [{
        "arxiv_id": it.get("arxiv_id"),
        "title": it.get("title"),
        "authors": it.get("authors"),
        "published": it.get("published"),
        "categories": it.get("categories"),
    } for it in resp.get("Items", [])]
    return {"category": category, "papers": items, "count": len(items)}

def q_author(author_name):
    resp = table.query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    items = [{
        "arxiv_id": it.get("arxiv_id"),
        "title": it.get("title"),
        "authors": it.get("authors"),
        "published": it.get("published"),
        "categories": it.get("categories"),
    } for it in resp.get("Items", [])]
    return {"author": author_name, "papers": items, "count": len(items)}

def q_get(arxiv_id):
    resp = table.query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
    )
    items = resp.get("Items", [])
    if not items:
        return None
    # 按要求“返回全文细节”，直接把该条目放入 "paper"
    return {"paper": items[0]}

def q_search(category, start_date, end_date):
    resp = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}') &
                               Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
    )
    items = [{
        "arxiv_id": it.get("arxiv_id"),
        "title": it.get("title"),
        "authors": it.get("authors"),
        "published": it.get("published"),
        "categories": it.get("categories"),
    } for it in resp.get("Items", [])]
    return {
        "category": category,
        "start": start_date,
        "end": end_date,
        "papers": items,
        "count": len(items),
    }

def q_keyword(keyword, limit=20):
    resp = table.query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=int(limit),
    )
    items = [{
        "arxiv_id": it.get("arxiv_id"),
        "title": it.get("title"),
        "authors": it.get("authors"),
        "published": it.get("published"),
        "categories": it.get("categories"),
    } for it in resp.get("Items", [])]
    return {"keyword": keyword, "papers": items, "count": len(items)}

# ---- HTTP handler ----
class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload, pretty=True):
        # 默认美化输出，和作业示例保持一致
        if pretty:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        else:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log(self, msg):
        print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)
            self.log(f"{self.command} {path}?{parsed.query}")

            # recent
            if path == "/papers/recent":
                category = (qs.get("category") or [""])[0]
                if not category:
                    self._send(400, {"error": "category is required"}); return
                limit = int((qs.get("limit") or ["20"])[0])
                self._send(200, q_recent(category, limit)); return

            # author
            if path.startswith("/papers/author/"):
                author = unquote(path[len("/papers/author/"):])
                if not author:
                    self._send(400, {"error": "author_name missing"}); return
                self._send(200, q_author(author)); return

            # keyword
            if path.startswith("/papers/keyword/"):
                kw = unquote(path[len("/papers/keyword/"):])
                if not kw:
                    self._send(400, {"error": "keyword missing"}); return
                limit = int((qs.get("limit") or ["20"])[0])
                self._send(200, q_keyword(kw, limit)); return



            # date range search
            if path == "/papers/search":
                category = (qs.get("category") or [""])[0]
                start_date = (qs.get("start") or [""])[0]
                end_date = (qs.get("end") or [""])[0]
                if not (category and start_date and end_date):
                    self._send(400, {"error": "category,start,end are required"}); return
                self._send(200, q_search(category, start_date, end_date)); return
                        # get by id
            if path.startswith("/papers/") and path.count("/") == 2:
                arxiv_id = unquote(path.split("/", 2)[2])
                data = q_get(arxiv_id)
                if not data:
                    self._send(404, {"error": "not found", "arxiv_id": arxiv_id}); return
                self._send(200, data); return
                
            # not found
            self._send(404, {"error": "route not found"})
        except Exception as e:
            self._send(500, {"error": "server_error", "message": str(e)})

# ---- main ----
def main():
    port = 8080
    import sys
    if len(sys.argv) >= 2:
        try:
            port = int(sys.argv[1])
        except Exception:
            pass
    print(f"Starting server on 0.0.0.0:{port} (region={REGION}, table={TABLE_NAME})")
    httpd = HTTPServer(("0.0.0.0", port), Handler)
    httpd.serve_forever()

if __name__ == "__main__":
    main()
