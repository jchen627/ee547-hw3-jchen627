#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HW3 Problem 2 — Part C: Query implementations for 5 access patterns.
Allowed deps: boto3 + stdlib (json, sys, os, datetime, time)
"""

import json
import sys
import os
import time
from boto3.dynamodb.conditions import Key
import boto3


# ------------ pretty print + timing ------------ #

def pretty_print(payload):
    """统一用示例要求的漂亮 JSON 输出。"""
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    sys.stdout.flush()


class Timer:
    """计算执行耗时（毫秒）。"""
    def __enter__(self):
        self._t0 = time.perf_counter()
        return self
    def __exit__(self, *exc):
        self.ms = int((time.perf_counter() - self._t0) * 1000)


# ------------ helpers ------------ #

WANTED_KEYS = ("arxiv_id", "title", "authors", "published", "categories")

def clean_item(item):
    """只保留要求的 5 个字段。"""
    return {k: item[k] for k in WANTED_KEYS if k in item}

def getenv_region():
    # 默认 us-east-1（你现在用的区）
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"

def getenv_table(default=None):
    return os.environ.get("ARXIV_TABLE") or default

def get_table(table_name, region):
    return boto3.resource("dynamodb", region_name=region).Table(table_name)


# ------------ queries (raw) ------------ #

def _q_recent_in_category(table, category, limit=20):
    resp = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=int(limit)
    )
    return resp.get('Items', [])

def _q_papers_by_author(table, author_name):
    resp = table.query(
        IndexName='AuthorIndex',
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    return resp.get('Items', [])

def _q_paper_by_id(table, arxiv_id):
    resp = table.query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
    )
    items = resp.get('Items', [])
    return items[0] if items else None

def _q_papers_in_date_range(table, category, start_date, end_date):
    resp = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}') &
                               Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
    )
    return resp.get('Items', [])

def _q_papers_by_keyword(table, keyword, limit=20):
    resp = table.query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=int(limit)
    )
    return resp.get('Items', [])


# ------------ CLI / output wiring ------------ #

def usage_and_exit():
    u = (
        "Usage:\n"
        "  python query_papers.py recent <category> [--limit N] [--table TABLE] [--region REGION]\n"
        "  python query_papers.py author <author_name> [--table TABLE] [--region REGION]\n"
        "  python query_papers.py get <arxiv_id> [--table TABLE] [--region REGION]\n"
        "  python query_papers.py daterange <category> <start_date> <end_date> [--table TABLE] [--region REGION]\n"
        "  python query_papers.py keyword <keyword> [--limit N] [--table TABLE] [--region REGION]\n"
    )
    print(u); sys.exit(1)

def parse_opts(argv, start_idx):
    opts = {}
    i = start_idx
    while i < len(argv):
        a = argv[i]
        if a == "--limit" and i+1 < len(argv):
            opts["limit"] = argv[i+1]; i += 2; continue
        if a == "--table" and i+1 < len(argv):
            opts["table"] = argv[i+1]; i += 2; continue
        if a == "--region" and i+1 < len(argv):
            opts["region"] = argv[i+1]; i += 2; continue
        if a.startswith("--limit="):
            opts["limit"] = a.split("=",1)[1]; i += 1; continue
        if a.startswith("--table="):
            opts["table"] = a.split("=",1)[1]; i += 1; continue
        if a.startswith("--region="):
            opts["region"] = a.split("=",1)[1]; i += 1; continue
        i += 1
    return opts


def main():
    if len(sys.argv) < 2:
        usage_and_exit()

    cmd = sys.argv[1]
    opts = parse_opts(sys.argv, 2)
    region = opts.get("region") or getenv_region()
    table_name = opts.get("table") or getenv_table("arxiv-papers")
    table = get_table(table_name, region)

    # recent
    if cmd == "recent":
        if len(sys.argv) < 3:
            usage_and_exit()
        category = sys.argv[2]
        limit = int(opts.get("limit", 20))
        with Timer() as t:
            raw = _q_recent_in_category(table, category, limit)
            results = [clean_item(it) for it in raw]
        pretty_print({
          "query_type": "recent_in_category",
          "parameters": {"category": category, "limit": limit},
          "results": results,
          "count": len(results),
          "execution_time_ms": t.ms
        })
        return

    # author
    if cmd == "author":
        if len(sys.argv) < 3:
            usage_and_exit()
        author = sys.argv[2]
        with Timer() as t:
            raw = _q_papers_by_author(table, author)
            results = [clean_item(it) for it in raw]
        pretty_print({
          "query_type": "papers_by_author",
          "parameters": {"author": author},
          "results": results,
          "count": len(results),
          "execution_time_ms": t.ms
        })
        return

    # get by id
    if cmd == "get":
        if len(sys.argv) < 3:
            usage_and_exit()
        arxiv_id = sys.argv[2]
        with Timer() as t:
            one = _q_paper_by_id(table, arxiv_id)
            results = [clean_item(one)] if one else []
        pretty_print({
          "query_type": "paper_by_id",
          "parameters": {"arxiv_id": arxiv_id},
          "results": results,
          "count": len(results),
          "execution_time_ms": t.ms
        })
        return

    # date range
    if cmd == "daterange":
        if len(sys.argv) < 5:
            usage_and_exit()
        category, start_date, end_date = sys.argv[2], sys.argv[3], sys.argv[4]
        with Timer() as t:
            raw = _q_papers_in_date_range(table, category, start_date, end_date)
            results = [clean_item(it) for it in raw]
        pretty_print({
          "query_type": "daterange_in_category",
          "parameters": {"category": category, "start_date": start_date, "end_date": end_date},
          "results": results,
          "count": len(results),
          "execution_time_ms": t.ms
        })
        return

    # keyword
    if cmd == "keyword":
        if len(sys.argv) < 3:
            usage_and_exit()
        kw = sys.argv[2]
        limit = int(opts.get("limit", 20))
        with Timer() as t:
            raw = _q_papers_by_keyword(table, kw, limit)
            results = [clean_item(it) for it in raw]
        pretty_print({
          "query_type": "papers_by_keyword",
          "parameters": {"keyword": kw, "limit": limit},
          "results": results,
          "count": len(results),
          "execution_time_ms": t.ms
        })
        return

    usage_and_exit()


if __name__ == "__main__":
    main()
