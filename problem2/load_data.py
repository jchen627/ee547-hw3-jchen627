#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HW3 Problem 2 — Part B: Load ArXiv papers into DynamoDB with denormalization.
Allowed deps: boto3 + stdlib (json, sys, os, datetime, re, collections)
"""

import json
import sys
import os
import re
from datetime import datetime
from collections import Counter

import boto3

STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
    'can', 'this', 'that', 'these', 'those', 'we', 'our', 'use', 'using',
    'based', 'approach', 'method', 'paper', 'propose', 'proposed', 'show'
}

# ------------- helpers ------------- #

def usage():
    print("Usage: python load_data.py <papers_json_path> <table_name> [--region REGION]")
    sys.exit(1)

def parse_args(argv):
    if len(argv) < 3:
        usage()
    papers_path = argv[1]
    table_name = argv[2]
    region = None
    if len(argv) >= 5 and argv[3] == "--region":
        region = argv[4]
    elif len(argv) >= 4 and argv[3].startswith("--region"):
        parts = argv[3].split("=", 1)
        if len(parts) == 2:
            region = parts[1]
    if region is None:
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2"
    return papers_path, table_name, region

def get_clients(region):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    client = boto3.client("dynamodb", region_name=region)
    return dynamodb, client

def list_tables_contains(client, table_name):
    start = None
    while True:
        kwargs = {}
        if start:
            kwargs["ExclusiveStartTableName"] = start
        resp = client.list_tables(**kwargs)
        if table_name in resp.get("TableNames", []):
            return True
        start = resp.get("LastEvaluatedTableName")
        if not start:
            return False

def ensure_table(client, dynamodb, table_name):
    """
    Create table + GSIs if not exists. If exists, verify GSIs and continue.
    Keys:
      - PK (HASH, S)
      - SK (RANGE, S)
    GSIs:
      - AuthorIndex:     GSI1PK (HASH), GSI1SK (RANGE)
      - PaperIdIndex:    GSI2PK (HASH), GSI2SK (RANGE)
      - KeywordIndex:    GSI3PK (HASH), GSI3SK (RANGE)
    Billing: PAY_PER_REQUEST
    """
    if not list_tables_contains(client, table_name):
        print(f"Creating DynamoDB table: {table_name}")
        client.create_table(
            TableName=table_name,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
                {"AttributeName": "GSI2PK", "AttributeType": "S"},
                {"AttributeName": "GSI2SK", "AttributeType": "S"},
                {"AttributeName": "GSI3PK", "AttributeType": "S"},
                {"AttributeName": "GSI3SK", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "AuthorIndex",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "PaperIdIndex",
                    "KeySchema": [
                        {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "KeywordIndex",
                    "KeySchema": [
                        {"AttributeName": "GSI3PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI3SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
        )
        waiter = client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        print("Table ACTIVE.")
    else:
        print(f"Table exists: {table_name}")
        # verify GSIs present; if missing, create them
        desc = client.describe_table(TableName=table_name)
        existing = set()
        for g in (desc["Table"].get("GlobalSecondaryIndexes") or []):
            existing.add(g["IndexName"])
        wanted = {"AuthorIndex", "PaperIdIndex", "KeywordIndex"}
        missing = [x for x in wanted if x not in existing]
        if missing:
            print(f"Adding missing GSIs: {', '.join(missing)}")
            gdefs = []
            for name in missing:
                if name == "AuthorIndex":
                    gdefs.append({
                        "IndexName": "AuthorIndex",
                        "KeySchema": [
                            {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                            {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    })
                elif name == "PaperIdIndex":
                    gdefs.append({
                        "IndexName": "PaperIdIndex",
                        "KeySchema": [
                            {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                            {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    })
                elif name == "KeywordIndex":
                    gdefs.append({
                        "IndexName": "KeywordIndex",
                        "KeySchema": [
                            {"AttributeName": "GSI3PK", "KeyType": "HASH"},
                            {"AttributeName": "GSI3SK", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    })
            client.update_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {"AttributeName": "PK", "AttributeType": "S"},
                    {"AttributeName": "SK", "AttributeType": "S"},
                    {"AttributeName": "GSI1PK", "AttributeType": "S"},
                    {"AttributeName": "GSI1SK", "AttributeType": "S"},
                    {"AttributeName": "GSI2PK", "AttributeType": "S"},
                    {"AttributeName": "GSI2SK", "AttributeType": "S"},
                    {"AttributeName": "GSI3PK", "AttributeType": "S"},
                    {"AttributeName": "GSI3SK", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexUpdates=[{"Create": g} for g in gdefs]
            )
            waiter = client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            print("GSIs ACTIVE.")
    return dynamodb.Table(table_name)

def load_papers_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "papers" in data:
        return data["papers"]
    if isinstance(data, list):
        return data
    raise ValueError("Unsupported JSON structure. Expect a list or { 'papers': [...] }")

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def normalize_paper(p):
    arxiv_id = p.get("arxiv_id") or p.get("id") or p.get("arXivId")
    title = p.get("title") or ""
    authors = p.get("authors")
    if isinstance(authors, str):
        authors = [a.strip() for a in authors.split(",") if a.strip()]
    authors = ensure_list(authors)
    abstract = p.get("abstract") or p.get("summary") or ""
    categories = p.get("categories") or p.get("category") or []
    if isinstance(categories, str):
        categories = [c.strip() for c in categories.split() if c.strip()]
    categories = ensure_list(categories)
    published = p.get("published") or p.get("published_at") or ""
    if "T" in published:
        published_date = published.split("T", 1)[0]
    elif len(published) >= 10:
        published_date = published[:10]
    else:
        published_date = datetime.utcnow().strftime("%Y-%m-%d")
        published = published_date + "T00:00:00Z"
    return {
        "arxiv_id": arxiv_id,
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "categories": categories,
        "published": published,
        "published_date": published_date,
    }

def extract_keywords(abstract, topk=10):
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9\-']+", abstract.lower())
    tokens = [t for t in tokens if t not in STOPWORDS and len(t) >= 2]
    counts = Counter(tokens)
    top = [w for w, _ in counts.most_common(topk)]
    return top

def base_fields(paper):
    return {
        "arxiv_id": paper["arxiv_id"],
        "title": paper["title"],
        "authors": paper["authors"],
        "published": paper["published"],
        "categories": paper["categories"],
    }

def main():
    papers_path, table_name, region = parse_args(sys.argv)
    dynamodb, client = get_clients(region)
    table = ensure_table(client, dynamodb, table_name)

    print(f"Loading papers from {papers_path} ...")
    raw = load_papers_json(papers_path)

    print("Extracting keywords from abstracts...")
    total_papers = 0
    items_to_write = []
    cnt_category = 0
    cnt_author = 0
    cnt_keyword = 0
    cnt_paperid = 0

    for rp in raw:
        p = normalize_paper(rp)
        if not p["arxiv_id"]:
            continue
        total_papers += 1
        keywords = extract_keywords(p["abstract"], topk=10)

        items_to_write.append({
            "PK": f"PAPER#{p['arxiv_id']}",
            "SK": f"DETAILS#{p['published_date']}",
            "GSI2PK": f"PAPER#{p['arxiv_id']}",
            "GSI2SK": p["published_date"],
            "entity_type": "PAPER_ITEM",
            "arxiv_id": p["arxiv_id"],
            "title": p["title"],
            "authors": p["authors"],
            "abstract": p["abstract"],
            "categories": p["categories"],
            "keywords": keywords,
            "published": p["published"],
            "published_date": p["published_date"],
        })
        cnt_paperid += 1

        for cat in p["categories"]:
            items_to_write.append({
                "PK": f"CATEGORY#{cat}",
                "SK": f"{p['published_date']}#{p['arxiv_id']}",
                "entity_type": "CATEGORY_ITEM",
                "arxiv_id": p["arxiv_id"],
                "title": p["title"],
                "authors": p["authors"],
                "abstract": p["abstract"],
                "categories": p["categories"],
                "keywords": keywords,
                "published": p["published"],
                "published_date": p["published_date"],
            })
            cnt_category += 1

        for author in p["authors"]:
            items_to_write.append({
                "PK": f"META#AUTHOR#{author}",
                "SK": f"{p['published_date']}#{p['arxiv_id']}",
                "GSI1PK": f"AUTOR#{author}".replace("AUTOR","AUTHOR"),  # safe guard
                "GSI1SK": f"{p['published_date']}#{p['arxiv_id']}",
                "entity_type": "AUTHOR_ITEM",
                **base_fields(p),
            })
            cnt_author += 1

        for kw in keywords:
            items_to_write.append({
                "PK": f"META#KEYWORD#{kw}",
                "SK": f"{p['published_date']}#{p['arxiv_id']}",
                "GSI3PK": f"KEYWORD#{kw}",
                "GSI3SK": f"{p['published_date']}#{p['arxiv_id']}",
                "entity_type": "KEYWORD_ITEM",
                **base_fields(p),
            })
            cnt_keyword += 1

    print("Writing items to DynamoDB (batch)...")
    total_items = 0
    with table.batch_writer(overwrite_by_pkeys=['PK', 'SK']) as batch:
        for it in items_to_write:
            batch.put_item(Item=it)
            total_items += 1

    denorm_factor = (total_items / total_papers) if total_papers else 0.0
    print(f"Loaded {total_papers} papers")
    print(f"Created {total_items} DynamoDB items (denormalized)")
    print(f"Denormalization factor: {denorm_factor:.1f}x\\n")
    print("Storage breakdown:")
    avg = lambda c: (c / total_papers) if total_papers else 0.0
    print(f"  - Category items: {cnt_category} ({avg(cnt_category):.1f} per paper avg)")
    print(f"  - Author items:   {cnt_author} ({avg(cnt_author):.1f} per paper avg)")
    print(f"  - Keyword items:  {cnt_keyword} ({avg(cnt_keyword):.1f} per paper avg)")
    print(f"  - Paper ID items: {cnt_paperid} ({avg(cnt_paperid):.1f} per paper)")


if __name__ == "__main__":
    main()
