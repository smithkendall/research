#!/usr/bin/env python3
"""
Build a research corpus of r/copywriters posts and comments via the Arctic
Shift API, for a qualitative study of how copywriters discuss generative AI's
impact on their work/identity/skills/career decisions. (IRB/ethics approved.)

Data source
-----------
Arctic Shift (https://github.com/ArthurHeitmann/arctic_shift) exposes
/api/posts/search and /api/comments/search, paginated over `created_utc`.
This is *not* a "recent only" API: it is generated from the same archived
Reddit dataset as Arctic Shift's monthly bulk .zst dumps (kept in sync
within ~36 hours), so a single paginated pull against the search endpoints
covers the entire requested date range, old and new alike.

We deliberately do NOT try to download and locally filter the bulk .zst
dumps: those are monthly, Reddit-wide (all subreddits combined), multi-
terabyte files distributed only via Academic Torrents, with no per-subreddit
split. For a single small/medium subreddit like r/copywriters, the search
API is the documented, practical way to get the same data (the project's
own hosted "download tool" for single-subreddit extraction is a thin
wrapper around this same backend). If you already have a local .zst/.jsonl
dump file you want folded in instead, that would be a separate ingestion
path -- not implemented here since none was provided.

Confirmed endpoint contract (checked against api/README.md in the repo,
2026-08-19):
    GET /api/posts/search    ?subreddit=&after=&before=&limit=&sort=&fields=
    GET /api/comments/search ?subreddit=&after=&before=&limit=&sort=&fields=
  - after/before: unix timestamps (post/comment creation time)
  - limit: 1-100 (or "auto"); we use 100
  - sort: asc|desc
  - fields: comma-separated field allow-list

Usage
-----
    python3 scrape_clean_copywriters.py --start 2024-08-01 --end 2026-07-31 \
        --out copywriters_corpus.jsonl

Requires only the Python standard library.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

API_BASE = "https://arctic-shift.photon-reddit.com/api"
SUBREDDIT = "copywriters"
PAGE_LIMIT = 100
REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
SLEEP_BETWEEN_REQUESTS = 0.5
USER_AGENT = "copywriters-research-corpus/1.0 (academic research, IRB-approved)"

MIN_WORDS = 20
REMOVED_MARKERS = {"[removed]", "[deleted]", ""}
BOT_AUTHORS = {"automoderator", "automod", "reddit"}

POST_FIELDS = "id,permalink,created_utc,subreddit,title,selftext,score,num_comments,link_flair_text,author"
COMMENT_FIELDS = "id,permalink,created_utc,subreddit,body,score,link_id,parent_id,link_flair_text,author"


def iso_date(ts) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()


def word_count(text: str) -> int:
    return len((text or "").split())


def is_bot(author: str | None) -> bool:
    return (author or "").strip().lower() in BOT_AUTHORS


def is_removed_marker(text: str) -> bool:
    return (text or "").strip().lower() in REMOVED_MARKERS


def full_permalink(permalink: str | None) -> str | None:
    if not permalink:
        return None
    if permalink.startswith("http"):
        return permalink
    return f"https://www.reddit.com{permalink}"


def fetch_page(endpoint: str, params: dict) -> list:
    url = f"{API_BASE}/{endpoint}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
                return payload.get("data", payload) if isinstance(payload, dict) else payload
        except urllib.error.HTTPError as e:
            if e.code == 429:
                reset = e.headers.get("X-RateLimit-Reset")
                wait = float(reset) if reset else min(2**attempt, 60)
                print(f"  rate limited; waiting {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            if e.code >= 500:
                wait = min(2**attempt, 60)
                print(f"  server error {e.code}; retrying in {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            wait = min(2**attempt, 60)
            print(f"  request error ({e}); retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {endpoint} after {MAX_RETRIES} retries: {last_err}")


def fetch_all(item_type: str, start_ts: int, end_ts: int) -> list:
    """Paginate /api/{posts,comments}/search ascending by created_utc."""
    endpoint = f"{item_type}/search"
    fields = POST_FIELDS if item_type == "posts" else COMMENT_FIELDS
    results = []
    cursor = start_ts
    while True:
        params = {
            "subreddit": SUBREDDIT,
            "after": cursor,
            "before": end_ts,
            "limit": PAGE_LIMIT,
            "sort": "asc",
            "fields": fields,
        }
        items = fetch_page(endpoint, params)
        if not items:
            break
        results.extend(items)
        max_ts = max((int(i["created_utc"]) for i in items if i.get("created_utc") is not None), default=cursor)
        # Guard against a stalled cursor when >=PAGE_LIMIT items share one created_utc second.
        cursor = max_ts + 1 if max_ts == cursor else max_ts
        print(f"  {item_type}: {len(results)} fetched so far (up to {iso_date(cursor)})", file=sys.stderr)
        if len(items) < PAGE_LIMIT:
            break
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return results


def process_item(item: dict, item_type: str) -> dict | None:
    if is_bot(item.get("author")):
        return None

    if item_type == "post":
        title = item.get("title") or ""
        text = item.get("selftext") or ""
        if is_removed_marker(text) or is_removed_marker(title):
            return None
        if word_count(title) + word_count(text) < MIN_WORDS:
            return None
    else:
        title = None
        text = item.get("body") or ""
        if is_removed_marker(text):
            return None
        if word_count(text) < MIN_WORDS:
            return None

    return {
        "id": item.get("id"),
        "permalink": full_permalink(item.get("permalink")),
        "created_utc": iso_date(item.get("created_utc")),
        "subreddit": item.get("subreddit"),
        "type": item_type,
        "parent_id": item.get("parent_id") if item_type == "comment" else None,
        "link_id": item.get("link_id") if item_type == "comment" else None,
        "title": title,
        "text": text,
        "score": item.get("score"),
        "num_comments": item.get("num_comments") if item_type == "post" else None,
        "link_flair_text": item.get("link_flair_text"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2024-08-01", help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", default="2026-07-31", help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--out", default="copywriters_corpus.jsonl", help="Output JSONL path")
    args = parser.parse_args()

    start_ts = int(datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86400

    print(f"Fetching posts from r/{SUBREDDIT}: {args.start} to {args.end} ...")
    raw_posts = fetch_all("posts", start_ts, end_ts)
    print(f"Fetching comments from r/{SUBREDDIT}: {args.start} to {args.end} ...")
    raw_comments = fetch_all("comments", start_ts, end_ts)

    total_pulled = len(raw_posts) + len(raw_comments)

    records: dict[str, dict] = {}
    for item in raw_posts:
        rec = process_item(item, "post")
        if rec and rec["id"] is not None:
            records[rec["id"]] = rec
    for item in raw_comments:
        rec = process_item(item, "comment")
        if rec and rec["id"] is not None:
            records[rec["id"]] = rec

    final = sorted(records.values(), key=lambda r: r["created_utc"] or "")

    out_path = Path(args.out)
    with out_path.open("w", encoding="utf-8") as f:
        for rec in final:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    by_month = Counter((r["created_utc"] or "unknown")[:7] for r in final)

    print("\n=== SUMMARY ===")
    print(f"Total items pulled (raw, pre-clean):  {total_pulled}")
    print(f"  posts:    {len(raw_posts)}")
    print(f"  comments: {len(raw_comments)}")
    print(f"Total items after cleaning/dedup:     {len(final)}")
    if final:
        print(f"Date range covered: {final[0]['created_utc']} to {final[-1]['created_utc']}")
    print("Counts by year-month:")
    for ym in sorted(by_month):
        print(f"  {ym}: {by_month[ym]}")
    print(f"\nSaved {len(final)} records to {out_path.resolve()}")


if __name__ == "__main__":
    main()
