#!/usr/bin/env python3
"""
Pull and clean r/copywriters posts + comments for a qualitative study on
how copywriters talk about generative AI's impact on their work, identity,
skills, and career decisions.

Data source: the Arctic Shift API (https://arctic-shift.photon-reddit.com),
confirmed against api/README.md in https://github.com/ArthurHeitmann/arctic_shift:
    GET /api/posts/search      (subreddit, after, before, limit, sort, ...)
    GET /api/comments/search   (subreddit, after, before, limit, sort, ...)

Note on the bulk Parquet/zst dumps: Arctic Shift's bulk dumps (mirrored on
Academic Torrents, see download_links.md in the repo above) are full-Reddit
archives covering every subreddit at once -- they are not split per
subreddit, and downloading/torrenting hundreds of GB just to filter out one
small subreddit is impractical. The live search API is built from the same
underlying archive plus ongoing collection, so it already serves full
subreddit history (not just a "recent gap"). This script therefore pulls
the entire requested date range through the search API, paginating with
'after'/'before' cursors on created_utc. If you ever need a subreddit far
larger than r/copywriters over a much longer window, switch to processing
the bulk dumps directly with Arctic Shift's processFiles.py instead.

Usage:
    python scrape_clean_copywriters.py --start 2024-08-01 --end 2026-07-31
    python scrape_clean_copywriters.py --selftest   # offline logic check, no network
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL = "https://arctic-shift.photon-reddit.com"
SUBREDDIT = "copywriters"
MIN_WORDS = 20
PAGE_LIMIT = 100
REMOVED_MARKERS = {"[removed]", "[deleted]", ""}

# Known Reddit bot / automated accounts to exclude. Extend as needed.
BOT_AUTHORS = {
    "automoderator",
    "auto-moderator",
    "automod",
    "reddit",
    "sneakpeekbot",
    "remindmebot",
    "converter-bot",
    "imagesgifbot",
    "table_it_bot",
    "gifv-bot",
    "haikubot-1911",
    "tweettranscriberbot",
    "wikitextbot",
}

session = requests.Session()
session.headers.update({"User-Agent": "copywriters-corpus-research/1.0"})


def _get(path, params, max_retries=6):
    """GET with retry/backoff honoring Arctic Shift's rate-limit headers."""
    for attempt in range(max_retries):
        try:
            resp = session.get(f"{BASE_URL}{path}", params=params, timeout=60)
        except requests.exceptions.RequestException as exc:
            wait = min(2 ** attempt, 60)
            print(f"  [retry] network error ({exc}); waiting {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429 or resp.status_code >= 500:
            reset = resp.headers.get("X-RateLimit-Reset")
            wait = float(reset) if reset else min(2 ** attempt, 60)
            print(f"  [retry] status={resp.status_code}; waiting {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
            continue

        resp.raise_for_status()

    raise RuntimeError(f"Failed after {max_retries} retries: {path} {params}")


def fetch_all(kind, after_epoch, before_epoch):
    """kind: 'posts' or 'comments'. Yields raw API objects, paginating asc by created_utc."""
    path = f"/api/{kind}/search"
    cursor = after_epoch
    page_seen_ids = set()

    while True:
        params = {
            "subreddit": SUBREDDIT,
            "after": cursor,
            "before": before_epoch,
            "limit": PAGE_LIMIT,
            "sort": "asc",
        }
        payload = _get(path, params)
        data = payload["data"] if isinstance(payload, dict) else payload
        if not data:
            break

        new_count = 0
        max_created = cursor
        for item in data:
            if item["id"] in page_seen_ids:
                continue
            page_seen_ids.add(item["id"])
            new_count += 1
            max_created = max(max_created, item["created_utc"])
            yield item

        if len(data) < PAGE_LIMIT:
            break
        if new_count == 0 or max_created <= cursor:
            # cursor isn't advancing -- bail out instead of looping forever
            break
        cursor = max_created + 1
        time.sleep(0.25)


def word_count(text):
    return len(text.split()) if text else 0


def is_bot(author):
    return (author or "").strip().lower() in BOT_AUTHORS


def _to_iso(created_utc):
    return datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()


def _full_permalink(item):
    permalink = item.get("permalink")
    if permalink:
        return f"https://www.reddit.com{permalink}" if permalink.startswith("/") else permalink
    # Fallback if the API ever omits permalink for an item.
    return f"https://www.reddit.com/r/{SUBREDDIT}/comments/{item['id']}/"


def clean_post(item):
    selftext = (item.get("selftext") or "").strip()
    if selftext in REMOVED_MARKERS:
        return None
    if is_bot(item.get("author")):
        return None
    if word_count(selftext) < MIN_WORDS:
        return None

    return {
        "id": item["id"],
        "type": "post",
        "permalink": _full_permalink(item),
        "created_utc": _to_iso(item["created_utc"]),
        "subreddit": item.get("subreddit", SUBREDDIT),
        "parent_id": None,
        "link_id": None,
        "title": item.get("title"),
        "text": selftext,
        "score": item.get("score"),
        "num_comments": item.get("num_comments"),
        "link_flair_text": item.get("link_flair_text"),
    }


def clean_comment(item):
    body = (item.get("body") or "").strip()
    if body in REMOVED_MARKERS:
        return None
    if is_bot(item.get("author")):
        return None
    if word_count(body) < MIN_WORDS:
        return None

    return {
        "id": item["id"],
        "type": "comment",
        "permalink": _full_permalink(item),
        "created_utc": _to_iso(item["created_utc"]),
        "subreddit": item.get("subreddit", SUBREDDIT),
        "parent_id": item.get("parent_id"),
        "link_id": item.get("link_id"),
        "title": None,
        "text": body,
        "score": item.get("score"),
        "num_comments": None,
        "link_flair_text": item.get("link_flair_text"),
    }


def print_summary(total_pulled, cleaned_records, out_path):
    by_month = {}
    for rec in cleaned_records:
        ym = rec["created_utc"][:7]
        by_month[ym] = by_month.get(ym, 0) + 1

    print("\n=== SUMMARY ===")
    print(f"Total items pulled (posts + comments, pre-clean): {total_pulled}")
    print(f"Total items after cleaning & dedup:                {len(cleaned_records)}")
    if cleaned_records:
        dates = sorted(r["created_utc"] for r in cleaned_records)
        print(f"Date range covered:                                {dates[0]} to {dates[-1]}")
    print(f"Output file:                                       {out_path}")
    print("\nCount by year-month:")
    for ym in sorted(by_month):
        print(f"  {ym}: {by_month[ym]}")


def run(start, end, out):
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    after_epoch = int(start_dt.timestamp())
    before_epoch = int(end_dt.timestamp()) + 86400  # make end date inclusive

    total_pulled = 0
    seen_ids = set()
    cleaned_records = []

    for kind, cleaner in (("posts", clean_post), ("comments", clean_comment)):
        print(f"Fetching {kind} for r/{SUBREDDIT} from {start} to {end}...")
        count_kind = 0
        for item in fetch_all(kind, after_epoch, before_epoch):
            total_pulled += 1
            count_kind += 1
            if item["id"] in seen_ids:
                continue
            record = cleaner(item)
            if record is None:
                continue
            seen_ids.add(item["id"])
            cleaned_records.append(record)
            if count_kind % 500 == 0:
                print(f"  ...{count_kind} {kind} pulled so far")
        print(f"  total {kind} pulled: {count_kind}")

    out_path = Path(out)
    with out_path.open("w", encoding="utf-8") as f:
        for rec in cleaned_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print_summary(total_pulled, cleaned_records, out_path.resolve())


def run_selftest():
    """Exercise the cleaning/dedup/summary pipeline against synthetic data,
    with no network calls, so the logic can be sanity-checked in isolation."""
    long_text = " ".join(["word"] * 25)

    synthetic_posts = [
        {
            "id": "p1", "author": "real_user_1", "created_utc": 1722556800,  # 2024-08-02
            "subreddit": "copywriters", "title": "AI changed how I pitch clients",
            "selftext": long_text, "score": 12, "num_comments": 4,
            "link_flair_text": "Discussion", "permalink": "/r/copywriters/comments/p1/x/",
        },
        {  # removed -> dropped
            "id": "p2", "author": "real_user_2", "created_utc": 1722643200,
            "subreddit": "copywriters", "title": "gone", "selftext": "[removed]",
            "score": 1, "num_comments": 0, "link_flair_text": None,
            "permalink": "/r/copywriters/comments/p2/x/",
        },
        {  # too short -> dropped
            "id": "p3", "author": "real_user_3", "created_utc": 1722729600,
            "subreddit": "copywriters", "title": "short", "selftext": "too short",
            "score": 0, "num_comments": 0, "link_flair_text": None,
            "permalink": "/r/copywriters/comments/p3/x/",
        },
        {  # bot -> dropped
            "id": "p4", "author": "AutoModerator", "created_utc": 1722816000,
            "subreddit": "copywriters", "title": "Weekly thread", "selftext": long_text,
            "score": 1, "num_comments": 0, "link_flair_text": None,
            "permalink": "/r/copywriters/comments/p4/x/",
        },
        {  # duplicate of p1 -> deduped
            "id": "p1", "author": "real_user_1", "created_utc": 1722556800,
            "subreddit": "copywriters", "title": "AI changed how I pitch clients",
            "selftext": long_text, "score": 12, "num_comments": 4,
            "link_flair_text": "Discussion", "permalink": "/r/copywriters/comments/p1/x/",
        },
    ]
    synthetic_comments = [
        {
            "id": "c1", "author": "real_user_4", "created_utc": 1725148800,  # 2024-09-01
            "subreddit": "copywriters", "body": long_text, "score": 5,
            "link_id": "t3_p1", "parent_id": "t3_p1", "link_flair_text": None,
            "permalink": "/r/copywriters/comments/p1/x/c1/",
        },
        {  # deleted -> dropped
            "id": "c2", "author": "[deleted]", "created_utc": 1725235200,
            "subreddit": "copywriters", "body": "[deleted]", "score": 0,
            "link_id": "t3_p1", "parent_id": "t3_p1", "link_flair_text": None,
            "permalink": "/r/copywriters/comments/p1/x/c2/",
        },
    ]

    total_pulled = len(synthetic_posts) + len(synthetic_comments)
    seen_ids = set()
    cleaned_records = []
    for item in synthetic_posts:
        rec = clean_post(item)
        if rec and item["id"] not in seen_ids:
            seen_ids.add(item["id"])
            cleaned_records.append(rec)
    for item in synthetic_comments:
        rec = clean_comment(item)
        if rec and item["id"] not in seen_ids:
            seen_ids.add(item["id"])
            cleaned_records.append(rec)

    out_path = Path("selftest_output.jsonl")
    with out_path.open("w", encoding="utf-8") as f:
        for rec in cleaned_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("*** SELF-TEST: synthetic data, no network calls, no real Reddit content ***")
    print_summary(total_pulled, cleaned_records, out_path.resolve())
    assert len(cleaned_records) == 2, f"expected 2 surviving records, got {len(cleaned_records)}"
    assert {r["id"] for r in cleaned_records} == {"p1", "c1"}
    assert all("author" not in r for r in cleaned_records)
    print("\nSelf-test assertions passed: removed/deleted, too-short, bot, and duplicate "
          "items were correctly filtered, and no author field is present in the output.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2024-08-01", help="Start date, YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", default="2026-07-31", help="End date, YYYY-MM-DD (inclusive)")
    parser.add_argument("--out", default="copywriters_corpus.jsonl", help="Output JSONL path")
    parser.add_argument("--selftest", action="store_true", help="Run offline logic check, no network calls")
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        return

    run(args.start, args.end, args.out)


if __name__ == "__main__":
    main()
