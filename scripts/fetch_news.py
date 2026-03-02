# scripts/fetch_news.py
# Collects Ontario media items from the last N hours, filters by keywords,
# de-duplicates, adds simple sentiment, and writes data/news_YYYY-MM-DD.json.

import json
import socket
from datetime import datetime
from pathlib import Path

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from utils import (
    now_toronto,
    window_start,
    compute_id,
    near_duplicate,
    label_sentiment,
    normalize_text,
)

# -------- Network safety --------
# Default socket timeout as a last-resort safety net
socket.setdefaulttimeout(12)
HTTP_TIMEOUT = 12  # seconds per HTTP request
UA = {"User-Agent": "MMAH-Monitor/1.0 (+github-actions)"}

# -------- Paths --------
CONFIG_SOURCES = Path("config/sources.yml")
CONFIG_KEYWORDS = Path("config/keywords.yml")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


# -------- Helpers --------
def load_yaml(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clean_html(html: str) -> str:
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(" ").strip()


def article_to_record(outlet: dict, entry) -> dict:
    """
    Convert a feedparser entry to our normalized record.
    """
    title = (entry.get("title") or "").strip()
    link = entry.get("link")
    summary = clean_html(entry.get("summary") or entry.get("description") or "")

    published = entry.get("published") or entry.get("updated") or ""
    try:
        dt = dateparser.parse(published)
        # If feed returns naive time, localize to Toronto
        if not dt.tzinfo:
            from pytz import timezone

            dt = timezone("America/Toronto").localize(dt)
    except Exception:
        dt = now_toronto()

    return {
        "id": compute_id(title, link),
        "outlet": outlet["name"],
        "region": outlet.get("region"),
        "title": title,
        "link": link,
        "summary": summary,
        "published": dt.isoformat(),
        "tags": outlet.get("tags", []),
    }


def in_time_window(rec: dict, start_dt) -> bool:
    """
    Keep items published after start_dt (last N hours window).
    """
    try:
        pub = datetime.fromisoformat(rec["published"])
    except Exception:
        return False
    return pub >= start_dt


def matches_keywords(rec: dict, kw: dict):
    """
    Simple keyword inclusion/exclusion on lowercase normalized text.
    Returns (bool, categories_list).
    """
    text = f"{rec['title']} {rec['summary']}"
    text_norm = normalize_text(text)

    def includes_any(words):
        return any(w.lower() in text_norm for w in words)

    def excludes_any(words):
        return any(w.lower() in text_norm for w in words)

    cats = []
    housing_cfg = kw["categories"]["housing"]
    municipal_cfg = kw["categories"]["municipal"]

    if includes_any(housing_cfg["include"]) and not excludes_any(
        housing_cfg.get("exclude", [])
    ):
        cats.append("Housing")

    if includes_any(municipal_cfg["include"]) and not excludes_any(
        municipal_cfg.get("exclude", [])
    ):
        cats.append("Municipal Affairs")

    if not cats:
        return False, []

    # Deduplicate categories if both matched from overlapping terms
    return True, list(set(cats))


def deduplicate(records: list) -> list:
    """
    Remove near-duplicate headlines across outlets.
    """
    deduped = []
    for r in records:
        if not any(near_duplicate(r["title"], e["title"]) for e in deduped):
            deduped.append(r)
    return deduped


# -------- Main --------
def main():
    start_dt = window_start(16)  # last 16 hours by default
    sources = load_yaml(CONFIG_SOURCES)["outlets"]
    keywords = load_yaml(CONFIG_KEYWORDS)

    collected = []

    for outlet in sources:
        try:
            # Fetch with explicit timeout and UA, then parse
            print(f"[INFO] Fetching {outlet['name']} | {outlet['rss']}", flush=True)
            resp = requests.get(outlet["rss"], timeout=HTTP_TIMEOUT, headers=UA)
            resp.raise_for_status()

            feed = feedparser.parse(resp.content)
            for entry in feed.entries:
                rec = article_to_record(outlet, entry)
                if not in_time_window(rec, start_dt):
                    continue

                ok, cats = matches_keywords(rec, keywords)
                if not ok:
                    continue

                rec["categories"] = cats
                rec["sentiment"] = label_sentiment(f"{rec['title']}. {rec['summary']}")
                collected.append(rec)

        except requests.RequestException as e:
            print(f"[WARN] Failed {outlet['name']}: {e}", flush=True)
        except Exception as e:
            print(f"[WARN] Unexpected error from {outlet['name']}: {e}", flush=True)

    collected = deduplicate(collected)

    # Save for build step
    date_str = now_toronto().strftime("%Y-%m-%d")
    out_path = DATA_DIR / f"news_{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Saved {len(collected)} news items -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
