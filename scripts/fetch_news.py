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
from requests.adapters import HTTPAdapter, Retry

from utils import (
    now_toronto,
    window_start,
    compute_id,
    near_duplicate,
    label_sentiment,
    normalize_text,
)

# -------- Network safety --------
socket.setdefaulttimeout(12)          # last-resort timeout for any blocking call
HTTP_TIMEOUT = 12                     # seconds per HTTP request
UA = {"User-Agent": "MMAH-Monitor/1.0 (+github-actions)"}

def get_session():
    """Create a requests session with retries/backoff for robustness."""
    retries = Retry(
        total=2,                      # quick retries
        backoff_factor=0.75,          # 0.75s, then 1.5s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update(UA)
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

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
        if not dt or isinstance(dt, str):
            raise ValueError("unparsed")
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
    """Keep items published after start_dt (last N hours window)."""
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
    text_norm = normalize_text(f"{rec['title']} {rec['summary']}")

    def includes_any(words): return any(w.lower() in text_norm for w in words)
    def excludes_any(words): return any(w.lower() in text_norm for w in words)

    cats = []
    housing_cfg = kw["categories"]["housing"]
    municipal_cfg = kw["categories"]["municipal"]

    if includes_any(housing_cfg["include"]) and not excludes_any(housing_cfg.get("exclude", [])):
        cats.append("Housing")
    if includes_any(municipal_cfg["include"]) and not excludes_any(municipal_cfg.get("exclude", [])):
        cats.append("Municipal Affairs")

    if not cats:
        return False, []
    return True, list(set(cats))  # dedupe categories

def deduplicate(records: list) -> list:
    """Remove near-duplicate headlines across outlets."""
    deduped = []
    for r in records:
        if not any(near_duplicate(r["title"], e["title"]) for e in deduped):
            deduped.append(r)
    return deduped

def fetch_feed_bytes(session: requests.Session, url: str) -> bytes:
    resp = session.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.content

# -------- Main --------
def main():
    # Widened to last 24 hours per request
    start_dt = window_start(24)
    sources = load_yaml(CONFIG_SOURCES)["outlets"]
    keywords = load_yaml(CONFIG_KEYWORDS)

    collected = []
    session = get_session()

    for outlet in sources:
        primary = outlet.get("rss")
        alt = outlet.get("rss_alt")

        try:
            if not primary:
                print(f"[WARN] {outlet.get('name','(unknown)')} missing 'rss' URL", flush=True)
                continue

            print(f"[INFO] Fetching {outlet['name']} | {primary}", flush=True)
            content = fetch_feed_bytes(session, primary)

        except Exception as e1:
            if alt:
                try:
                    print(f"[WARN] Primary failed, trying alt | {alt}", flush=True)
                    content = fetch_feed_bytes(session, alt)
                except Exception as e2:
                    print(f"[WARN] Failed {outlet['name']}: {e1} | alt failed: {e2}", flush=True)
                    continue
            else:
                print(f"[WARN] Failed {outlet['name']}: {e1}", flush=True)
                continue

        feed = feedparser.parse(content)
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

    collected = deduplicate(collected)

    # Save for build step
    date_str = now_toronto().strftime("%Y-%m-%d")
    out_path = DATA_DIR / f"news_{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Saved {len(collected)} news items -> {out_path}", flush=True)

if __name__ == "__main__":
    main()
