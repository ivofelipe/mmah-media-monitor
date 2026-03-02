# scripts/fetch_news.py
# Collects Ontario media items from the last N hours, filters by keywords,
# de-duplicates, adds simple sentiment, and writes data/news_YYYY-MM-DD.json.
#
# Performance notes:
# - Parallel fetching (ThreadPoolExecutor) with per-host timeout/retry profiles
# - CBC endpoints get a more tolerant profile (slower in practice), others are faster
# - Mirror fallback via r.jina.ai is kept as the last resort

import json
import socket
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

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

# ---------------- Global settings ----------------
# Backstop timeout for any blocking call
socket.setdefaulttimeout(24)

# Parallelism (tune if you want)
MAX_WORKERS = 12

# User-Agent
UA = {"User-Agent": "MMAH-Monitor/1.2 (+github-actions)"}

# Default (non-CBC) profile
DEFAULT_CONNECT_TO = 4
DEFAULT_READ_TO = 12
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF = 0.9

# CBC profile (a bit more tolerant)
CBC_CONNECT_TO = 6
CBC_READ_TO = 20
CBC_RETRIES = 4
CBC_BACKOFF = 1.25

# Paths
CONFIG_SOURCES = Path("config/sources.yml")
CONFIG_KEYWORDS = Path("config/keywords.yml")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


# ---------------- Helpers ----------------
def load_yaml(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clean_html(html: str) -> str:
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(" ").strip()


def article_to_record(outlet: dict, entry) -> dict:
    """Convert a feedparser entry to our normalized record."""
    title = (entry.get("title") or "").strip()
    link = entry.get("link")
    summary = clean_html(entry.get("summary") or entry.get("description") or "")
    published = entry.get("published") or entry.get("updated") or ""

    try:
        dt = dateparser.parse(published)
        if not dt or isinstance(dt, str):
            raise ValueError("unparsed")
        if not getattr(dt, "tzinfo", None):
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
    try:
        pub = datetime.fromisoformat(rec["published"])
    except Exception:
        return False
    return pub >= start_dt


def matches_keywords(rec: dict, kw: dict):
    """
    Flexible keyword inclusion/exclusion across multiple categories.
    Returns (bool, matched_categories_list).
    """
    categories = kw.get("categories", {})
    if not categories:
        return False, []

    text_norm = normalize_text(f"{rec['title']} {rec['summary']}")
    matched = []
    for cat_name, cat_cfg in categories.items():
        inc = [w.lower() for w in cat_cfg.get("include", [])]
        exc = [w.lower() for w in cat_cfg.get("exclude", [])]
        if inc and any(w in text_norm for w in inc):
            if not any(w in text_norm for w in exc):
                matched.append(cat_name)

    if not matched:
        return False, []
    return True, sorted(set(matched))


def deduplicate(records: list) -> list:
    """Remove near-duplicate headlines across outlets."""
    deduped = []
    for r in records:
        if not any(near_duplicate(r["title"], e["title"]) for e in deduped):
            deduped.append(r)
    return deduped


# ---------------- Networking ----------------
def _is_cbc(url: str | None) -> bool:
    if not url:
        return False
    host = urlparse(url).hostname or ""
    return "cbc.ca" in host


def _make_session(connect_to: int, read_to: int, retries: int, backoff: float) -> requests.Session:
    """Create a tuned requests session with retries/backoff."""
    r = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update(UA)
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    s.request_timeout = (connect_to, read_to)
    return s


def _fetch_once(session: requests.Session, url: str) -> bytes:
    resp = session.get(url, timeout=getattr(session, "request_timeout", (DEFAULT_CONNECT_TO, DEFAULT_READ_TO)))
    resp.raise_for_status()
    return resp.content


def _fetch_with_fallbacks(outlet: dict) -> bytes:
    """
    Try primary -> alt -> mirror (last resort) with tuned per-host timeouts.
    """
    primary = outlet.get("rss")
    alt = outlet.get("rss_alt")
    is_cbc = _is_cbc(primary) or _is_cbc(alt)

    if is_cbc:
        session = _make_session(CBC_CONNECT_TO, CBC_READ_TO, CBC_RETRIES, CBC_BACKOFF)
    else:
        session = _make_session(DEFAULT_CONNECT_TO, DEFAULT_READ_TO, DEFAULT_RETRIES, DEFAULT_BACKOFF)

    if not primary:
        raise RuntimeError("missing 'rss' URL")

    try:
        return _fetch_once(session, primary)
    except Exception as e1:
        if alt:
            try:
                return _fetch_once(session, alt)
            except Exception as e2:
                # LAST RESORT: mirror via r.jina.ai/http://<original>
                try:
                    mirror = None
                    if primary.startswith("http"):
                        mirror = f"https://r.jina.ai/{primary.replace('https://', 'http://')}"
                    if mirror:
                        return _fetch_once(session, mirror)
                    raise e2
                except Exception as e3:
                    raise RuntimeError(f"Failed {outlet.get('name','(unknown)')}: {e1} | alt failed: {e2} | mirror failed: {e3}") from e3
        else:
            raise RuntimeError(f"Failed {outlet.get('name','(unknown)')}: {e1}") from e1


# ---------------- Per-outlet task ----------------
def _process_outlet(outlet: dict, start_dt, keywords: dict) -> list[dict]:
    """
    Fetch, parse and filter a single outlet; return list of qualified records.
    """
    name = outlet.get("name", "(unknown)")
    print(f"[INFO] Fetching {name}", flush=True)
    try:
        raw = _fetch_with_fallbacks(outlet)
    except Exception as err:
        print(f"[WARN] {name}: {err}", flush=True)
        return []

    feed = feedparser.parse(raw)
    out = []
    for entry in feed.entries:
        rec = article_to_record(outlet, entry)
        if not in_time_window(rec, start_dt):
            continue
        ok, cats = matches_keywords(rec, keywords)
        if not ok:
            continue

        # Backward-compatible sentiment structure for build script
        s_label = label_sentiment(f"{rec['title']}. {rec['summary']}")
        s_compound = {"positive": 0.6, "neutral": 0.0, "negative": -0.6}.get(s_label, 0.0)
        rec["sentiment"] = {"label": s_label, "compound": s_compound}

        rec["categories"] = cats
        out.append(rec)

    print(f"[INFO] {name}: kept {len(out)} item(s)", flush=True)
    return out


# ---------------- Main ----------------
def main():
    # 24h window (default comes from utils)
    start_dt = window_start()

    sources_root = load_yaml(CONFIG_SOURCES)
    sources = sources_root.get("outlets", []) if isinstance(sources_root, dict) else sources_root
    keywords = load_yaml(CONFIG_KEYWORDS) or {}

    collected = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_process_outlet, outlet, start_dt, keywords): outlet for outlet in sources}
        for fut in as_completed(futures):
            try:
                collected.extend(fut.result())
            except Exception as err:
                outlet = futures[fut]
                print(f"[WARN] Unhandled error in {outlet.get('name','(unknown)')}: {err}", flush=True)

    collected = deduplicate(collected)

    # Save for build step
    date_str = now_toronto().strftime("%Y-%m-%d")
    out_path = DATA_DIR / f"news_{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Saved {len(collected)} news items -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
