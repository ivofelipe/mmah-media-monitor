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
