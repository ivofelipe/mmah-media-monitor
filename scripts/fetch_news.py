
import feedparser
import yaml
from dateutil import parser as dateparser
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
from utils import now_toronto, window_start, compute_id, near_duplicate, label_sentiment, normalize_text

import socket, requests
socket.setdefaulttimeout(12)         # safety net for any library call
HTTP_TIMEOUT = 12                    # seconds
UA = {"User-Agent": "MMAH-Monitor/1.0 (+github-actions)"}

CONFIG_SOURCES = Path("config/sources.yml")
CONFIG_KEYWORDS = Path("config/keywords.yml")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

def load_yaml(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def clean_html(html):
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(" ").strip()

def article_to_record(outlet, entry):
    title = entry.get("title", "").strip()
    link = entry.get("link")
    summary = clean_html(entry.get("summary") or entry.get("description") or "")
    published = entry.get("published") or entry.get("updated") or ""
    try:
        dt = dateparser.parse(published)
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

def in_time_window(rec, start_dt):
    try:
        pub = datetime.fromisoformat(rec["published"])
    except Exception:
        return False
    return pub >= start_dt

def matches_keywords(rec, kw):
    text = f"{rec['title']} {rec['summary']}"
    text_norm = normalize_text(text)
    def includes_any(words):
        return any(w.lower() in text_norm for w in words)
    def excludes_any(words):
        return any(w.lower() in text_norm for w in words)

    cats = []
    if includes_any(kw["categories"]["housing"]["include"]) and not excludes_any(kw["categories"]["housing"].get("exclude", [])):
        cats.append("Housing")
    if includes_any(kw["categories"]["municipal"]["include"]) and not excludes_any(kw["categories"]["municipal"].get("exclude", [])):
        cats.append("Municipal Affairs")
    if not cats:
        return False, []
    return True, list(set(cats))

def deduplicate(records):
    deduped = []
    for r in records:
        if not any(near_duplicate(r["title"], e["title"]) for e in deduped):
            deduped.append(r)
    return deduped

def main():
    start_dt = window_start(16)
    sources = load_yaml(CONFIG_SOURCES)["outlets"]
    keywords = load_yaml(CONFIG_KEYWORDS)

    collected = []
    for outlet in sources:
        try:

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
        except Exception as e:
            print(f"[WARN] Failed {outlet['name']}: {e}")

    collected = deduplicate(collected)

    # Save for build step
    date_str = now_toronto().strftime("%Y-%m-%d")
    out_path = DATA_DIR / f"news_{date_str}.json"
    import json
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Saved {len(collected)} news items -> {out_path}")

if __name__ == "__main__":
    main()
