
import re
import pytz
import hashlib
from datetime import datetime, timedelta
from rapidfuzz import fuzz
from nltk.sentiment import SentimentIntensityAnalyzer

TORONTO_TZ = pytz.timezone("America/Toronto")

def now_toronto():
    return datetime.now(TORONTO_TZ)

def window_start(hours=16):
    return now_toronto() - timedelta(hours=hours)

def normalize_text(s):
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip().lower()
    return re.sub(r"[^a-z0-9\s\-]", "", s)

def compute_id(title, link):
    base = (title or "") + (link or "")
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def near_duplicate(title_a, title_b, threshold=92):
    a = normalize_text(title_a)
    b = normalize_text(title_b)
    return fuzz.token_sort_ratio(a, b) >= threshold

_sia = None

def sentiment():
    global _sia
    if _sia is None:
        from nltk import download
        download('vader_lexicon', quiet=True)
        _sia = SentimentIntensityAnalyzer()
    return _sia

def label_sentiment(text):
    s = sentiment().polarity_scores(text or "")
    return {"compound": s["compound"]}
