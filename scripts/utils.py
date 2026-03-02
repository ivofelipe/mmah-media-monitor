# scripts/utils.py
# Shared utility functions for the MMAH media monitor.
# - Time helpers (Toronto-local "now" and rolling window start)
# - Text normalization
# - Headline de-duplication
# - Simple sentiment labelling (lightweight heuristic)
# - Stable record ID generation

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

import pytz
from rapidfuzz import fuzz


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

# Default collection window across the project (hours)
DEFAULT_WINDOW_HOURS = 24


def now_toronto() -> datetime:
    """
    Return the current timezone-aware datetime in America/Toronto.
    """
    tz = pytz.timezone("America/Toronto")
    return datetime.now(tz)


def window_start(hours: int = DEFAULT_WINDOW_HOURS) -> datetime:
    """
    Return the Toronto-local timestamp 'hours' ago.
    Used to bound the item collection window.
    """
    return now_toronto() - timedelta(hours=hours)


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------

def compute_id(title: str | None, link: str | None) -> str:
    """
    Create a stable ID from a title+link pair using SHA1 (hex).
    Using both guards against duplicate headlines across outlets.

    Parameters
    ----------
    title : str | None
        Article title (may be empty/None).
    link : str | None
        Article URL (may be empty/None).

    Returns
    -------
    str
        Hex digest string (40 chars).
    """
    base = f"{(title or '').strip()}|{(link or '').strip()}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


# ---------------------------------------------------------------------------
# Text normalization
# ---------------------------------------------------------------------------

_whitespace_re = re.compile(r"\s+", re.MULTILINE)
_punct_re = re.compile(r"[^\w\s]", re.UNICODE)


def normalize_text(text: str | None) -> str:
    """
    Normalize text for keyword matching and fuzzy comparisons:
      - coerce to str
      - unicode NFKC normalize
      - lower-case
      - strip punctuation
      - collapse whitespace

    This keeps alphanumerics and underscores; removes symbols/emoji.

    Parameters
    ----------
    text : str | None

    Returns
    -------
    str
        Cleaned, lower-case text with single spaces.
    """
    if not text:
        return ""
    s = unicodedata.normalize("NFKC", str(text))
    s = s.lower()
    # Remove punctuation/symbols but keep word chars & whitespace
    s = _punct_re.sub(" ", s)
    # Collapse whitespace
    s = _whitespace_re.sub(" ", s).strip()
    return s


# ---------------------------------------------------------------------------
# Headline near-duplicate detection
# ---------------------------------------------------------------------------

def near_duplicate(a: str | None, b: str | None, *, threshold: int = 90) -> bool:
    """
    Heuristic duplicate check between two titles using token-set ratio.

    Parameters
    ----------
    a, b : str | None
        Headline strings to compare.
    threshold : int
        0..100. Higher means stricter; 90 is conservative for news titles.

    Returns
    -------
    bool
    """
    if not a or not b:
        return False
    a_n = normalize_text(a)
    b_n = normalize_text(b)
    if not a_n or not b_n:
        return False
    score = fuzz.token_set_ratio(a_n, b_n)
    return score >= threshold


# ---------------------------------------------------------------------------
# Lightweight sentiment labelling
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _SentimentLexicon:
    positive: tuple[str, ...]
    negative: tuple[str, ...]


# Minimal domain-agnostic lexicon (no downloads required).
# We bias slightly toward neutral to avoid over-labelling.
_LEXICON = _SentimentLexicon(
    positive=(
        "improve", "improves", "improved", "improving",
        "progress", "progresses",
        "boost", "boosts", "boosted",
        "increase", "increases", "increased",
        "expand", "expands", "expanded",
        "approve", "approves", "approved",
        "support", "supports", "supported",
        "funding", "invest", "investment", "invests",
        "affordable", "affordability",
        "win", "wins", "won",
        "successful", "success",
    ),
    negative=(
        "concern", "concerns",
        "problem", "problems",
        "delay", "delays", "delayed",
        "halt", "halts", "halted",
        "pause", "paused",
        "probe", "investigation", "investigations",
        "criticize", "criticism", "criticized",
        "oppose", "opposes", "opposition",
        "lawsuit", "appeal", "appeals",
        "risk", "risks",
        "crisis", "shortage", "deficit",
        "homelessness", "encampment",
        "collapse", "fail", "fails", "failed", "failure",
        "fraud", "scandal",
        "overrun", "overruns",
        "charge", "charged",
        "resign", "resignation",
    ),
)


def _count_hits(tokens: Iterable[str], vocab: Iterable[str]) -> int:
    vocab_set = set(vocab)
    return sum(1 for t in tokens if t in vocab_set)


def label_sentiment(text: str | None) -> str:
    """
    Return a coarse sentiment label: 'positive' | 'negative' | 'neutral'.

    Rationale
    ---------
    We avoid heavyweight models and external downloads (e.g., VADER),
    preferring a small lexicon and normalized token overlap. The goal is
    triage-level signal, *not* NLP-grade sentiment analysis.

    Heuristic
    ---------
    - tokenize normalized text (whitespace)
    - count positive/negative lexicon hits
    - require a 2+ margin to move away from neutral

    Parameters
    ----------
    text : str | None

    Returns
    -------
    str
    """
    if not text:
        return "neutral"

    norm = normalize_text(text)
    if not norm:
        return "neutral"

    tokens = norm.split()
    pos = _count_hits(tokens, _LEXICON.positive)
    neg = _count_hits(tokens, _LEXICON.negative)

    # Small margin to keep neutrality dominant on mixed signals
    if pos - neg >= 2:
        return "positive"
    if neg - pos >= 2:
        return "negative"
    return "neutral"
