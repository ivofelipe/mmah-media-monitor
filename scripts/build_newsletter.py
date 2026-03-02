
import json
from pathlib import Path
from datetime import datetime
import pytz
from jinja2 import Environment, FileSystemLoader, select_autoescape
from utils import now_toronto

DOCS_DIR = Path("docs")
DOCS_DIR.mkdir(exist_ok=True)
TEMPLATES_DIR = Path("templates")

def load_json(path):
    if Path(path).exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def human_time(iso):
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = pytz.timezone("America/Toronto").localize(dt)
        return dt.astimezone(pytz.timezone("America/Toronto")).strftime("%b %d, %Y %I:%M %p")
    except Exception:
        return iso

def tone_label(compound):
    if compound >= 0.25:
        return "Positive"
    if compound <= -0.25:
        return "Negative"
    return "Neutral / Mixed"

def summarize_top(stories):
    if not stories:
        return "No relevant coverage detected in the last 16 hours."
    housing_n = sum(1 for s in stories if "Housing" in s.get("categories", []))
    muni_n = sum(1 for s in stories if "Municipal Affairs" in s.get("categories", []))
    avg = 0.0
    if stories:
        avg = sum(s["sentiment"]["compound"] for s in stories) / max(1, len(stories))
    tone = "mixed"
    if avg >= 0.25: tone = "generally positive"
    elif avg <= -0.25: tone = "generally negative"

    top_outlets = {}
    for s in stories:
        top_outlets[s["outlet"]] = top_outlets.get(s["outlet"], 0) + 1
    leaders = sorted(top_outlets.items(), key=lambda x: x[1], reverse=True)[:3]
    leaders_str = ", ".join([f"{n} ({c})" for n,c in leaders]) if leaders else "various local outlets"

    return (
        f"Coverage in the last 16 hours includes {housing_n} housing and {muni_n} municipal affairs "
        f"stories across {leaders_str}, with {tone} tone overall."
    )

def compute_overall_tone(stories):
    if not stories:
        return "Neutral / Mixed"
    avg = sum(s["sentiment"]["compound"] for s in stories) / len(stories)
    return tone_label(avg)

def main():
    date_str = now_toronto().strftime("%Y-%m-%d")
    news = load_json(f"data/news_{date_str}.json")

    for s in news:
        s["published_human"] = human_time(s["published"])
        s["tone_label"] = tone_label(s["sentiment"]["compound"])
    housing = [s for s in news if "Housing" in s.get("categories", [])]
    municipal = [s for s in news if "Municipal Affairs" in s.get("categories", [])]

    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=select_autoescape(["html", "xml"])
    )
    template = env.get_template("newsletter.html.j2")

    html = template.render(
        date_human=now_toronto().strftime("%B %d, %Y"),
        generated=now_toronto().strftime("%b %d, %Y %I:%M %p"),
        top_summary=summarize_top(news),
        overall_tone=compute_overall_tone(news),
        housing=housing,
        municipal=municipal,
    )

    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")
    archives = Path("docs/archive")
    archives.mkdir(parents=True, exist_ok=True)
    (archives / f"{date_str}.html").write_text(html, encoding="utf-8")
    print(f"[INFO] Built newsletter: docs/index.html and docs/archive/{date_str}.html")

if __name__ == "__main__":
    main()
