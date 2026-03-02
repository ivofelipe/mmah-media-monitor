# MMAH Daily Media Monitor (Ontario)

This repository builds a daily media monitoring **newsletter** for the Ontario Ministry of Municipal Affairs and Housing.

## What it does

- Collects Ontario media **RSS** items from the last **16 hours**
- Filters by **Housing** and **Municipal Affairs** topics
- Deduplicates near-identical headlines
- Performs basic **sentiment** analysis (VADER)
- Renders a static HTML page under **docs/** and publishes to **GitHub Pages**
- Runs daily at **7:00 a.m. America/Toronto** (DST-safe)

## Quick start

1. Enable **GitHub Pages** → Deploy from a branch → `main` → `/docs`.
2. Review and adjust RSS feeds in `config/sources.yml`.
3. Tune keywords in `config/keywords.yml` (optional).
4. Commit and push; the page will be available at:
   - `https://<your-org-or-user>.github.io/<repo-name>/`
5. **One-time test run**: use the workflow’s **Run workflow** button (see below). It bypasses the 7 a.m. guard so you can validate immediately.

## Test run & schedule

- The workflow runs **hourly**, but only **publishes at 07:00** Toronto time.
- If you trigger `Build MMAH Newsletter` via **Actions → Run workflow**, the job **bypasses** the hour check and builds immediately (good for first-run testing).

## Ontario wordmark

- Replace `assets/ontario-wordmark.svg` with the **official wordmark** before publishing externally. See the Government of Ontario **logo usage guidelines** and ensure proper clearspace, colour and size. The included file is a **placeholder**.

## Local testing

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/fetch_news.py
python scripts/build_newsletter.py
open docs/index.html
```

## Notes & compliance

- Uses **RSS** and stores **metadata only** (title, snippet, link, outlet).
- Links out to original content. Respect each site's terms of use.
- If a feed changes or breaks, update its URL in `sources.yml`.
