"""Collect issue dates for monthly CPI releases from the NSO archive.

The public archive exposes an issue date (day precision), not a verified time
of day. Immutable, content-addressed snapshots are written only when archive
records change. This helper intentionally lives outside ``ingestion/`` so the
existing IMF CPI collector and its event schema are left untouched.
"""

from __future__ import annotations

import hashlib
import json
import calendar
import re
import time
from datetime import date, datetime, time as daytime, timedelta, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "nso_cpi_release_calendar"
ARCHIVE_URL = "https://www.nso.gov.vn/en/cpi/"
MIN_PERIOD = "2015-01"
REQUEST_TIMEOUT = 30
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2,
    "mar": 3, "march": 3, "apr": 4, "april": 4, "may": 5,
    "jun": 6, "june": 6, "jul": 7, "july": 7, "aug": 8,
    "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


class TextExtractor(HTMLParser):
    """Convert HTML snippets to plain text without a third-party parser."""

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        value = data.strip()
        if value:
            self.parts.append(value)


def plain_text(fragment: str) -> str:
    parser = TextExtractor()
    parser.feed(unescape(fragment or ""))
    return " ".join(parser.parts)


def parse_period(value: str, title: str) -> str | None:
    numeric = re.search(r"(?<!\d)(\d{1,2})\s*/\s*(20\d{2})(?!\d)", value)
    if numeric:
        month, year = int(numeric.group(1)), int(numeric.group(2))
        return f"{year:04d}-{month:02d}" if 1 <= month <= 12 else None
    named = re.search(r"\b([A-Za-z]+)(?:\s+of)?\s+(20\d{2})\b", value)
    if not named:
        named = re.search(r"\b([A-Za-z]+)(?:\s+of)?\s+(20\d{2})\b", title)
    if named:
        month = MONTHS.get(named.group(1).lower())
        if month:
            return f"{int(named.group(2)):04d}-{month:02d}"
    return None


def parse_issue_date(value: str) -> date | None:
    match = re.search(r"(\d{1,2})/(\d{1,2})/(20\d{2})", value)
    if not match:
        return None
    day, month, year = map(int, match.groups())  # NSO archive uses DD/MM/YYYY.
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_detail_issue_date(page_html: str) -> date | None:
    match = re.search(
        r'class=["\']date-of-issue["\'][^>]*>\s*Date\s+of\s+issue\s*:\s*<span>\s*(\d{1,2}/\d{1,2}/20\d{2})',
        page_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return parse_issue_date(match.group(1)) if match else None


def parse_archive_page(page_html: str) -> list[dict]:
    rows: list[dict] = []
    for card in re.findall(
        r'<section\s+class=["\']item["\']\s*>(.*?)</section>',
        page_html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        title_match = re.search(r"<h3[^>]*>(.*?)</h3>", card, re.IGNORECASE | re.DOTALL)
        issue_match = re.search(
            r'class=["\']archive-issue-date["\'][^>]*>(.*?)</span>',
            card,
            re.IGNORECASE | re.DOTALL,
        )
        period_match = re.search(
            r'class=["\']archive-reference-period["\'][^>]*>(.*?)</span>',
            card,
            re.IGNORECASE | re.DOTALL,
        )
        if not title_match or not issue_match or not period_match:
            continue

        title = plain_text(title_match.group(1))
        issue_date = parse_issue_date(plain_text(issue_match.group(1)))
        period = parse_period(plain_text(period_match.group(1)), title)
        if not issue_date or not period or period < MIN_PERIOD:
            continue
        if not ("consumer price index" in title.lower() or "cpi" in title.lower()):
            continue

        # The archive gives only a calendar date. Treat information as
        # available after the whole issue date has passed in Vietnam time.
        conservative_available_at = datetime.combine(
            issue_date + timedelta(days=1), daytime.min, tzinfo=VIETNAM_TZ
        ).astimezone(timezone.utc)
        prior_html = page_html[: page_html.find(card)]
        url_matches = re.findall(
            r'<a\s+href=["\']([^"\']+)["\'][^>]*>\s*</p>',
            prior_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        rows.append(
            {
                "target_month": period,
                "issue_date": issue_date.isoformat(),
                "release_timestamp_precision": "date_only",
                "conservative_available_at_proxy_utc": conservative_available_at.isoformat(),
                "availability_basis": "nso_issue_date_plus_one_day_vietnam_midnight_proxy",
                "article_title": title,
                "article_url": url_matches[-1] if url_matches else None,
                "source": "National Statistics Office of Vietnam CPI archive",
            }
        )
    return rows


def collect_calendar() -> list[dict]:
    session = requests.Session()
    session.headers.update({"User-Agent": "NowCPI research pipeline/1.0"})
    by_period: dict[str, dict] = {}
    found_oldest_period = False

    for page in range(1, 100):
        response = session.get(
            ARCHIVE_URL,
            params={"paged": page},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        page_rows = parse_archive_page(response.text)
        if not page_rows:
            raise RuntimeError(
                f"No issue-date records parsed from NSO CPI archive page {page}."
            )
        for row in page_rows:
            old = by_period.get(row["target_month"])
            # Prefer the dedicated CPI index article over the press summary if
            # two articles reference the same month; first archive occurrence
            # is normally the latest and carries the dedicated index title.
            dedicated = "gold price index" in row["article_title"].lower()
            old_dedicated = old and "gold price index" in old["article_title"].lower()
            if old is None or (dedicated and not old_dedicated):
                by_period[row["target_month"]] = row
        if any(row["target_month"] == MIN_PERIOD for row in page_rows) or any(
            row["target_month"] < MIN_PERIOD for row in page_rows
        ):
            found_oldest_period = True
            break
        time.sleep(0.15)

    if not found_oldest_period:
        raise RuntimeError(f"NSO archive scan did not reach {MIN_PERIOD}.")
    if not by_period:
        raise RuntimeError("No NSO CPI release dates were found.")

    # The CPI archive tag omits a few old posts. Fill only missing months by
    # searching the same NSO WordPress site and reading each article's
    # date-of-issue widget. This is still source metadata from NSO itself.
    end_period = max(by_period)
    year, month = map(int, end_period.split("-"))
    missing_periods = []
    for current_year in range(2015, year + 1):
        last_month = month if current_year == year else 12
        for current_month in range(1, last_month + 1):
            period = f"{current_year:04d}-{current_month:02d}"
            if period not in by_period:
                missing_periods.append(period)

    api_search = "https://www.nso.gov.vn/wp-json/wp/v2/search"
    for period in missing_periods:
        period_year, period_month = map(int, period.split("-"))
        month_name = calendar.month_name[period_month]
        response = session.get(
            api_search,
            params={
                "search": f"Consumer price index {month_name} {period_year}",
                "per_page": 20,
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        candidates = response.json()
        for candidate in candidates:
            title = plain_text(candidate.get("title", ""))
            if "consumer price index" not in title.lower() and "cpi" not in title.lower():
                continue
            if parse_period("", title) != period:
                continue
            article_url = candidate.get("url")
            if not article_url:
                continue
            article = session.get(article_url, timeout=REQUEST_TIMEOUT)
            article.raise_for_status()
            issue_date = parse_detail_issue_date(article.text)
            if not issue_date:
                continue
            available_at = datetime.combine(
                issue_date + timedelta(days=1), daytime.min, tzinfo=VIETNAM_TZ
            ).astimezone(timezone.utc)
            by_period[period] = {
                "target_month": period,
                "issue_date": issue_date.isoformat(),
                "release_timestamp_precision": "date_only",
                "conservative_available_at_proxy_utc": available_at.isoformat(),
                "availability_basis": "nso_issue_date_plus_one_day_vietnam_midnight_proxy",
                "article_title": title,
                "article_url": article_url,
                "source": "National Statistics Office of Vietnam CPI article archive",
            }
            break
        time.sleep(0.15)

    still_missing = sorted(set(missing_periods) - set(by_period))
    if still_missing:
        raise RuntimeError(
            "NSO CPI release dates remain missing for: " + ", ".join(still_missing)
        )
    return [by_period[key] for key in sorted(by_period)]


def save_content_version(rows: list[dict]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    canonical = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    content_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    path = OUTPUT_DIR / f"snapshot_{content_hash[:16]}.json"
    if not path.exists():
        snapshot = {
            "schema_version": 1,
            "snapshot_id": content_hash,
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_url": ARCHIVE_URL,
            "metadata_precision": "issue date only; exact publication time is not exposed",
            "record_count": len(rows),
            "records": rows,
        }
        path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def main() -> None:
    rows = collect_calendar()
    path = save_content_version(rows)
    print(
        f"NSO CPI release calendar: {len(rows)} month(s), "
        f"{rows[0]['target_month']}..{rows[-1]['target_month']}; snapshot={path}"
    )


if __name__ == "__main__":
    main()
