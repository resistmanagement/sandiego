#!/usr/bin/env python3
"""
Re-parse all saved raw Redfin HTML files using the fixed enrich.py parser,
then update the database.

This does NOT make any network requests — it only reads local HTML files
from raw_html/<item_number>/redfin.html.

Usage:
    python reparse_html.py               # Re-parse all
    python reparse_html.py --items 0340  # Specific items only
    python reparse_html.py --dry-run     # Print without saving
"""
import argparse
import sys
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

# Patch requests.Session.get BEFORE importing enrich so the session is never used
import requests

DEFAULT_DB_URL = "postgresql://localhost:5432/sandiego_auction"
RAW_HTML_DIR = Path("raw_html")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def make_mock_response(html: str, url: str) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.text = html
    resp.url = url
    return resp


def reparse_item(item_number: str, redfin_url: str) -> dict:
    """Parse the saved HTML for an item using the (now fixed) fetch_redfin_data."""
    html_path = RAW_HTML_DIR / item_number / "redfin.html"
    if not html_path.exists():
        return {}

    html = html_path.read_text(encoding="utf-8", errors="replace")

    # Monkey-patch requests so fetch_redfin_data reads local file instead of fetching
    import enrich
    session = MagicMock()
    session.get.return_value = make_mock_response(html, redfin_url or f"https://www.redfin.com/home/{item_number}")

    result = enrich.fetch_redfin_data(session, redfin_url or f"https://www.redfin.com/home/{item_number}")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--items", help="Comma-separated item numbers to re-parse")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    args = parser.parse_args()

    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(args.db_url)

    # Load all properties with saved HTML
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if args.items:
            items = [i.strip() for i in args.items.split(",")]
            cur.execute(
                "SELECT id, item_number, redfin_url FROM auction_properties "
                "WHERE item_number = ANY(%s) ORDER BY item_number",
                (items,)
            )
        else:
            cur.execute(
                "SELECT id, item_number, redfin_url FROM auction_properties ORDER BY item_number"
            )
        properties = cur.fetchall()

    log.info("Checking %d properties for saved HTML...", len(properties))

    import enrich

    processed = 0
    skipped = 0
    updated = 0
    errors = 0

    for prop in properties:
        item = prop["item_number"]
        redfin_url = prop["redfin_url"] or ""
        html_path = RAW_HTML_DIR / item / "redfin.html"

        if not html_path.exists():
            skipped += 1
            continue

        processed += 1
        try:
            data = reparse_item(item, redfin_url)
        except Exception as e:
            log.error("[%s] Parse error: %s", item, e)
            errors += 1
            continue

        if not data:
            log.warning("[%s] No data extracted", item)
            continue

        estimate = data.get("redfin_estimate")
        sale_price = data.get("last_sale_price")
        sale_date = data.get("last_sale_date")

        if args.dry_run:
            log.info("[%s] estimate=%s  last_sale=%s on %s  beds=%s baths=%s sqft=%s",
                     item, estimate, sale_price, sale_date,
                     data.get("bedrooms"), data.get("bathrooms"), data.get("sqft"))
            continue

        try:
            enrich.update_enrichment(conn, item, data)
            conn.commit()
            updated += 1
            log.info("[%s] ✓  estimate=%s  last_sale=%s on %s",
                     item, estimate, sale_price, sale_date)
        except Exception as e:
            conn.rollback()
            log.error("[%s] DB error: %s", item, e)
            errors += 1

    conn.close()
    log.info("Done: %d processed, %d updated, %d skipped (no HTML), %d errors",
             processed, updated, skipped, errors)


if __name__ == "__main__":
    main()
