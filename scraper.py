#!/usr/bin/env python3
"""
San Diego County Property Tax Auction Scraper
==============================================
Scrapes property auction data from https://sdttc.mytaxsale.com/auction/49
and stores it in a local PostgreSQL database.

Usage:
    python scraper.py                     # Scrape all pages
    python scraper.py --pages 1-5         # Scrape pages 1 through 5
    python scraper.py --dry-run           # Print data without saving to DB
    python scraper.py --db-url URL        # Custom database URL
"""

import argparse
import logging
import math
import re
import sys
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://sdttc.mytaxsale.com"
AUCTION_ID = 49
AUCTION_URL = f"{BASE_URL}/auction/{AUCTION_ID}"
DEFAULT_DB_URL = "postgresql://localhost:5432/sandiego_auction"

# Throttle between requests (seconds)
PAGE_DELAY = 1.5
DETAIL_DELAY = 0.3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_currency(text: str) -> Optional[Decimal]:
    """Convert '$68,000.00' -> Decimal('68000.00'). Returns None for '-' or empty."""
    if not text:
        return None
    # Aggressively strip all whitespace
    cleaned = "".join(text.split())
    if cleaned in ("-", ""):
        return None
    cleaned = re.sub(r"[,$]", "", cleaned)
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        log.warning("Could not parse currency: %r", text)
        return None


def parse_close_time(text: str) -> Optional[datetime]:
    """Parse '3/16/26 8:00 AM' -> datetime. Returns None for '-' or empty."""
    text = text.strip()
    if not text or text == "-":
        return None
    # Normalize whitespace (the page has line breaks between date and time)
    text = " ".join(text.split())
    for fmt in ("%m/%d/%y %I:%M %p", "%m/%d/%Y %I:%M %p"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    log.warning("Could not parse close time: %r", text)
    return None


def parse_int(text: str) -> Optional[int]:
    """Parse integer from string, return None if not numeric."""
    if not text or not text.strip():
        return None
    cleaned = text.strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------


def create_session() -> requests.Session:
    """Create a requests session with browser-like headers."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    # Hit the main page first to establish session cookies
    resp = session.get(AUCTION_URL)
    resp.raise_for_status()
    log.info("Session established (cookies: %d)", len(session.cookies))
    return session


def get_total_pages(session: requests.Session) -> int:
    """Determine the total number of pages from page 1."""
    resp = session.get(AUCTION_URL, params={"page": 1})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Strategy 1: "N properties available for sale" in .page_desc div
    page_desc = soup.find(class_="page_desc")
    if page_desc:
        m = re.search(r"(\d+)\s+propert", page_desc.get_text())
        if m:
            total_items = int(m.group(1))
            # Count items on page 1 to determine page size
            items_on_page1 = len(set(
                re.search(r"toggle_item_details\(\s*'(\d+)'\s*,\s*'(\d+)'\s*\)", a.get("href","")).group(2)
                for a in soup.find_all("a", href=re.compile(r"toggle_item_details"))
                if re.search(r"toggle_item_details\(\s*'(\d+)'\s*,\s*'(\d+)'\s*\)", a.get("href",""))
            ))
            per_page = items_on_page1 if items_on_page1 > 0 else 25
            total_pages = math.ceil(total_items / per_page)
            log.info("Site reports %d properties (~%d per page → %d pages)",
                     total_items, per_page, total_pages)
            return total_pages

    # Strategy 2: "Page X of Y" text
    paging_text = soup.find(string=re.compile(r"of\s+\d+"))
    if paging_text:
        match = re.search(r"of\s+(\d+)", paging_text)
        if match:
            return int(match.group(1))

    # Fallback: look in tfoot / pagination area
    for td in soup.find_all("td"):
        text = td.get_text()
        match = re.search(r"of\s+(\d+)", text)
        if match:
            return int(match.group(1))

    log.warning("Could not determine total pages, defaulting to 29")
    return 29


def scrape_page(session: requests.Session, page: int) -> list[dict]:
    """Scrape one page of auction results. Returns list of item dicts."""
    url = AUCTION_URL
    params = {"page": page}
    resp = session.get(url, params=params)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    items = []

    # Find all toggle_item_details links to get internal IDs and row data
    toggle_links = soup.find_all("a", href=re.compile(r"toggle_item_details"))
    # Deduplicate - each item's toggle link can appear multiple times
    seen_ids = set()

    for link in toggle_links:
        # Extract internal item ID from: javascript:toggle_item_details('49', '18433')
        href = link.get("href", "")
        id_match = re.search(r"toggle_item_details\(\s*'(\d+)'\s*,\s*'(\d+)'\s*\)", href)
        if not id_match:
            continue

        auction_id = int(id_match.group(1))
        internal_id = int(id_match.group(2))

        if internal_id in seen_ids:
            continue
        seen_ids.add(internal_id)

        # The link is in a <td>, which is in a <tr> - get the parent row
        row = link.find_parent("tr")
        if not row:
            continue

        cells = row.find_all("td")
        if len(cells) < 6:
            continue

        item_number = cells[1].get_text(strip=True)
        opening_bid = parse_currency(cells[2].get_text(strip=True))
        best_bid = parse_currency(cells[3].get_text(strip=True))
        close_time = parse_close_time(cells[4].get_text(" ", strip=True))
        status = cells[5].get_text(strip=True)

        # Check for cancel reason in the next sibling row
        cancel_reason = None
        next_row = row.find_next_sibling("tr")
        if next_row:
            next_text = next_row.get_text(strip=True)
            if next_text in ("WITHDRAWN", "REDEEMED"):
                cancel_reason = next_text

        item = {
            "auction_id": auction_id,
            "internal_id": internal_id,
            "item_number": item_number,
            "opening_bid": opening_bid,
            "best_bid": best_bid,
            "close_time": close_time,
            "status": status,
            "cancel_reason": cancel_reason,
        }
        items.append(item)

    log.info("Page %d: found %d items", page, len(items))
    return items


def fetch_item_details(session: requests.Session, internal_id: int) -> dict:
    """Fetch detail fields for a single item via the AJAX endpoint."""
    url = f"{AUCTION_URL}/{internal_id}/item_details"
    params = {"_": int(time.time() * 1000)}  # cache buster
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Referer": AUCTION_URL,
    }

    try:
        resp = session.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("Failed to fetch details for internal_id %d: %s", internal_id, exc)
        return {}

    # Response is JSON: {"item_details.{id}": "<html string>"}
    try:
        data = resp.json()
        html_key = f"item_details.{internal_id}"
        html_content = data.get(html_key, "")
    except (ValueError, KeyError):
        # Fallback: treat entire response as HTML
        html_content = resp.text

    soup = BeautifulSoup(html_content, "html.parser")

    details = {}
    field_map = {
        "APN:": "apn",
        "Property Type:": "property_type",
        "ID#:": "_detail_id",
        "Address:": "address",
        "City:": "city",
        "Postal Code:": "postal_code",
        "Tax Rate Area:": "tax_rate_area",
        "Land Value:": "land_value",
        "Improvements:": "improvements",
        "Total Assessed Value:": "total_assessed_value",
        "Assessed Value Year:": "assessed_value_year",
        "Property Description:": "property_description",
        "Timeshare Association:": "timeshare_association",
        "Default Year:": "default_year",
        "Assessee:": "assessee",
    }

    rows = soup.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True)
            value = " ".join(cells[1].get_text().split()).strip()
            if label in field_map:
                key = field_map[label]
                details[key] = value if value else None

    # Convert numeric fields
    for currency_field in ("land_value", "improvements", "total_assessed_value"):
        if currency_field in details and details[currency_field]:
            details[currency_field] = parse_currency(details[currency_field])

    for int_field in ("assessed_value_year", "default_year"):
        if int_field in details and details[int_field]:
            details[int_field] = parse_int(details[int_field])

    # Remove the duplicate ID field
    details.pop("_detail_id", None)

    return details


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def init_db(db_url: str) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL and create the table if needed."""
    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    schema_sql = open("schema.sql").read()
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()
    log.info("Database initialized")
    return conn


def upsert_property(conn, item: dict):
    """Insert or update a single property record."""
    sql = """
        INSERT INTO auction_properties (
            item_number, opening_bid, best_bid, close_time, status, cancel_reason,
            apn, property_type, address, city, postal_code, tax_rate_area,
            land_value, improvements, total_assessed_value, assessed_value_year,
            property_description, timeshare_association, default_year, assessee,
            internal_id, auction_id, scraped_at
        ) VALUES (
            %(item_number)s, %(opening_bid)s, %(best_bid)s, %(close_time)s,
            %(status)s, %(cancel_reason)s,
            %(apn)s, %(property_type)s, %(address)s, %(city)s, %(postal_code)s,
            %(tax_rate_area)s, %(land_value)s, %(improvements)s,
            %(total_assessed_value)s, %(assessed_value_year)s,
            %(property_description)s, %(timeshare_association)s,
            %(default_year)s, %(assessee)s,
            %(internal_id)s, %(auction_id)s, NOW()
        )
        ON CONFLICT (auction_id, item_number) DO UPDATE SET
            opening_bid          = COALESCE(EXCLUDED.opening_bid,          auction_properties.opening_bid),
            best_bid             = COALESCE(EXCLUDED.best_bid,             auction_properties.best_bid),
            close_time           = COALESCE(EXCLUDED.close_time,           auction_properties.close_time),
            status               = EXCLUDED.status,
            cancel_reason        = COALESCE(EXCLUDED.cancel_reason,        auction_properties.cancel_reason),
            apn                  = COALESCE(EXCLUDED.apn,                  auction_properties.apn),
            property_type        = COALESCE(EXCLUDED.property_type,        auction_properties.property_type),
            address              = COALESCE(EXCLUDED.address,              auction_properties.address),
            city                 = COALESCE(EXCLUDED.city,                 auction_properties.city),
            postal_code          = COALESCE(EXCLUDED.postal_code,          auction_properties.postal_code),
            tax_rate_area        = COALESCE(EXCLUDED.tax_rate_area,        auction_properties.tax_rate_area),
            land_value           = COALESCE(EXCLUDED.land_value,           auction_properties.land_value),
            improvements         = COALESCE(EXCLUDED.improvements,         auction_properties.improvements),
            total_assessed_value = COALESCE(EXCLUDED.total_assessed_value, auction_properties.total_assessed_value),
            assessed_value_year  = COALESCE(EXCLUDED.assessed_value_year,  auction_properties.assessed_value_year),
            property_description = COALESCE(EXCLUDED.property_description, auction_properties.property_description),
            timeshare_association= COALESCE(EXCLUDED.timeshare_association,auction_properties.timeshare_association),
            default_year         = COALESCE(EXCLUDED.default_year,         auction_properties.default_year),
            assessee             = COALESCE(EXCLUDED.assessee,             auction_properties.assessee),
            internal_id          = EXCLUDED.internal_id,
            scraped_at           = NOW()
    """

    # Ensure all keys exist
    defaults = {
        "apn": None, "property_type": None, "address": None, "city": None,
        "postal_code": None, "tax_rate_area": None, "land_value": None,
        "improvements": None, "total_assessed_value": None,
        "assessed_value_year": None, "property_description": None,
        "timeshare_association": None, "default_year": None, "assessee": None,
    }
    record = {**defaults, **item}

    with conn.cursor() as cur:
        cur.execute(sql, record)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Scrape San Diego County property tax auction data"
    )
    parser.add_argument(
        "--db-url",
        default=DEFAULT_DB_URL,
        help=f"PostgreSQL connection URL (default: {DEFAULT_DB_URL})",
    )
    parser.add_argument(
        "--pages",
        default=None,
        help="Page range to scrape, e.g. '1-5' or '3' (default: all pages)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print scraped data instead of saving to database",
    )
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Skip fetching item details (only scrape main table data)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DETAIL_DELAY,
        help=f"Delay between detail requests in seconds (default: {DETAIL_DELAY})",
    )
    args = parser.parse_args()

    # Determine page range
    if args.pages:
        if "-" in args.pages:
            start, end = args.pages.split("-", 1)
            page_range = range(int(start), int(end) + 1)
        else:
            page_range = range(int(args.pages), int(args.pages) + 1)
    else:
        page_range = None  # Will be determined after fetching page 1

    # Database connection
    conn = None
    if not args.dry_run:
        try:
            conn = init_db(args.db_url)
        except Exception as exc:
            log.error("Database connection failed: %s", exc)
            log.error(
                "Make sure PostgreSQL is running and the database exists.\n"
                "  createdb sandiego_auction\n"
                "Or pass a custom URL: --db-url postgresql://user:pass@host:5432/dbname"
            )
            sys.exit(1)

    # Create HTTP session
    session = create_session()

    # Get total pages if not specified
    if page_range is None:
        total_pages = get_total_pages(session)
        page_range = range(1, total_pages + 1)
        log.info("Will scrape %d pages", total_pages)

    total_items = 0
    total_details = 0
    seen_item_numbers = set()  # track every item_number seen on the site this run
    is_full_scrape = args.pages is None  # only mark "Removed" after a full run

    try:
        for page_num in page_range:
            log.info("--- Scraping page %d ---", page_num)
            items = scrape_page(session, page_num)

            for item in items:
                seen_item_numbers.add(item["item_number"])

                # Fetch item details
                if not args.skip_details:
                    details = fetch_item_details(session, item["internal_id"])
                    item.update(details)
                    total_details += 1
                    time.sleep(args.delay)

                if args.dry_run:
                    _print_item(item)
                else:
                    upsert_property(conn, item)

                total_items += 1

            # Commit after each page
            if conn:
                conn.commit()
                log.info("Page %d committed (%d items so far)", page_num, total_items)

            time.sleep(PAGE_DELAY)

        # After a full scrape, mark any Upcoming properties that weren't seen on
        # the site as "Removed" — they have disappeared from the auction listing.
        if is_full_scrape and conn and seen_item_numbers and not args.dry_run:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE auction_properties
                       SET status = 'Removed', scraped_at = NOW()
                     WHERE auction_id = %(auction_id)s
                       AND status = 'Upcoming'
                       AND item_number != ALL(%(seen)s)
                    RETURNING item_number
                """, {"auction_id": AUCTION_ID, "seen": list(seen_item_numbers)})
                removed_rows = cur.fetchall()
            conn.commit()
            if removed_rows:
                removed_nums = sorted(r[0] for r in removed_rows)
                log.info("Marked %d properties as Removed (no longer on site): %s",
                         len(removed_nums), removed_nums)

    except KeyboardInterrupt:
        log.info("\nInterrupted by user")
        if conn:
            conn.commit()
            log.info("Partial results committed")
    finally:
        if conn:
            conn.close()

    log.info("Done. Scraped %d items (%d with details).", total_items, total_details)


def _print_item(item: dict):
    """Pretty-print a single item for dry-run mode."""
    print(f"\n{'='*60}")
    print(f"  Item #{item.get('item_number', '?')}  (internal: {item.get('internal_id')})")
    print(f"  Opening Bid: {item.get('opening_bid')}  |  Best Bid: {item.get('best_bid')}")
    print(f"  Close: {item.get('close_time')}  |  Status: {item.get('status')}")
    if item.get("cancel_reason"):
        print(f"  Cancel Reason: {item['cancel_reason']}")
    if item.get("apn"):
        print(f"  APN: {item['apn']}")
        print(f"  Address: {item.get('address')}, {item.get('city')} {item.get('postal_code')}")
        print(f"  Type: {item.get('property_type')}")
        print(f"  Assessed: ${item.get('total_assessed_value')} ({item.get('assessed_value_year')})")
        print(f"  Land: ${item.get('land_value')}  Improvements: ${item.get('improvements')}")
        print(f"  Default Year: {item.get('default_year')}  |  Assessee: {item.get('assessee')}")
        print(f"  Description: {item.get('property_description')}")


if __name__ == "__main__":
    main()
