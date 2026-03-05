#!/usr/bin/env python3
"""
Redfin Property Enrichment Script
===================================
Enriches auction property data with Redfin property details:
- Property type (residential/commercial), bedroom/bathroom count
- Square footage, lot size, year built
- Redfin estimated value
- Property images
- Tax history (past 3+ years)
- Sale history
- GPS coordinates

Two-phase approach:
  Phase 1: Resolve auction addresses to Redfin URLs (via Playwright browser)
  Phase 2: Fetch each Redfin page and extract all data (via requests)

Usage:
    python enrich.py                         # Enrich all properties
    python enrich.py --resolve-only          # Only resolve URLs (Phase 1)
    python enrich.py --fetch-only            # Only fetch data (Phase 2, needs URLs)
    python enrich.py --items 0003,0034,0036  # Specific items only
    python enrich.py --dry-run               # Print data without saving
    python enrich.py --limit 10              # Process first N properties
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REDFIN_BASE = "https://www.redfin.com"
RESOLVED_URLS_FILE = "redfin_urls.csv"
PROPERTIES_FILE = "properties_list.txt"
PHOTOS_DIR = "photos"

# Throttle
FETCH_DELAY = 2.0  # seconds between Redfin page fetches
RESOLVE_DELAY = 1.5  # seconds between Playwright autocomplete searches

DEFAULT_DB_URL = "postgresql://localhost:5432/sandiego_auction"

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
    if not text:
        return None
    cleaned = "".join(text.split())
    if cleaned in ("-", ""):
        return None
    cleaned = re.sub(r"[,$]", "", cleaned)
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def parse_int(text: str) -> Optional[int]:
    if not text:
        return None
    cleaned = re.sub(r"[,\s]", "", text.strip())
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_float(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[,\s]", "", text.strip())
    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Phase 1: Resolve addresses to Redfin URLs
# ---------------------------------------------------------------------------


def load_properties_from_file() -> list[dict]:
    """Load properties from the scraped properties_list.txt or from DB."""
    # First try reading from auction scraper output
    if not os.path.exists(PROPERTIES_FILE):
        log.error("Properties file %s not found. Run scraper.py first.", PROPERTIES_FILE)
        sys.exit(1)

    properties = []
    with open(PROPERTIES_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("ID#") or line.startswith("---"):
                continue
            # Format: "0003     $68,000.00  Upcoming     919 OLIVE AVE FALLBROOK CA 92028-1560"
            match = re.match(
                r"(\d{4})\s+(\$[\d,]+\.\d{2})\s+(Upcoming|Canceled)\s+(.*)",
                line,
            )
            if match:
                properties.append({
                    "item_number": match.group(1),
                    "opening_bid": match.group(2),
                    "status": match.group(3),
                    "address": match.group(4).strip(),
                })
    log.info("Loaded %d properties from %s", len(properties), PROPERTIES_FILE)
    return properties


def load_resolved_urls() -> dict[str, str]:
    """Load previously resolved Redfin URLs from CSV."""
    urls = {}
    if os.path.exists(RESOLVED_URLS_FILE):
        with open(RESOLVED_URLS_FILE) as f:
            reader = csv.DictReader(f)
            for row in reader:
                urls[row["item_number"]] = row.get("redfin_url", "")
    return urls


def save_resolved_urls(urls: dict[str, str]):
    """Save resolved Redfin URLs to CSV."""
    with open(RESOLVED_URLS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["item_number", "redfin_url"])
        writer.writeheader()
        for item_number, url in sorted(urls.items()):
            writer.writerow({"item_number": item_number, "redfin_url": url})
    log.info("Saved %d URLs to %s", len(urls), RESOLVED_URLS_FILE)


def resolve_address_to_redfin_url(address: str) -> Optional[str]:
    """
    Resolve an address to a Redfin property URL.
    Uses Redfin's search with constructed URL patterns.
    Returns the full Redfin URL or None.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    })

    # Parse address: "919 OLIVE AVE FALLBROOK CA 92028-1560"
    # Extract zip
    zip_match = re.search(r"(\d{5})(?:-\d{4})?$", address)
    if not zip_match:
        return None
    zipcode = zip_match.group(1)

    # Extract state (assume CA for all San Diego properties)
    state = "CA"

    # Remove state + zip from end to get street + city
    addr_no_zip = re.sub(r"\s*CA\s+\d{5}(?:-\d{4})?\s*$", "", address).strip()

    # Try to split street from city - city is typically the last 1-2 words
    # that aren't part of standard street suffixes
    parts = addr_no_zip.split()

    # Common street suffixes
    suffixes = {
        "ST", "AVE", "BLVD", "DR", "RD", "LN", "WAY", "CT", "CIR", "PL",
        "TER", "PKWY", "HWY", "LOOP", "PATH", "RUN", "TRL", "SQ",
    }

    # Try different splits, checking if construct resolves
    # Heuristic: find the last street suffix, everything after is city
    last_suffix_idx = -1
    for i, part in enumerate(parts):
        # Handle unit numbers like #308
        cleaned = part.rstrip(",").rstrip(".")
        if cleaned.upper() in suffixes:
            last_suffix_idx = i

    if last_suffix_idx >= 0 and last_suffix_idx < len(parts) - 1:
        # Check for unit numbers after suffix (e.g., AVE #308)
        street_end = last_suffix_idx + 1
        if street_end < len(parts) and parts[street_end].startswith("#"):
            street_end += 1
        street = " ".join(parts[:street_end])
        city = " ".join(parts[street_end:])
    else:
        # Fallback: can't parse, return None
        return None

    if not city:
        return None

    # Construct Redfin URL slug
    street_slug = re.sub(r"[#]", "", street)  # Remove # from unit numbers
    street_slug = re.sub(r"\s+", "-", street_slug.strip()).title()
    city_slug = re.sub(r"\s+", "-", city.strip()).title()

    # Try the URL - Redfin sometimes resolves without /home/ suffix via redirect
    url = f"{REDFIN_BASE}/CA/{city_slug}/{street_slug}-{zipcode}"

    try:
        resp = session.get(url, allow_redirects=True, timeout=15)
        if resp.status_code == 200 and "/home/" in resp.url:
            return resp.url
    except requests.RequestException:
        pass

    return None


def resolve_via_playwright(properties: list[dict], existing_urls: dict) -> dict[str, str]:
    """
    Resolve addresses to Redfin URLs using Playwright browser automation.
    Uses system Chrome with stealth settings to bypass bot detection.
    Intercepts stingray autocomplete API responses.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    urls = dict(existing_urls)
    to_resolve = [p for p in properties if p["item_number"] not in urls]

    if not to_resolve:
        log.info("All %d properties already resolved", len(properties))
        return urls

    # Build address cache from already-resolved URLs to avoid re-searching
    # identical addresses. Also tracks failed lookups to limit retries.
    address_cache: dict[str, str] = {}  # clean_address -> redfin_url (or "" for no result)
    address_fail_count: dict[str, int] = {}  # clean_address -> number of failed attempts
    MAX_ADDRESS_RETRIES = 3

    for item_num, url in existing_urls.items():
        # Find the property's address to seed the cache
        for p in properties:
            if p["item_number"] == item_num:
                addr = re.sub(r"-\d{4}$", "", p["address"])
                if url:
                    address_cache[addr] = url
                break

    log.info("Resolving %d addresses via Playwright browser...", len(to_resolve))

    with sync_playwright() as pw:
        # Use system Chrome with stealth settings
        try:
            browser = pw.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            browser = pw.chromium.launch(headless=False)

        context = browser.new_context()

        # Hide automation indicators
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)

        page = context.new_page()

        # Capture stingray autocomplete responses
        autocomplete_responses = []

        def handle_response(response):
            if "location-autocomplete" in response.url and "redfin" in response.url:
                try:
                    body = response.text()
                    # Response format: {}&&{"payload": {...}}
                    if "&&" in body:
                        json_str = body.split("&&", 1)[1]
                    else:
                        json_str = body
                    data = json.loads(json_str)
                    autocomplete_responses.append(data)
                except Exception:
                    pass

        page.on("response", handle_response)

        # Navigate to Redfin and wait for search box to become enabled
        page.goto("https://www.redfin.com/", wait_until="domcontentloaded")
        time.sleep(5)  # Give JS time to fully initialize

        # Wait for search box - try multiple approaches
        search_ready = False
        for attempt in range(3):
            try:
                page.wait_for_selector(
                    "#search-box-input:not([disabled])", state="attached", timeout=15000,
                )
                search_ready = True
                break
            except Exception:
                log.info("Search box not ready, waiting... (attempt %d)", attempt + 1)
                time.sleep(5)

        if not search_ready:
            log.error("Could not find enabled search box on Redfin")
            browser.close()
            return urls

        time.sleep(1)

        for i, prop in enumerate(to_resolve):
            address = prop["address"]
            item_num = prop["item_number"]

            # Remove zip+4 suffix for cleaner search
            clean_address = re.sub(r"-\d{4}$", "", address)

            # Check address cache — reuse result if we've already searched this address
            if clean_address in address_cache:
                cached_url = address_cache[clean_address]
                urls[item_num] = cached_url
                if cached_url:
                    log.info("[%d/%d] %s -> %s (cached)", i + 1, len(to_resolve), item_num, cached_url)
                else:
                    log.info("[%d/%d] %s: no results (cached, address already tried)", i + 1, len(to_resolve), item_num)
                continue

            # Skip if this address has already failed MAX_ADDRESS_RETRIES times
            if address_fail_count.get(clean_address, 0) >= MAX_ADDRESS_RETRIES:
                urls[item_num] = ""
                address_cache[clean_address] = ""
                log.info("[%d/%d] %s: skipped (address failed %d times)", i + 1, len(to_resolve), item_num, MAX_ADDRESS_RETRIES)
                continue

            try:
                search_box = page.get_by_placeholder("City, Address", exact=False).first

                # Clear previous input
                search_box.click(click_count=3)
                time.sleep(0.1)
                page.keyboard.press("Backspace")
                time.sleep(0.5)

                # Type character by character to trigger autocomplete
                search_box.press_sequentially(clean_address, delay=30)

                # Wait for autocomplete dropdown to populate
                redfin_url = ""
                for wait_attempt in range(12):  # up to ~6 seconds
                    time.sleep(0.5)
                    # Look for autocomplete suggestion links with /home/ in href
                    # These are INSIDE the autocomplete dropdown, not the page body
                    try:
                        links = page.evaluate("""
                            () => {
                                const results = [];
                                // Redfin autocomplete renders links in a listbox/suggestions area
                                const allLinks = document.querySelectorAll('a[href*="/home/"]');
                                for (const a of allLinks) {
                                    // Only get links that are in the autocomplete area (visible, near searchbox)
                                    const parent = a.closest('[role="listbox"], [class*="AutoComplete"], [class*="autocomplete"], [class*="suggestion"]');
                                    if (parent || a.closest('[class*="SearchMenu"]')) {
                                        results.push(a.getAttribute('href'));
                                    }
                                }
                                return results;
                            }
                        """)
                        if links:
                            href = links[0]
                            redfin_url = href if href.startswith("http") else f"{REDFIN_BASE}{href}"
                            break
                    except Exception:
                        pass

                # Fallback: try API response interception
                if not redfin_url and autocomplete_responses:
                    data = autocomplete_responses[-1]
                    payload = data.get("payload", {})
                    sections = payload.get("sections", [])
                    for section in sections:
                        for row in section.get("rows", []):
                            row_url = row.get("url", "")
                            if "/home/" in row_url:
                                redfin_url = f"{REDFIN_BASE}{row_url}" if not row_url.startswith("http") else row_url
                                break
                        if redfin_url:
                            break

                # Clear autocomplete for next round
                autocomplete_responses.clear()

                urls[item_num] = redfin_url
                if redfin_url:
                    address_cache[clean_address] = redfin_url
                    log.info("[%d/%d] %s -> %s", i + 1, len(to_resolve), item_num, redfin_url)
                else:
                    address_fail_count[clean_address] = address_fail_count.get(clean_address, 0) + 1
                    fails = address_fail_count[clean_address]
                    if fails >= MAX_ADDRESS_RETRIES:
                        address_cache[clean_address] = ""  # Cache the failure so future dupes skip instantly
                    log.warning("[%d/%d] %s: no results for '%s' (attempt %d/%d)",
                                i + 1, len(to_resolve), item_num, address, fails, MAX_ADDRESS_RETRIES)

                # Clear search for next iteration
                search_box.fill("")
                time.sleep(0.3)

            except Exception as exc:
                urls[item_num] = ""
                log.warning("[%d/%d] %s: error - %s", i + 1, len(to_resolve), item_num, exc)

            # Save progress periodically
            if (i + 1) % 25 == 0:
                save_resolved_urls(urls)

        browser.close()

    save_resolved_urls(urls)
    resolved_count = sum(1 for v in urls.values() if v)
    log.info("Resolved %d/%d addresses to Redfin URLs", resolved_count, len(urls))
    return urls


# ---------------------------------------------------------------------------
# Phase 2: Fetch and parse Redfin property pages
# ---------------------------------------------------------------------------


def create_session() -> requests.Session:
    """Create HTTP session for Redfin page fetching."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def fetch_redfin_data(session: requests.Session, redfin_url: str) -> dict:
    """Fetch a Redfin property page and extract all available data."""
    result = {}

    try:
        resp = session.get(redfin_url, timeout=30)
        if resp.status_code != 200:
            log.warning("Redfin returned %d for %s", resp.status_code, redfin_url)
            return result
    except requests.RequestException as exc:
        log.warning("Failed to fetch %s: %s", redfin_url, exc)
        return result

    raw = resp.text
    soup = BeautifulSoup(raw, "html.parser")

    result["redfin_url"] = redfin_url

    # --- 1. JSON-LD structured data (most reliable) ---
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(data, dict) or "mainEntity" not in data:
            continue

        entity = data["mainEntity"]
        offers = data.get("offers", {})

        # NOTE: offers.price is the LAST SOLD price for off-market properties
        # (availability=OutOfStock) or the listing price for active ones.
        # It is NOT the Redfin Estimate. Use predictedValue from embedded JSON
        # for the estimate (step 3 below). Capture the last-sale price here
        # as a fallback only.
        price = offers.get("price")
        availability = offers.get("availability", "")
        if price and "OutOfStock" in availability:
            # Off-market: offers.price is the last sold price
            result.setdefault("last_sale_price", Decimal(str(price)))

        # Property type from @type (SingleFamilyResidence, Apartment, etc.)
        entity_type = entity.get("@type", "")
        result["redfin_property_type"] = entity.get("accommodationCategory", entity_type)

        # Classify use type
        res_types = {"SingleFamilyResidence", "Apartment", "House", "Townhouse"}
        if entity_type in res_types or "Residential" in result.get("redfin_property_type", ""):
            result["use_type"] = "Residential"
        elif "Commercial" in result.get("redfin_property_type", ""):
            result["use_type"] = "Commercial"
        else:
            result["use_type"] = result.get("redfin_property_type", "Other")

        # Basic facts
        result["bedrooms"] = entity.get("numberOfBedrooms")
        result["bathrooms"] = entity.get("numberOfBathroomsTotal")
        result["year_built"] = entity.get("yearBuilt")

        # Square footage
        floor_size = entity.get("floorSize", {})
        if isinstance(floor_size, dict) and floor_size.get("value"):
            result["sqft"] = int(floor_size["value"])

        # GPS
        geo = entity.get("geo", {})
        if geo:
            result["latitude"] = geo.get("latitude")
            result["longitude"] = geo.get("longitude")

    # --- 2. "About this home" text (lot size, description) ---
    about_match = re.search(
        r"is a ([\d,]+) square foot (\w[\w\s]*?) on a ([\d,]+) square foot lot",
        raw,
    )
    if about_match:
        result.setdefault("sqft", parse_int(about_match.group(1)))
        result["lot_sqft"] = parse_int(about_match.group(3))
    else:
        # Try acre-based lot
        about_acres = re.search(
            r"is a ([\d,]+) square foot (\w[\w\s]*?) on a ([\d.]+) acre",
            raw,
        )
        if about_acres:
            result.setdefault("sqft", parse_int(about_acres.group(1)))
            acres = parse_float(about_acres.group(3))
            if acres:
                result["lot_acres"] = acres
                result["lot_sqft"] = int(acres * 43560)

    # Full about text — extract from Redfin's remarks/description section
    about_div = soup.find("div", {"data-rf-test-id": "TextBase"}) or soup.find("div", class_=re.compile(r"remarks"))
    if about_div:
        result["about_text"] = about_div.get_text(" ", strip=True)[:2000]
    else:
        # Fallback: extract description from embedded JSON
        desc_match = re.search(r'"text\\":\\"(.*?)\\"', raw)
        if desc_match:
            result["about_text"] = desc_match.group(1).replace("\\n", " ")[:2000]

    # --- 3. Embedded JSON from __reactServerState (most complete source) ---
    # Redfin embeds all property data as escaped JSON inside a <script> tag.
    # Fields appear as e.g. \"lotSize\":6534 in the raw HTML.
    embedded_fields = {
        "lotSize": r'"lotSize\\":\s*(\d+)',
        "sqFtFinished": r'"sqFtFinished\\":\s*(\d+)',
        "pricePerSqFt": r'"pricePerSqFt\\":\s*(\d+)',
        "stories": r'"stories\\":\s*(\d+)',
        "yearBuilt": r'"yearBuilt\\":\s*(\d+)',
        "numBedrooms": r'"numBedrooms\\":\s*(\d+)',
        "numBathrooms": r'"numBathrooms\\":\s*(\d+)',
        "predictedValue": r'"predictedValue\\":\s*([\d.]+)',
        "lotSqFt": r'"lotSqFt\\":\s*(\d+)',
    }
    embedded = {}
    for name, pattern in embedded_fields.items():
        m = re.search(pattern, raw)
        if m:
            # predictedValue may have a decimal component; round to integer
            try:
                embedded[name] = round(float(m.group(1)))
            except ValueError:
                embedded[name] = int(m.group(1))

    # Apply embedded data as fallbacks/supplements
    if embedded.get("lotSize") or embedded.get("lotSqFt"):
        lot = embedded.get("lotSize") or embedded.get("lotSqFt")
        result.setdefault("lot_sqft", lot)
    if embedded.get("sqFtFinished"):
        result.setdefault("sqft", embedded["sqFtFinished"])
    if embedded.get("pricePerSqFt"):
        result.setdefault("price_per_sqft", Decimal(str(embedded["pricePerSqFt"])))
    if embedded.get("stories"):
        result.setdefault("stories", embedded["stories"])
    if embedded.get("yearBuilt"):
        result.setdefault("year_built", embedded["yearBuilt"])
    if embedded.get("numBedrooms"):
        result.setdefault("bedrooms", embedded["numBedrooms"])
    if embedded.get("numBathrooms"):
        result.setdefault("bathrooms", embedded["numBathrooms"])
    if embedded.get("predictedValue"):
        # predictedValue is the authoritative Redfin Estimate — always assign
        result["redfin_estimate"] = Decimal(str(embedded["predictedValue"]))

    # --- 4. Lot size fallback from non-escaped JSON ---
    if "lot_sqft" not in result:
        lot_match = re.search(r'"lotSize":\s*(\d+)', raw)
        if lot_match:
            result["lot_sqft"] = int(lot_match.group(1))

    # Convert lot_sqft to acres if we have sqft but not acres
    if result.get("lot_sqft") and not result.get("lot_acres"):
        result["lot_acres"] = round(result["lot_sqft"] / 43560, 4)

    # --- 5. Price per sqft fallback ---
    if "price_per_sqft" not in result:
        ppsf_match = re.search(r'\$([\d,]+)\s*(?:Est\.\s*)?(?:Price/Sq\.?\s*Ft|per sq)', raw, re.I)
        if ppsf_match:
            result["price_per_sqft"] = parse_currency(ppsf_match.group(1))

    # --- 6. Stories fallback ---
    if "stories" not in result:
        stories_match = re.search(r'"stories":\s*(\d+)', raw)
        if stories_match:
            result["stories"] = int(stories_match.group(1))

    # --- 7. Street view URL ---
    sv_match = re.search(r'(https://maps\.google\.com/maps/api/staticmap[^"&]+(?:&amp;[^"]+)*)', raw)
    if sv_match:
        result["street_view_url"] = sv_match.group(1).replace("&amp;", "&")

    # Also capture Google streetview image URL if present
    sv_img = soup.find('img', src=re.compile(r'maps\.google|streetview|maps\.googleapis'))
    if sv_img:
        result["streetview_img_url"] = sv_img.get('src', '').replace("&amp;", "&")

    # --- 8. Property photos (SUBJECT PROPERTY ONLY) ---
    # Subject property photos are in the InlinePhotoPreview section (above the rail)
    # and use bigphoto/islphoto URLs. Nearby/similar home cards use mbphoto URLs
    # inside bp-Homecard divs — those must be excluded.
    subject_photos = []

    # Strategy 1: Extract from the main photo section (aboveTheRail / InlinePhotoPreview)
    above_rail = soup.find(class_=re.compile(r'aboveTheRail|InlinePhotoPreview'))
    if above_rail:
        for img in above_rail.find_all('img', src=re.compile(r'ssl\.cdn-redfin\.com/photo/')):
            src = img.get('src', '')
            if src and src not in subject_photos:
                subject_photos.append(src)
        # Also check background-image styles in the section
        for el in above_rail.find_all(style=re.compile(r'ssl\.cdn-redfin\.com/photo/')):
            style = el.get('style', '')
            urls = re.findall(r'(https://ssl\.cdn-redfin\.com/photo/[^")]+)', style)
            for u in urls:
                if u not in subject_photos:
                    subject_photos.append(u)

    # Strategy 2: Fallback — bigphoto/islphoto URLs NOT inside HomeCard sections
    if not subject_photos:
        homecard_sections = soup.find_all(class_=re.compile(r'bp-Homecard|HomeCard'))
        homecard_html = ''.join(str(s) for s in homecard_sections)
        homecard_photo_set = set(re.findall(
            r'https://ssl\.cdn-redfin\.com/photo/\d+/(?:bigphoto|islphoto|mbphoto)/\d+/[^"]+\.(?:jpg|webp)',
            homecard_html,
        ))
        all_photos = re.findall(
            r'https://ssl\.cdn-redfin\.com/photo/\d+/(?:bigphoto|islphoto)/\d+/[^"]+\.(?:jpg|webp)',
            raw,
        )
        seen = set()
        for url in all_photos:
            if url not in homecard_photo_set and url not in seen:
                seen.add(url)
                subject_photos.append(url)

    result["photos"] = subject_photos
    result["has_listing_photos"] = len(subject_photos) > 0

    # --- 9. Tax history (parsed from structured HTML table) ---
    tax_history = {}
    tax_table = soup.find("table", class_="TaxHistoryTable")
    if tax_table:
        for row in tax_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 5:
                year_text = cells[0].get_text(strip=True)
                tax_text = cells[1].get_text(strip=True)
                land_text = cells[2].get_text(strip=True)
                additions_text = cells[3].get_text(strip=True)
                assessed_text = cells[4].get_text(strip=True)

                # Strip percentage annotations e.g. "$1,398(+2.6%)"
                tax_clean = re.sub(r'\([^)]*\)', '', tax_text)
                yr = parse_int(year_text)
                if yr and 2000 <= yr <= 2030:
                    tax_history[yr] = {
                        "tax_year": yr,
                        "property_tax": parse_currency(tax_clean),
                        "land_value": parse_currency(land_text),
                        "improvements_value": parse_currency(additions_text),
                        "assessed_value": parse_currency(assessed_text),
                    }
    result["tax_history"] = sorted(tax_history.values(), key=lambda x: x["tax_year"], reverse=True)

    # --- 10. Sale history ---
    # Look for patterns like "Jul 2023 Sold $X,XXX,XXX" or "Jan 17, 1991 Sold $108,000"
    page_text = soup.get_text(" ", strip=True)
    sale_entries = re.findall(
        r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(?:\d{1,2},?\s+)?\d{4})\s*(?:Sold|Listed)\s*(?:for\s*)?\$([\d,]+)",
        page_text,
        re.I,
    )
    sale_history = []
    seen_sales = set()
    for date_str, price in sale_entries:
        date_str_clean = date_str.strip().rstrip(",")
        if date_str_clean in seen_sales:
            continue
        seen_sales.add(date_str_clean)
        sale_date = None
        for fmt in ("%b %d, %Y", "%b %d %Y", "%b %Y"):
            try:
                sale_date = datetime.strptime(date_str_clean, fmt).date()
                break
            except ValueError:
                continue
        if sale_date is None:
            continue
        sale_history.append({
            "sale_date": sale_date,
            "sale_price": parse_currency(price),
        })
    result["sale_history"] = sale_history

    # Set last sale from sale history
    if sale_history:
        latest = max(sale_history, key=lambda x: x["sale_date"])
        result["last_sale_date"] = latest["sale_date"]
        result["last_sale_price"] = latest["sale_price"]

    # Keep raw HTML for saving to disk (removed before DB insert)
    result["_raw_html"] = raw

    return result


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def init_db(db_url: str):
    """Connect to PostgreSQL and ensure schema exists."""
    import psycopg2
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    schema_sql = open("schema.sql").read()
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()
    log.info("Database initialized")
    return conn


def update_enrichment(conn, item_number: str, data: dict):
    """Update enrichment fields for a property."""
    sql = """
        UPDATE auction_properties SET
            use_type = %(use_type)s,
            redfin_property_type = %(redfin_property_type)s,
            sqft = %(sqft)s,
            lot_sqft = %(lot_sqft)s,
            lot_acres = %(lot_acres)s,
            bedrooms = %(bedrooms)s,
            bathrooms = %(bathrooms)s,
            year_built = %(year_built)s,
            stories = %(stories)s,
            redfin_estimate = %(redfin_estimate)s,
            price_per_sqft = %(price_per_sqft)s,
            redfin_url = %(redfin_url)s,
            latitude = %(latitude)s,
            longitude = %(longitude)s,
            about_text = %(about_text)s,
            street_view_url = %(street_view_url)s,
            last_sale_date = %(last_sale_date)s,
            last_sale_price = %(last_sale_price)s,
            enriched_at = NOW()
        WHERE item_number = %(item_number)s AND auction_id = 49
        RETURNING id
    """

    defaults = {
        "use_type": None, "redfin_property_type": None, "sqft": None,
        "lot_sqft": None, "lot_acres": None, "bedrooms": None, "bathrooms": None,
        "year_built": None, "stories": None, "redfin_estimate": None,
        "price_per_sqft": None, "redfin_url": None, "latitude": None,
        "longitude": None, "about_text": None, "street_view_url": None,
        "last_sale_date": None, "last_sale_price": None,
    }

    record = {**defaults, **{k: v for k, v in data.items() if k in defaults}, "item_number": item_number}

    with conn.cursor() as cur:
        cur.execute(sql, record)
        row = cur.fetchone()
        if not row:
            log.warning("No DB row found for item %s", item_number)
            return None
        property_id = row[0]

    # Insert tax history
    if data.get("tax_history"):
        for tax in data["tax_history"]:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO property_tax_history
                        (auction_property_id, tax_year, property_tax, assessed_value, land_value, improvements_value)
                    VALUES (%(pid)s, %(tax_year)s, %(property_tax)s, %(assessed_value)s, %(land_value)s, %(improvements_value)s)
                    ON CONFLICT (auction_property_id, tax_year) DO UPDATE SET
                        property_tax = EXCLUDED.property_tax,
                        assessed_value = EXCLUDED.assessed_value,
                        land_value = EXCLUDED.land_value,
                        improvements_value = EXCLUDED.improvements_value
                """, {**tax, "pid": property_id})

    # Insert sale history
    if data.get("sale_history"):
        for sale in data["sale_history"]:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO property_sale_history
                        (auction_property_id, sale_date, sale_price)
                    VALUES (%(pid)s, %(sale_date)s, %(sale_price)s)
                    ON CONFLICT (auction_property_id, sale_date) DO UPDATE SET
                        sale_price = EXCLUDED.sale_price
                """, {**sale, "pid": property_id})

    # Insert photos
    if data.get("photos"):
        for idx, photo_url in enumerate(data["photos"]):
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO property_images
                        (auction_property_id, image_url, image_type, sort_order)
                    VALUES (%(pid)s, %(url)s, 'photo', %(order)s)
                    ON CONFLICT (auction_property_id, image_url) DO NOTHING
                """, {"pid": property_id, "url": photo_url, "order": idx})

    return property_id


# ---------------------------------------------------------------------------
# Photo Downloads
# ---------------------------------------------------------------------------


def download_photos(session: requests.Session, item_number: str, photo_urls: list[str],
                    streetview_url: str = None) -> int:
    """Download property photos to photos/<item_number>/ directory.
    If no listing photos exist but a streetview URL is available, download that instead.
    Returns the number of photos successfully downloaded."""
    if not photo_urls and not streetview_url:
        return 0

    item_dir = os.path.join(PHOTOS_DIR, item_number)
    os.makedirs(item_dir, exist_ok=True)

    downloaded = 0

    # Download listing photos
    for idx, url in enumerate(photo_urls):
        # Derive filename from URL or use index
        url_filename = url.rsplit("/", 1)[-1].split("?")[0]
        if not url_filename or len(url_filename) > 100:
            ext = ".jpg"
            if ".webp" in url:
                ext = ".webp"
            url_filename = f"photo_{idx:03d}{ext}"

        filepath = os.path.join(item_dir, url_filename)

        # Skip if already downloaded
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            downloaded += 1
            continue

        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 100:
                with open(filepath, "wb") as f:
                    f.write(resp.content)
                downloaded += 1
            else:
                log.debug("Photo %d: HTTP %d (%d bytes)", idx, resp.status_code, len(resp.content))
        except requests.RequestException as exc:
            log.debug("Photo %d download failed: %s", idx, exc)

    # If no listing photos were found/downloaded, grab the streetview image
    if downloaded == 0 and streetview_url:
        sv_path = os.path.join(item_dir, "streetview.jpg")
        if os.path.exists(sv_path) and os.path.getsize(sv_path) > 0:
            downloaded = 1
        else:
            try:
                resp = session.get(streetview_url, timeout=15)
                if resp.status_code == 200 and len(resp.content) > 100:
                    with open(sv_path, "wb") as f:
                        f.write(resp.content)
                    downloaded = 1
                    log.info("  -> Saved streetview image (no listing photos)")
            except requests.RequestException as exc:
                log.debug("Streetview download failed: %s", exc)

    return downloaded


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_enrichment(item_number: str, data: dict):
    """Pretty-print enrichment data for dry-run mode."""
    print(f"\n{'='*70}")
    print(f"  Item #{item_number}")
    print(f"  Redfin: {data.get('redfin_url', 'N/A')}")
    print(f"  Type: {data.get('use_type', '?')} - {data.get('redfin_property_type', '?')}")
    print(f"  Beds: {data.get('bedrooms', '?')} | Baths: {data.get('bathrooms', '?')} | Year: {data.get('year_built', '?')}")
    print(f"  Sqft: {data.get('sqft', '?')} | Lot: {data.get('lot_sqft', '?')} sqft ({data.get('lot_acres', '?')} acres)")
    print(f"  Redfin Estimate: ${data.get('redfin_estimate', '?')} | $/sqft: ${data.get('price_per_sqft', '?')}")
    print(f"  GPS: {data.get('latitude', '?')}, {data.get('longitude', '?')}")
    if data.get("last_sale_date"):
        print(f"  Last Sale: {data['last_sale_date']} for ${data.get('last_sale_price', '?')}")
    photos = data.get("photos", [])
    print(f"  Photos: {len(photos)} images")
    if photos:
        print(f"    First: {photos[0]}")
    tax = data.get("tax_history", [])
    if tax:
        print(f"  Tax History ({len(tax)} years):")
        for t in tax[:3]:
            print(f"    {t['tax_year']}: Tax=${t['property_tax']} Assessed=${t['assessed_value']}")
    sales = data.get("sale_history", [])
    if sales:
        print(f"  Sale History ({len(sales)} records):")
        for s in sales:
            print(f"    {s['sale_date']}: ${s.get('sale_price', '?')}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Enrich auction properties with Redfin data")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL, help="PostgreSQL URL")
    parser.add_argument("--dry-run", action="store_true", help="Print data without saving")
    parser.add_argument("--resolve-only", action="store_true", help="Only resolve URLs (Phase 1)")
    parser.add_argument("--fetch-only", action="store_true", help="Only fetch data (Phase 2)")
    parser.add_argument("--items", default=None, help="Comma-separated item numbers")
    parser.add_argument("--limit", type=int, default=None, help="Process first N properties")
    parser.add_argument("--delay", type=float, default=FETCH_DELAY, help="Delay between fetches")
    parser.add_argument("--skip-resolved", action="store_true", help="Skip already-resolved items")
    args = parser.parse_args()

    # Load properties
    properties = load_properties_from_file()

    # Filter to specific items
    if args.items:
        item_set = set(args.items.split(","))
        properties = [p for p in properties if p["item_number"] in item_set]

    # Only process available (non-canceled) properties by default
    properties = [p for p in properties if p["status"] == "Upcoming"]

    if args.limit:
        properties = properties[: args.limit]

    log.info("Processing %d properties", len(properties))

    # --- Phase 1: Resolve URLs ---
    if not args.fetch_only:
        existing_urls = load_resolved_urls()

        if args.skip_resolved:
            to_resolve = [p for p in properties if p["item_number"] not in existing_urls]
        else:
            to_resolve = properties

        if to_resolve:
            urls = resolve_via_playwright(to_resolve, existing_urls)
        else:
            urls = existing_urls
            log.info("All properties already resolved")

        if args.resolve_only:
            resolved = sum(1 for v in urls.values() if v)
            log.info("Phase 1 complete. %d/%d resolved.", resolved, len(urls))
            return
    else:
        urls = load_resolved_urls()

    # --- Phase 2: Fetch Redfin data ---
    session = create_session()
    conn = None
    if not args.dry_run:
        try:
            import psycopg2
            conn = init_db(args.db_url)
        except Exception as exc:
            log.error("Database connection failed: %s", exc)
            sys.exit(1)

    enriched = 0
    skipped = 0
    errors = 0

    for i, prop in enumerate(properties):
        item_num = prop["item_number"]
        redfin_url = urls.get(item_num, "")

        if not redfin_url:
            log.info("[%d/%d] %s: no Redfin URL, skipping", i + 1, len(properties), item_num)
            skipped += 1
            continue

        log.info("[%d/%d] %s: fetching %s", i + 1, len(properties), item_num, redfin_url)

        try:
            data = fetch_redfin_data(session, redfin_url)
        except Exception as exc:
            log.error("[%d/%d] %s: error - %s", i + 1, len(properties), item_num, exc)
            errors += 1
            continue

        # Save raw HTML for potential re-processing
        raw_dir = os.path.join("raw_html", item_num)
        os.makedirs(raw_dir, exist_ok=True)
        raw_path = os.path.join(raw_dir, "redfin.html")
        if data.get("_raw_html"):
            with open(raw_path, "w", encoding="utf-8") as f:
                f.write(data.pop("_raw_html"))

        # Download photos to disk
        photos = data.get("photos", [])
        streetview_url = data.get("streetview_img_url") or data.get("street_view_url")
        dl_count = download_photos(session, item_num, photos, streetview_url=streetview_url)
        if dl_count:
            label = "listing photos" if photos else "streetview"
            log.info("  -> Downloaded %d %s to %s/%s/", dl_count, label, PHOTOS_DIR, item_num)

        if args.dry_run:
            print_enrichment(item_num, data)
        else:
            pid = update_enrichment(conn, item_num, data)
            if pid:
                conn.commit()
                log.info("  -> Saved (id=%d, %d photos, %d tax years)",
                         pid, len(data.get("photos", [])), len(data.get("tax_history", [])))

        enriched += 1
        time.sleep(args.delay)

    if conn:
        conn.close()

    log.info("Done. Enriched: %d, Skipped: %d, Errors: %d", enriched, skipped, errors)


if __name__ == "__main__":
    main()
