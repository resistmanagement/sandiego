#!/usr/bin/env python3
"""
Quick verification: scan all auction pages and compare against DB.
Identifies any properties showing Canceled on the site but Upcoming in DB.
"""
import math, re, sys
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://sdttc.mytaxsale.com"
AUCTION_URL = f"{BASE_URL}/auction/49"
DB_URL = "postgresql://localhost:5432/sandiego_auction"

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
})
session.get(AUCTION_URL)

def scrape_page_statuses(page):
    resp = session.get(AUCTION_URL, params={"page": page})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    items = {}
    seen = set()
    for link in soup.find_all("a", href=re.compile(r"toggle_item_details")):
        m = re.search(r"toggle_item_details\(\s*'(\d+)'\s*,\s*'(\d+)'\s*\)", link.get("href",""))
        if not m or m.group(2) in seen:
            continue
        seen.add(m.group(2))
        row = link.find_parent("tr")
        if not row:
            continue
        cells = row.find_all("td")
        if len(cells) < 6:
            continue
        item_number = cells[1].get_text(strip=True)
        status = cells[5].get_text(strip=True)
        next_row = row.find_next_sibling("tr")
        next_text = next_row.get_text(strip=True) if next_row else ""
        cancel_reason = next_text if next_text in ("WITHDRAWN", "REDEEMED") else None
        items[item_number] = (status, cancel_reason)
    return items

# Get total pages from page_desc
resp = session.get(AUCTION_URL, params={"page": 1})
soup = BeautifulSoup(resp.text, "html.parser")
page_desc = soup.find(class_="page_desc")
total_items_on_site = 0
if page_desc:
    m = re.search(r"(\d+)\s+propert", page_desc.get_text())
    if m:
        total_items_on_site = int(m.group(1))
per_page = 25
total_pages = math.ceil(total_items_on_site / per_page) if total_items_on_site else 29
print(f"Site: {total_items_on_site} properties across ~{total_pages} pages")

# Fetch DB current statuses
conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
with conn.cursor() as cur:
    cur.execute("SELECT item_number, status, cancel_reason FROM auction_properties")
    db_rows = {r["item_number"]: (r["status"], r["cancel_reason"]) for r in cur.fetchall()}
conn.close()
print(f"DB: {len(db_rows)} properties total\n")

# Scan all pages
all_site_items = {}
for page in range(1, total_pages + 1):
    items = scrape_page_statuses(page)
    all_site_items.update(items)
    sys.stdout.write(f"\rScanned page {page}/{total_pages} ({len(all_site_items)} items so far)...")
    sys.stdout.flush()
print(f"\nDone. Found {len(all_site_items)} items on site.\n")

# Find mismatches
new_cancellations = []
for item_number, (site_status, site_reason) in all_site_items.items():
    db_status, db_reason = db_rows.get(item_number, ("MISSING", None))
    if site_status == "Canceled" and db_status != "Canceled":
        new_cancellations.append((item_number, site_status, site_reason, db_status, db_reason))

if new_cancellations:
    print(f"Properties now Canceled on site but not updated in DB ({len(new_cancellations)}):")
    for item, ss, sr, ds, dr in sorted(new_cancellations):
        print(f"  #{item:>6}  site=Canceled/{sr:<10}  db={ds}/{dr}")
else:
    print("No discrepancies found — DB is already up to date.")

# Also check anything in DB not seen on site
missing_from_site = [item for item in db_rows if item not in all_site_items]
if missing_from_site:
    print(f"\nProperties in DB but NOT found on site ({len(missing_from_site)}): {sorted(missing_from_site)}")

