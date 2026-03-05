#!/usr/bin/env python3
"""List all auction properties: ID, Address, Opening Bid."""

import re
import sys
import time
from decimal import Decimal

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://sdttc.mytaxsale.com"
AUCTION_URL = f"{BASE_URL}/auction/49"


def main():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    session.get(AUCTION_URL)

    all_items = []

    # Scrape all 29 pages for main table data + internal IDs
    for page in range(1, 30):
        resp = session.get(AUCTION_URL, params={"page": page})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=re.compile(r"toggle_item_details"))
        seen = set()
        for link in links:
            m = re.search(r"toggle_item_details\(\s*'\d+'\s*,\s*'(\d+)'\s*\)", link.get("href", ""))
            if not m:
                continue
            iid = int(m.group(1))
            if iid in seen:
                continue
            seen.add(iid)
            row = link.find_parent("tr")
            if not row:
                continue
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            item_number = cells[1].get_text(strip=True)
            bid_text = cells[2].get_text(strip=True)
            status = cells[5].get_text(strip=True)
            all_items.append((item_number, bid_text, iid, status))
        print(f"  Page {page}/29 scraped ({len(seen)} items)", file=sys.stderr)
        time.sleep(0.5)

    print(f"\n  Fetching addresses for {len(all_items)} items...\n", file=sys.stderr)

    # Print header
    print(f"{'ID#':<8} {'Opening Bid':>14}  {'Status':<12} {'Address'}")
    print(f"{'---':<8} {'----------':>14}  {'------':<12} {'-------'}")

    for i, (item_number, bid_text, iid, status) in enumerate(all_items):
        # Fetch detail for address
        try:
            resp = session.get(
                f"{AUCTION_URL}/{iid}/item_details",
                params={"_": int(time.time() * 1000)},
                headers={"X-Requested-With": "XMLHttpRequest", "Referer": AUCTION_URL},
                timeout=15,
            )
            data = resp.json()
            html = data.get(f"item_details.{iid}", "")
            detail_soup = BeautifulSoup(html, "html.parser")
            address = ""
            for tr in detail_soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2 and tds[0].get_text(strip=True) == "Address:":
                    address = " ".join(tds[1].get_text().split()).strip()
                    break
        except Exception:
            address = "(error)"

        print(f"{item_number:<8} {bid_text:>14}  {status:<12} {address}")

        if (i + 1) % 50 == 0:
            print(f"  ... {i+1}/{len(all_items)} done", file=sys.stderr)
        time.sleep(0.2)

    print(f"\n  Total: {len(all_items)} properties", file=sys.stderr)


if __name__ == "__main__":
    main()
