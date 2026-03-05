#!/usr/bin/env python3
"""Re-fetch detail page fields (APN, property type, assessed value, etc.)
for all properties where those fields are NULL in the DB."""

import sys
import time
import logging

import psycopg2
import requests

# Import helpers directly from scraper
sys.path.insert(0, ".")
from scraper import fetch_item_details, DEFAULT_DB_URL, DETAIL_DELAY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

UPDATE_SQL = """
    UPDATE auction_properties SET
        apn                  = COALESCE(%s, apn),
        property_type        = COALESCE(%s, property_type),
        address              = COALESCE(%s, address),
        city                 = COALESCE(%s, city),
        postal_code          = COALESCE(%s, postal_code),
        tax_rate_area        = COALESCE(%s, tax_rate_area),
        land_value           = COALESCE(%s, land_value),
        improvements         = COALESCE(%s, improvements),
        total_assessed_value = COALESCE(%s, total_assessed_value),
        assessed_value_year  = COALESCE(%s, assessed_value_year),
        property_description = COALESCE(%s, property_description),
        timeshare_association= COALESCE(%s, timeshare_association),
        default_year         = COALESCE(%s, default_year),
        assessee             = COALESCE(%s, assessee),
        scraped_at           = NOW()
    WHERE id = %s
"""

def main():
    conn = psycopg2.connect(DEFAULT_DB_URL)
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, item_number, internal_id
            FROM auction_properties
            WHERE auction_id = 49 AND apn IS NULL
            ORDER BY item_number::int
        """)
        rows = cur.fetchall()

    total = len(rows)
    log.info("Fetching details for %d properties with missing data…", total)

    ok = 0
    failed = 0

    for i, (prop_id, item_number, internal_id) in enumerate(rows, 1):
        details = fetch_item_details(session, internal_id)
        if not details:
            log.warning("[%d/%d] #%s — no data returned", i, total, item_number)
            failed += 1
            time.sleep(DETAIL_DELAY)
            continue

        with conn.cursor() as cur:
            cur.execute(UPDATE_SQL, (
                details.get("apn"),
                details.get("property_type"),
                details.get("address"),
                details.get("city"),
                details.get("postal_code"),
                details.get("tax_rate_area"),
                details.get("land_value"),
                details.get("improvements"),
                details.get("total_assessed_value"),
                details.get("assessed_value_year"),
                details.get("property_description"),
                details.get("timeshare_association"),
                details.get("default_year"),
                details.get("assessee"),
                prop_id,
            ))
        conn.commit()
        ok += 1

        if i % 50 == 0:
            log.info("Progress: %d/%d (ok=%d, failed=%d)", i, total, ok, failed)

        time.sleep(DETAIL_DELAY)

    conn.close()
    log.info("Done. %d updated, %d failed.", ok, failed)

if __name__ == "__main__":
    main()
