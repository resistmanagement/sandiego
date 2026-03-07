#!/usr/bin/env python3
"""
Export ALL property data from the local database to a JSON snapshot that is
committed to the repo and applied in CI.

Run this locally whenever the local database changes (after scraping or
enriching):

    python export_enriched.py

The output is data/enriched.json and should be committed to git.  CI then
runs apply_enriched.py which:
  1. INSERTs any properties not yet seen by the live scraper (ON CONFLICT
     DO NOTHING), preserving the 115+ historical items no longer on the
     live auction site.
  2. UPDATEs enriched fields (Redfin data) for all enriched properties.
  3. Inserts tax + sale history.
"""
import json
import os
from datetime import datetime, date
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = os.environ.get("DB_URL", "postgresql://localhost:5432/sandiego_auction")
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "enriched.json")

# All columns except the serial PK (id) — matches schema.sql
PROPERTY_COLS = [
    "item_number", "opening_bid", "best_bid", "close_time", "status", "cancel_reason",
    "apn", "property_type", "address", "city", "postal_code", "tax_rate_area",
    "land_value", "improvements", "total_assessed_value", "assessed_value_year",
    "property_description", "timeshare_association", "default_year", "assessee",
    "internal_id", "auction_id", "scraped_at",
    "use_type", "redfin_property_type", "sqft", "lot_sqft", "lot_acres",
    "bedrooms", "bathrooms", "year_built", "stories", "redfin_estimate",
    "price_per_sqft", "redfin_url", "latitude", "longitude",
    "about_text", "street_view_url",
    "last_sale_date", "last_sale_price", "last_sale_buyer", "enriched_at",
]

ENRICHED_COLS = [
    "use_type", "redfin_property_type", "sqft", "lot_sqft", "lot_acres",
    "bedrooms", "bathrooms", "year_built", "stories", "redfin_estimate",
    "price_per_sqft", "redfin_url", "latitude", "longitude",
    "about_text", "street_view_url",
    "last_sale_date", "last_sale_price", "last_sale_buyer", "enriched_at",
]


def serialize(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def main():
    conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
    cur = conn.cursor()

    # All properties — full row data
    cols = ", ".join(PROPERTY_COLS)
    cur.execute(f"SELECT {cols} FROM auction_properties ORDER BY item_number")
    props = [{k: serialize(v) for k, v in row.items()} for row in cur.fetchall()]

    # Tax history — all rows
    cur.execute("""
        SELECT p.item_number, t.tax_year, t.property_tax, t.assessed_value,
               t.land_value, t.improvements_value
          FROM property_tax_history t
          JOIN auction_properties p ON p.id = t.auction_property_id
         ORDER BY p.item_number, t.tax_year
    """)
    tax_rows = [{k: serialize(v) for k, v in row.items()} for row in cur.fetchall()]

    # Sale history — all rows
    cur.execute("""
        SELECT p.item_number, s.sale_date, s.sale_price, s.buyer
          FROM property_sale_history s
          JOIN auction_properties p ON p.id = s.auction_property_id
         ORDER BY p.item_number, s.sale_date
    """)
    sale_rows = [{k: serialize(v) for k, v in row.items()} for row in cur.fetchall()]

    cur.close()
    conn.close()

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    snapshot = {
        "exported_at": datetime.now(datetime.UTC if hasattr(datetime, 'UTC') else __import__('datetime').timezone.utc).isoformat(),
        "properties": props,
        "tax_history": tax_rows,
        "sale_history": sale_rows,
    }
    with open(OUT_PATH, "w") as f:
        json.dump(snapshot, f, indent=2)

    enriched_count = sum(1 for p in props if p.get("enriched_at"))
    print(
        f"Exported {len(props)} properties ({enriched_count} enriched), "
        f"{len(tax_rows)} tax records, "
        f"{len(sale_rows)} sale records → {OUT_PATH}"
    )


if __name__ == "__main__":
    main()
