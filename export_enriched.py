#!/usr/bin/env python3
"""
Export Redfin-enriched property data from the local database to a JSON
snapshot file that is committed to the repo and applied in CI.

Run this locally whenever enriched data changes:

    python export_enriched.py

The output is data/enriched.json and should be committed to git.
CI then runs apply_enriched.py to load this snapshot into the
ephemeral database before building the static site.
"""
import json
import os
from datetime import datetime, date
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = os.environ.get("DB_URL", "postgresql://localhost:5432/sandiego_auction")
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "enriched.json")

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

    # Enriched fields for each property (only rows that have been enriched)
    cols = ", ".join(["item_number"] + ENRICHED_COLS)
    cur.execute(
        f"SELECT {cols} FROM auction_properties WHERE enriched_at IS NOT NULL ORDER BY item_number"
    )
    props = [{k: serialize(v) for k, v in row.items()} for row in cur.fetchall()]

    # Tax history
    cur.execute("""
        SELECT p.item_number, t.tax_year, t.property_tax, t.assessed_value,
               t.land_value, t.improvements_value
          FROM property_tax_history t
          JOIN auction_properties p ON p.id = t.auction_property_id
         WHERE p.enriched_at IS NOT NULL
         ORDER BY p.item_number, t.tax_year
    """)
    tax_rows = [{k: serialize(v) for k, v in row.items()} for row in cur.fetchall()]

    # Sale history
    cur.execute("""
        SELECT p.item_number, s.sale_date, s.sale_price, s.buyer
          FROM property_sale_history s
          JOIN auction_properties p ON p.id = s.auction_property_id
         WHERE p.enriched_at IS NOT NULL
         ORDER BY p.item_number, s.sale_date
    """)
    sale_rows = [{k: serialize(v) for k, v in row.items()} for row in cur.fetchall()]

    cur.close()
    conn.close()

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    snapshot = {
        "exported_at": datetime.utcnow().isoformat(),
        "properties": props,
        "tax_history": tax_rows,
        "sale_history": sale_rows,
    }
    with open(OUT_PATH, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(
        f"Exported {len(props)} enriched properties, "
        f"{len(tax_rows)} tax records, "
        f"{len(sale_rows)} sale records → {OUT_PATH}"
    )


if __name__ == "__main__":
    main()
