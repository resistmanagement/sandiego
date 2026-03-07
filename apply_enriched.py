#!/usr/bin/env python3
"""
Apply the full property snapshot (data/enriched.json) to the database.

Used in CI after the main scraper has populated base property records:

    python apply_enriched.py
    python apply_enriched.py --db-url postgresql://user:pass@host/db

Step 1 — INSERT any property rows not already inserted by the live scraper
         (ON CONFLICT DO NOTHING), so historical items no longer on the live
         auction site are preserved in the output.
Step 2 — UPDATE enriched (Redfin) fields on all properties that have been
         enriched locally.
Step 3 — Insert tax + sale history (ON CONFLICT DO NOTHING).
"""
import argparse
import json
import os

import psycopg2
import psycopg2.extras

DEFAULT_DB_URL = os.environ.get("DB_URL", "postgresql://localhost:5432/sandiego_auction")
SNAPSHOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "enriched.json")

# Columns used in the INSERT (everything except serial PK)
INSERT_COLS = [
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    args = parser.parse_args()

    with open(SNAPSHOT_PATH) as f:
        snapshot = json.load(f)

    conn = psycopg2.connect(args.db_url)
    cur = conn.cursor()

    # Step 1 — INSERT snapshot rows that the live scraper hasn't seen
    col_list = ", ".join(INSERT_COLS)
    val_list = ", ".join(f"%({c})s" for c in INSERT_COLS)
    insert_sql = f"""
        INSERT INTO auction_properties ({col_list})
        VALUES ({val_list})
        ON CONFLICT (auction_id, item_number) DO NOTHING
    """
    inserted = 0
    for prop in snapshot["properties"]:
        cur.execute(insert_sql, prop)
        inserted += cur.rowcount

    # Step 2 — UPDATE enriched fields on all enriched properties
    set_clause = ", ".join(f"{col} = %({col})s" for col in ENRICHED_COLS)
    update_sql = f"""
        UPDATE auction_properties
           SET {set_clause}
         WHERE item_number = %(item_number)s
           AND %(enriched_at)s IS NOT NULL
    """
    updated = 0
    for prop in snapshot["properties"]:
        if prop.get("enriched_at"):
            cur.execute(update_sql, prop)
            updated += cur.rowcount

    # Step 3 — Tax history
    tax_inserted = 0
    for row in snapshot["tax_history"]:
        cur.execute("""
            INSERT INTO property_tax_history
                   (auction_property_id, tax_year, property_tax, assessed_value,
                    land_value, improvements_value)
            SELECT p.id, %(tax_year)s, %(property_tax)s, %(assessed_value)s,
                   %(land_value)s, %(improvements_value)s
              FROM auction_properties p
             WHERE p.item_number = %(item_number)s
            ON CONFLICT (auction_property_id, tax_year) DO NOTHING
        """, row)
        tax_inserted += cur.rowcount

    # Sale history
    sale_inserted = 0
    for row in snapshot["sale_history"]:
        cur.execute("""
            INSERT INTO property_sale_history
                   (auction_property_id, sale_date, sale_price, buyer)
            SELECT p.id, %(sale_date)s, %(sale_price)s, %(buyer)s
              FROM auction_properties p
             WHERE p.item_number = %(item_number)s
            ON CONFLICT (auction_property_id, sale_date) DO NOTHING
        """, row)
        sale_inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"Applied: {inserted} properties inserted (historical), "
        f"{updated} enriched, "
        f"{tax_inserted} tax records, "
        f"{sale_inserted} sale records"
    )


if __name__ == "__main__":
    main()
