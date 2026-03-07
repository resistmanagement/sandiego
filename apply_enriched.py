#!/usr/bin/env python3
"""
Apply the enriched data snapshot (data/enriched.json) to the database.

Used in CI after the main scraper has populated the base property records:

    python apply_enriched.py
    python apply_enriched.py --db-url postgresql://user:pass@host/db

Properties are matched by item_number. Rows not present in the
snapshot (not yet enriched locally) are left untouched.
"""
import argparse
import json
import os

import psycopg2
import psycopg2.extras

DEFAULT_DB_URL = os.environ.get("DB_URL", "postgresql://localhost:5432/sandiego_auction")
SNAPSHOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "enriched.json")

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

    # Update enriched fields on existing properties matched by item_number
    set_clause = ", ".join(f"{col} = %({col})s" for col in ENRICHED_COLS)
    update_sql = f"""
        UPDATE auction_properties
           SET {set_clause}
         WHERE item_number = %(item_number)s
    """
    updated = 0
    for prop in snapshot["properties"]:
        cur.execute(update_sql, prop)
        updated += cur.rowcount

    # Tax history
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
        f"Applied: {updated} properties updated, "
        f"{tax_inserted} tax records, "
        f"{sale_inserted} sale records"
    )


if __name__ == "__main__":
    main()
