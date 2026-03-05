#!/usr/bin/env python3
"""Reconcile CSV auction list with database and update/insert as needed."""
import csv
import re
import psycopg2
from datetime import datetime

CSV_FILE = "Auction List - properties.csv"
DB_URL = "postgresql://localhost:5432/sandiego_auction"


def parse_money(val):
    """Parse a money string like '$ 68,000.00' into a float, or None."""
    if not val or not val.strip() or val.strip() == "-":
        return None
    cleaned = re.sub(r"[$ ,]", "", val.strip())
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_close_time(val):
    """Parse close time like '03/16/2026 08:00:00' into datetime."""
    if not val or not val.strip() or val.strip() == "-":
        return None
    try:
        return datetime.strptime(val.strip(), "%m/%d/%Y %H:%M:%S")
    except ValueError:
        return None


def clean(val):
    """Strip whitespace or return None if empty/dash."""
    if not val:
        return None
    val = val.strip()
    if val in ("", "-"):
        return None
    return val


def main():
    # Read CSV
    csv_rows = {}
    with open(CSV_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            item_num = row["ID#"].strip()
            csv_rows[item_num] = row
    print(f"CSV data rows: {len(csv_rows)}")

    # Read DB
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT item_number FROM auction_properties")
    db_items = set(row[0] for row in cur.fetchall())
    print(f"DB items: {len(db_items)}")

    csv_items = set(csv_rows.keys())
    in_csv_not_db = sorted(csv_items - db_items, key=lambda x: int(x))
    in_db_not_csv = sorted(db_items - csv_items, key=lambda x: int(x))
    in_both = csv_items & db_items

    print(f"\nIn both: {len(in_both)}")
    print(f"In CSV but not DB ({len(in_csv_not_db)}): {in_csv_not_db}")
    print(f"In DB but not CSV ({len(in_db_not_csv)}): {in_db_not_csv}")

    # Show details of items in DB but not CSV
    if in_db_not_csv:
        print("\n--- Items in DB but not in CSV ---")
        for item in in_db_not_csv:
            cur.execute(
                "SELECT item_number, status, cancel_reason, address FROM auction_properties WHERE item_number = %s",
                (item,),
            )
            row = cur.fetchone()
            print(f"  {row[0]}: status={row[1]}, reason={row[2]}, addr={row[3]}")

    # Show details of items in CSV but not DB
    if in_csv_not_db:
        print("\n--- Items in CSV but not in DB ---")
        for item in in_csv_not_db:
            row = csv_rows[item]
            print(
                f"  {item}: {row['Street Address'].strip()}, "
                f"type={row['Auction Type'].strip()}, "
                f"status={row['Status'].strip()}, "
                f"canceled={row['Canceled'].strip()}"
            )

    # --- Phase 2: Check null fields in DB ---
    print("\n=== NULL field counts in DB ===")
    for col in [
        "apn", "property_type", "land_value", "improvements",
        "total_assessed_value", "property_description", "tax_rate_area",
        "assessed_value_year", "timeshare_association",
    ]:
        cur.execute(f"SELECT COUNT(*) FROM auction_properties WHERE {col} IS NULL")
        cnt = cur.fetchone()[0]
        if cnt > 0:
            print(f"  {col}: {cnt} NULLs out of 715")

    # --- Phase 3: Update all properties from CSV data ---
    print("\n=== Updating DB from CSV ===")
    updated = 0
    for item_num, row in csv_rows.items():
        apn = clean(row.get("APN"))
        property_type = clean(row.get("Auction Type"))
        address = clean(row.get("Street Address"))
        city = clean(row.get("City"))
        postal_code = clean(row.get("Postal Code"))
        tax_rate_area = clean(row.get("Tax Rate Area"))
        land_value = parse_money(row.get("Land Value"))
        improvements = parse_money(row.get("Improvements"))
        total_assessed_value = parse_money(row.get("Total Assessed Value"))
        assessed_value_year_str = clean(row.get("Assessed Value Year"))
        assessed_value_year = int(assessed_value_year_str) if assessed_value_year_str else None
        property_description = clean(row.get("Property Description"))
        timeshare_association = clean(row.get("Timeshare Association"))
        opening_bid = parse_money(row.get("Opening Bid"))
        best_bid = parse_money(row.get("Best Bid"))
        close_time = parse_close_time(row.get("Close(PDT)"))
        status_val = clean(row.get("Status"))
        cancel_reason = clean(row.get("Canceled"))

        cur.execute("""
            UPDATE auction_properties SET
                apn = COALESCE(%s, apn),
                property_type = COALESCE(%s, property_type),
                address = COALESCE(%s, address),
                city = COALESCE(%s, city),
                postal_code = COALESCE(%s, postal_code),
                tax_rate_area = COALESCE(%s, tax_rate_area),
                land_value = COALESCE(%s, land_value),
                improvements = COALESCE(%s, improvements),
                total_assessed_value = COALESCE(%s, total_assessed_value),
                assessed_value_year = COALESCE(%s, assessed_value_year),
                property_description = COALESCE(%s, property_description),
                timeshare_association = COALESCE(%s, timeshare_association),
                opening_bid = COALESCE(%s, opening_bid),
                best_bid = COALESCE(%s, best_bid),
                close_time = COALESCE(%s, close_time),
                cancel_reason = COALESCE(%s, cancel_reason)
            WHERE item_number = %s AND auction_id = 49
        """, (
            apn, property_type, address, city, postal_code, tax_rate_area,
            land_value, improvements, total_assessed_value, assessed_value_year,
            property_description, timeshare_association,
            opening_bid, best_bid, close_time, cancel_reason,
            item_num,
        ))
        if cur.rowcount > 0:
            updated += 1

    conn.commit()
    print(f"Updated {updated} properties")

    # --- Phase 4: Re-check null fields ---
    print("\n=== NULL field counts after update ===")
    for col in [
        "apn", "property_type", "land_value", "improvements",
        "total_assessed_value", "property_description", "tax_rate_area",
        "assessed_value_year", "timeshare_association",
    ]:
        cur.execute(f"SELECT COUNT(*) FROM auction_properties WHERE {col} IS NULL")
        cnt = cur.fetchone()[0]
        if cnt > 0:
            print(f"  {col}: {cnt} NULLs out of 715")

    # Print status distribution
    print("\n=== Status Distribution ===")
    cur.execute("SELECT status, COUNT(*) FROM auction_properties GROUP BY status ORDER BY COUNT(*) DESC")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")

    print("\n=== Cancel Reasons ===")
    cur.execute("SELECT cancel_reason, COUNT(*) FROM auction_properties WHERE cancel_reason IS NOT NULL GROUP BY cancel_reason ORDER BY COUNT(*) DESC")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
