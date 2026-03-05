# San Diego County Property Tax Auction Scraper

Scrapes property auction data from the [SDTTC Tax Sale](https://sdttc.mytaxsale.com/auction/49) (March 2026) and stores it in a local PostgreSQL database.

## Data Collected

For each of the 715 auction properties:

| Field | Example |
|---|---|
| ID# | 0003 |
| Opening Bid | $68,000.00 |
| Best Bid | - |
| Close (PDT) | 3/16/26 8:00 AM |
| Status | Upcoming / Canceled |
| APN | 1030911500 |
| Property Type | Improved Property |
| Address | 919 OLIVE AVE FALLBROOK CA 92028-1560 |
| City / Postal Code | FALLBROOK / 92028 |
| Land Value | $72,491 |
| Improvements | $62,134 |
| Total Assessed Value | $134,625 |
| Default Year | 2019 |
| Assessee | MARTINEZ MIGUEL, et al |

## Setup

### 1. Prerequisites

- Python 3.10+
- PostgreSQL running locally

### 2. Create the database

```bash
createdb sandiego_auction
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

## Usage

### Scrape all properties (full run)

```bash
python scraper.py
```

This will scrape all 29 pages (~715 items) and store them in the `sandiego_auction` database. Takes roughly 10-15 minutes due to polite rate limiting.

### Dry run (preview without database)

```bash
python scraper.py --dry-run
```

### Scrape specific pages

```bash
python scraper.py --pages 1-5      # Pages 1 through 5
python scraper.py --pages 3        # Just page 3
```

### Skip fetching detail fields

```bash
python scraper.py --skip-details   # Only scrape main table data (fast)
```

### Custom database URL

```bash
python scraper.py --db-url postgresql://user:pass@localhost:5432/mydb
```

### Adjust request delay

```bash
python scraper.py --delay 0.5      # 0.5s between detail requests
```

## Re-running

The scraper uses `ON CONFLICT ... DO UPDATE`, so re-running will update existing records (e.g., to capture bid changes during the auction).

## Database Queries

```sql
-- All available (non-canceled) properties
SELECT item_number, opening_bid, address, city, status
FROM auction_properties
WHERE status = 'Upcoming'
ORDER BY opening_bid;

-- Properties under $100k
SELECT item_number, opening_bid, address, city, total_assessed_value
FROM auction_properties
WHERE status = 'Upcoming' AND opening_bid < 100000
ORDER BY opening_bid;

-- By city
SELECT city, COUNT(*) as count, MIN(opening_bid) as min_bid, MAX(opening_bid) as max_bid
FROM auction_properties
WHERE status = 'Upcoming'
GROUP BY city
ORDER BY count DESC;
```
