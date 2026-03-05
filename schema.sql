-- San Diego County Property Tax Auction Database Schema
-- Auction: https://sdttc.mytaxsale.com/auction/49

CREATE TABLE IF NOT EXISTS auction_properties (
    -- Internal surrogate key
    id                  SERIAL PRIMARY KEY,

    -- Auction table header fields
    item_number         VARCHAR(10)   NOT NULL,          -- ID# e.g. "0003"
    opening_bid         NUMERIC(12,2),                   -- Opening Bid
    best_bid            NUMERIC(12,2),                   -- Best Bid (NULL if "-")
    close_time          TIMESTAMP,                       -- Close(PDT)
    status              VARCHAR(30),                     -- Status: Upcoming, Canceled, Sold, etc.
    cancel_reason       VARCHAR(30),                     -- WITHDRAWN, REDEEMED (if canceled)

    -- Detail fields
    apn                 VARCHAR(20),                     -- Assessor Parcel Number
    property_type       VARCHAR(50),                     -- Improved Property, Unimproved, Timeshare
    address             VARCHAR(200),                    -- Full address line
    city                VARCHAR(100),
    postal_code         VARCHAR(10),
    tax_rate_area       VARCHAR(20),
    land_value          NUMERIC(12,2),
    improvements        NUMERIC(12,2),
    total_assessed_value NUMERIC(12,2),
    assessed_value_year INTEGER,
    property_description TEXT,
    timeshare_association VARCHAR(200),
    default_year        INTEGER,
    assessee            VARCHAR(200),

    -- Metadata
    internal_id         INTEGER,                         -- Site internal ID (e.g. 18433)
    auction_id          INTEGER DEFAULT 49,              -- Auction ID on the site
    scraped_at          TIMESTAMP DEFAULT NOW(),

    -- Enriched fields (from Redfin / public records)
    use_type            VARCHAR(100),                    -- Residential, Commercial, etc.
    redfin_property_type VARCHAR(100),                   -- Single Family Residential, Condo, etc.
    sqft                INTEGER,                         -- Improved square footage
    lot_sqft            INTEGER,                         -- Lot size in sq ft
    lot_acres           NUMERIC(10,4),                   -- Lot size in acres
    bedrooms            INTEGER,
    bathrooms           NUMERIC(4,1),
    year_built          INTEGER,
    stories             INTEGER,
    redfin_estimate     NUMERIC(12,2),                   -- Redfin estimated value
    price_per_sqft      NUMERIC(10,2),                   -- Estimated $/sqft
    redfin_url          TEXT,                             -- Redfin property URL
    latitude            NUMERIC(10,7),
    longitude           NUMERIC(10,7),
    about_text          TEXT,                             -- Redfin "About this home" blurb
    street_view_url     TEXT,                             -- Google Maps street view image URL

    -- Last sale info
    last_sale_date      DATE,
    last_sale_price     NUMERIC(12,2),
    last_sale_buyer     VARCHAR(200),

    enriched_at         TIMESTAMP,                       -- When enrichment data was fetched

    -- Prevent duplicate inserts
    UNIQUE(auction_id, item_number)
);

-- Tax history table (past 3+ years)
CREATE TABLE IF NOT EXISTS property_tax_history (
    id                  SERIAL PRIMARY KEY,
    auction_property_id INTEGER REFERENCES auction_properties(id) ON DELETE CASCADE,
    tax_year            INTEGER NOT NULL,
    property_tax        NUMERIC(12,2),
    assessed_value      NUMERIC(12,2),
    land_value          NUMERIC(12,2),
    improvements_value  NUMERIC(12,2),
    UNIQUE(auction_property_id, tax_year)
);

-- Sale history table
CREATE TABLE IF NOT EXISTS property_sale_history (
    id                  SERIAL PRIMARY KEY,
    auction_property_id INTEGER REFERENCES auction_properties(id) ON DELETE CASCADE,
    sale_date           DATE,
    sale_price          NUMERIC(12,2),
    buyer               VARCHAR(200),
    UNIQUE(auction_property_id, sale_date)
);

-- Property images
CREATE TABLE IF NOT EXISTS property_images (
    id                  SERIAL PRIMARY KEY,
    auction_property_id INTEGER REFERENCES auction_properties(id) ON DELETE CASCADE,
    image_url           TEXT NOT NULL,
    image_type          VARCHAR(30) DEFAULT 'photo',     -- photo, street_view, map
    sort_order          INTEGER DEFAULT 0,
    UNIQUE(auction_property_id, image_url)
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_auction_properties_apn ON auction_properties(apn);
CREATE INDEX IF NOT EXISTS idx_auction_properties_status ON auction_properties(status);
CREATE INDEX IF NOT EXISTS idx_auction_properties_city ON auction_properties(city);
CREATE INDEX IF NOT EXISTS idx_auction_properties_postal_code ON auction_properties(postal_code);
CREATE INDEX IF NOT EXISTS idx_auction_properties_opening_bid ON auction_properties(opening_bid);
