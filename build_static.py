#!/usr/bin/env python3
"""
Build static site for GitHub Pages deployment.

Exports the PostgreSQL database to docs/data/properties.json and
generates docs/index.html from the Flask template with these changes:
  - Data loaded from ./data/properties.json instead of /api/properties
  - Ratings stored in localStorage instead of via POST /api/.../rating
  - Photo viewer button removed (photos not deployed to GitHub Pages)
"""
import json
import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
from decimal import Decimal

DB_URL = "postgresql://localhost:5432/sandiego_auction"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOCS_DIR = os.path.join(BASE_DIR, "docs")
DATA_DIR = os.path.join(DOCS_DIR, "data")
TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "index.html")


def serialize_value(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def export_json():
    """Query the database and write docs/data/properties.json."""
    os.makedirs(DATA_DIR, exist_ok=True)

    conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT p.*,
               COUNT(DISTINCT i.id) AS photo_count,
               COUNT(DISTINCT t.id) AS tax_years,
               COUNT(DISTINCT s.id) AS sale_count
          FROM auction_properties p
          LEFT JOIN property_images i
                 ON p.id = i.auction_property_id AND i.image_type = 'photo'
          LEFT JOIN property_tax_history t ON p.id = t.auction_property_id
          LEFT JOIN property_sale_history s ON p.id = s.auction_property_id
         GROUP BY p.id
         ORDER BY p.item_number
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    properties = []
    for row in rows:
        prop = {k: serialize_value(v) for k, v in row.items()}
        # user_rating is handled client-side via localStorage
        prop.pop("user_rating", None)
        properties.append(prop)

    out = os.path.join(DATA_DIR, "properties.json")
    with open(out, "w") as f:
        json.dump(properties, f, separators=(",", ":"))

    size_kb = os.path.getsize(out) / 1024
    print(f"Exported {len(properties)} properties → {out} ({size_kb:.1f} KB)")


LOCALSTORAGE_EXPORT_FN = """\
        // Export ratings (static: reads from localStorage)
        function exportRatings() {
            const ratings = JSON.parse(localStorage.getItem('propertyRatings') || '{}');
            const count = Object.keys(ratings).length;
            if (count === 0) { alert('No ratings to export.'); return; }
            downloadJSON(ratings, 'my-ratings.json');
            document.getElementById('ratings-info').textContent = `Exported ${count} rating${count !== 1 ? 's' : ''}`;
        }"""

LOCALSTORAGE_IMPORT_FN = """\
        // Import ratings (static: writes to localStorage)
        async function importRatings(file) {
            if (!file) return;
            try {
                const ratings = JSON.parse(await file.text());
                const saved = JSON.parse(localStorage.getItem('propertyRatings') || '{}');
                Object.assign(saved, ratings);
                localStorage.setItem('propertyRatings', JSON.stringify(saved));
                allProperties.forEach(p => {
                    if (saved[String(p.id)]) p.user_rating = saved[String(p.id)];
                });
                const count = Object.keys(ratings).length;
                document.getElementById('ratings-info').textContent = `Imported ${count} rating${count !== 1 ? 's' : ''}`;
                performSearch();
            } catch (e) {
                alert('Import failed: ' + e.message);
            }
        }"""

LOCALSTORAGE_RATE_FN = """\
        // Rating function (localStorage-based for static GitHub Pages)
        function rateProperty(propertyId, rating, buttonElement) {
            const card = buttonElement.closest('.property-card');
            const buttons = card.querySelectorAll('.rating-btn');
            const property = allProperties.find(p => p.id === propertyId);
            const newRating = property?.user_rating === rating ? null : rating;

            // Persist in localStorage
            const saved = JSON.parse(localStorage.getItem('propertyRatings') || '{}');
            if (newRating) { saved[String(propertyId)] = newRating; }
            else { delete saved[String(propertyId)]; }
            localStorage.setItem('propertyRatings', JSON.stringify(saved));

            if (property) property.user_rating = newRating;
            buttons.forEach(b => b.classList.remove('active-thumbs-up', 'active-thumbs-down'));
            if (newRating === 'thumbs_up') buttonElement.classList.add('active-thumbs-up');
            else if (newRating === 'thumbs_down') buttonElement.classList.add('active-thumbs-down');
            performSearch();
        }"""


def build_html():
    """Generate docs/index.html from templates/index.html with static modifications."""
    with open(TEMPLATE_PATH) as f:
        html = f.read()

    # ------------------------------------------------------------------ #
    # 1. Load properties from static JSON instead of Flask API            #
    # ------------------------------------------------------------------ #
    html = html.replace(
        "const response = await fetch('/api/properties');",
        "const response = await fetch('./data/properties.json');",
    )

    # ------------------------------------------------------------------ #
    # 2. After loading JSON, restore ratings from localStorage            #
    # ------------------------------------------------------------------ #
    html = html.replace(
        "allProperties = await response.json();",
        "allProperties = await response.json();\n"
        "                // Restore ratings from localStorage\n"
        "                const _savedRatings = JSON.parse(localStorage.getItem('propertyRatings') || '{}');\n"
        "                allProperties.forEach(p => { p.user_rating = _savedRatings[String(p.id)] || null; });",
    )

    # ------------------------------------------------------------------ #
    # 3. Replace async API-based rateProperty with localStorage version   #
    # ------------------------------------------------------------------ #
    html = re.sub(
        r"        // Rating function\n        async function rateProperty.*?(?=\n[ \t]*\n        // Modal functions)",
        LOCALSTORAGE_RATE_FN,
        html,
        flags=re.DOTALL,
    )

    # ------------------------------------------------------------------ #
    # 3b. Replace exportRatings with localStorage version                 #
    # ------------------------------------------------------------------ #
    html = re.sub(
        r"        // Export ratings \(Flask:.*?\n        async function exportRatings\(\).*?\n        \}",
        LOCALSTORAGE_EXPORT_FN,
        html,
        flags=re.DOTALL,
    )

    # ------------------------------------------------------------------ #
    # 3c. Replace importRatings with localStorage version                 #
    # ------------------------------------------------------------------ #
    html = re.sub(
        r"        // Import ratings \(Flask:.*?\n        async function importRatings\(file\).*?\n        \}",
        LOCALSTORAGE_IMPORT_FN,
        html,
        flags=re.DOTALL,
    )

    # ------------------------------------------------------------------ #
    # 4. Remove photo viewer button (photos not deployed to GitHub Pages) #
    # ------------------------------------------------------------------ #
    # Matches: ${prop.photo_count > 0 ? `<button …</button>` : ''}
    html = re.sub(
        r"\n[ \t]*\$\{prop\.photo_count > 0 \? `[\s\S]*?` : ''\}",
        "",
        html,
    )

    # ------------------------------------------------------------------ #
    # 5. Stamp build date in subtitle                                     #
    # ------------------------------------------------------------------ #
    build_date = datetime.now().strftime("%B %d, %Y")
    html = html.replace(
        '<div class="subtitle">March 2026 •',
        f'<div class="subtitle">March 2026 • Data as of {build_date} •',
    )

    out = os.path.join(DOCS_DIR, "index.html")
    with open(out, "w") as f:
        f.write(html)
    print(f"Generated  {out}")


def main():
    os.makedirs(DOCS_DIR, exist_ok=True)

    # .nojekyll prevents GitHub Pages from processing with Jekyll
    nojekyll = os.path.join(DOCS_DIR, ".nojekyll")
    open(nojekyll, "w").close()

    export_json()
    build_html()

    print("\nBuild complete — docs/ is ready for GitHub Pages.")
    print("  Settings → Pages → Source: main branch, /docs folder")
    print("  URL: https://resistmanagement.github.io/sandiego/")


if __name__ == "__main__":
    main()
