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
        # user_rating and user_priority are handled client-side via localStorage
        prop.pop("user_rating", None)
        prop.pop("user_priority", None)
        properties.append(prop)

    out = os.path.join(DATA_DIR, "properties.json")
    with open(out, "w") as f:
        json.dump(properties, f, separators=(",", ":"))

    size_kb = os.path.getsize(out) / 1024
    print(f"Exported {len(properties)} properties → {out} ({size_kb:.1f} KB)")


LOCALSTORAGE_EXPORT_FN = """\
        // Export data (static: reads from localStorage)
        function exportData() {
            const ratings = JSON.parse(localStorage.getItem('propertyRatings') || '{}');
            const priorities = JSON.parse(localStorage.getItem('propertyPriorities') || '{}');
            const savedFilters = JSON.parse(localStorage.getItem('propertySavedFilters') || '{}');
            const rCount = Object.keys(ratings).length;
            const pCount = Object.keys(priorities).length;
            if (rCount === 0 && pCount === 0) { alert('No ratings or priorities to export.'); return; }
            const ts = new Date().toISOString().slice(0,19).replace(/:/g,'-');
            downloadJSON({ ratings, priorities, savedFilters }, `my-ratings-${ts}.json`);
            document.getElementById('ratings-info').textContent = `Exported ${rCount} rating${rCount !== 1 ? 's' : ''}, ${pCount} priorit${pCount !== 1 ? 'ies' : 'y'}`;
        }

        // Shim for old exportRatings calls
        function exportRatings() { return exportData(); }"""

LOCALSTORAGE_IMPORT_FN = """\
        // Import data (static: writes to localStorage)
        async function importData(file) {
            if (!file) return;
            try {
                const data = JSON.parse(await file.text());
                const ratings = data.ratings || (typeof data === 'object' && !data.savedFilters ? data : {});
                const priorities = data.priorities || {};
                const savedFilters = data.savedFilters || {};

                // Merge ratings
                const savedR = JSON.parse(localStorage.getItem('propertyRatings') || '{}');
                Object.assign(savedR, ratings);
                localStorage.setItem('propertyRatings', JSON.stringify(savedR));

                // Merge priorities
                const savedP = JSON.parse(localStorage.getItem('propertyPriorities') || '{}');
                Object.assign(savedP, priorities);
                localStorage.setItem('propertyPriorities', JSON.stringify(savedP));

                // Merge saved filters
                if (Object.keys(savedFilters).length > 0) {
                    const existing = JSON.parse(localStorage.getItem('propertySavedFilters') || '{}');
                    Object.assign(existing, savedFilters);
                    localStorage.setItem('propertySavedFilters', JSON.stringify(existing));
                    populateSavedFilterDropdown();
                }

                allProperties.forEach(p => {
                    const id = String(p.id);
                    if (savedR[id]) p.user_rating = savedR[id];
                    if (savedP[id]) p.user_priority = savedP[id];
                });
                const rCount = Object.keys(ratings).length;
                const pCount = Object.keys(priorities).length;
                document.getElementById('ratings-info').textContent = `Imported ${rCount} rating${rCount !== 1 ? 's' : ''}, ${pCount} priorit${pCount !== 1 ? 'ies' : 'y'}`;
                performSearch();
            } catch (e) {
                alert('Import failed: ' + e.message);
            }
        }

        // Shim for old importRatings calls
        async function importRatings(file) { return importData(file); }"""

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

LOCALSTORAGE_PRIORITY_FN = """\
        // Priority select function (localStorage-based for static GitHub Pages)
        function setPriority(propertyId, selectElement) {
            const priority = selectElement.value || null;

            const saved = JSON.parse(localStorage.getItem('propertyPriorities') || '{}');
            if (priority) { saved[String(propertyId)] = priority; }
            else { delete saved[String(propertyId)]; }
            localStorage.setItem('propertyPriorities', JSON.stringify(saved));

            const property = allProperties.find(p => p.id === propertyId);
            if (property) property.user_priority = priority;
            selectElement.className = 'priority-select' + (priority === 'High' ? ' pri-high' : priority === 'Medium' ? ' pri-medium' : priority === 'Low' ? ' pri-low' : '');
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
    # 2. After loading JSON, restore ratings AND priorities from localStorage
    # ------------------------------------------------------------------ #
    html = html.replace(
        "allProperties = await response.json();",
        "allProperties = await response.json();\n"
        "                // Restore ratings and priorities from localStorage\n"
        "                const _savedRatings = JSON.parse(localStorage.getItem('propertyRatings') || '{}');\n"
        "                const _savedPriorities = JSON.parse(localStorage.getItem('propertyPriorities') || '{}');\n"
        "                allProperties.forEach(p => {\n"
        "                    p.user_rating = _savedRatings[String(p.id)] || null;\n"
        "                    p.user_priority = _savedPriorities[String(p.id)] || null;\n"
        "                });",
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
    # 3b. Replace setPriority with localStorage version                   #
    # ------------------------------------------------------------------ #
    html = re.sub(
        r"        // ── Priority ─+\n        async function setPriority.*?(?=\n[ \t]*\n        // ── Export)",
        LOCALSTORAGE_PRIORITY_FN,
        html,
        flags=re.DOTALL,
    )

    # ------------------------------------------------------------------ #
    # 3c. Replace exportData/exportRatings with localStorage version      #
    # ------------------------------------------------------------------ #
    html = re.sub(
        r"        // ── Export / Import.*?(?=\n[ \t]*\n        // ── Saved Filters)",
        LOCALSTORAGE_EXPORT_FN,
        html,
        flags=re.DOTALL,
    )

    # ------------------------------------------------------------------ #
    # 3d. Replace importData/importRatings with localStorage version      #
    # ------------------------------------------------------------------ #
    # Already included in LOCALSTORAGE_EXPORT_FN block above (export+import together)
    # Remove the separate import block if still present
    html = re.sub(
        r"        // Import ratings \(Flask:.*?async function importRatings\(file\).*?\n        \}",
        "",
        html,
        flags=re.DOTALL,
    )

    # ------------------------------------------------------------------ #
    # 3e. Replace importData (Flask version) with shim note               #
    # (importData is emitted by LOCALSTORAGE_EXPORT_FN block)             #
    # ------------------------------------------------------------------ #
    html = re.sub(
        r"        // ── Export / Import .*?\n        async function importData\(file\).*?\n        \}(?=\n[ \t]*\n        // ── Saved Filters)",
        LOCALSTORAGE_EXPORT_FN,
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
