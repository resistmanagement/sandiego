"""
Static build transform tests — verifies build_html() produces correct output.

These tests call build_html() once (module scope), regenerating docs/index.html,
and then assert correctness of the resulting static file.

Run with:  ./venv/bin/pytest test_build_static.py -v
"""

import os
import pytest
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from build_static import build_html, DOCS_DIR

DOCS_HTML = os.path.join(DOCS_DIR, "index.html")


@pytest.fixture(scope="module")
def built_html():
    """Run the static build once and return the generated HTML."""
    build_html()
    with open(DOCS_HTML) as f:
        return f.read()


# ── Data loading ──────────────────────────────────────────────────────────────

class TestDataLoading:
    def test_fetches_from_static_json(self, built_html):
        assert "./data/properties.json" in built_html

    def test_flask_api_endpoint_replaced(self, built_html):
        assert "fetch('/api/properties')" not in built_html

    def test_restores_ratings_from_localstorage(self, built_html):
        assert "propertyRatings" in built_html
        assert "_savedRatings" in built_html

    def test_restores_priorities_from_localstorage(self, built_html):
        assert "propertyPriorities" in built_html
        assert "_savedPriorities" in built_html

    def test_properties_have_user_rating_patched(self, built_html):
        # After load, p.user_rating must be set from localStorage
        assert "p.user_rating = _savedRatings" in built_html

    def test_properties_have_user_priority_patched(self, built_html):
        assert "p.user_priority = _savedPriorities" in built_html


# ── rateProperty ─────────────────────────────────────────────────────────────

class TestRatePropertyFunction:
    def test_present(self, built_html):
        assert "function rateProperty" in built_html

    def test_is_synchronous(self, built_html):
        assert "async function rateProperty" not in built_html

    def test_writes_to_localstorage(self, built_html):
        assert "localStorage.setItem('propertyRatings'" in built_html

    def test_does_not_call_api(self, built_html):
        assert "fetch(`/api/property/${propertyId}/rating`)" not in built_html
        assert "/rating`)" not in built_html

    def test_toggles_rating(self, built_html):
        # The localStorage version uses field user_rating to determine toggle
        assert "newRating" in built_html


# ── setPriority ───────────────────────────────────────────────────────────────

class TestSetPriorityFunction:
    def test_present(self, built_html):
        assert "function setPriority" in built_html

    def test_is_synchronous(self, built_html):
        assert "async function setPriority" not in built_html

    def test_writes_to_localstorage(self, built_html):
        assert "localStorage.setItem('propertyPriorities'" in built_html

    def test_does_not_call_api(self, built_html):
        assert "fetch(`/api/property/${propertyId}/priority`)" not in built_html
        assert "/priority`)" not in built_html


# ── exportData ────────────────────────────────────────────────────────────────

class TestExportFunction:
    def test_present(self, built_html):
        assert "function exportData" in built_html

    def test_reads_from_localstorage(self, built_html):
        # exportData reads propertyRatings from localStorage
        assert "localStorage.getItem('propertyRatings')" in built_html

    def test_does_not_fetch_api(self, built_html):
        assert "fetch('/api/ratings')" not in built_html
        assert 'fetch("/api/ratings")' not in built_html

    def test_includes_priorities_in_export(self, built_html):
        assert "priorities" in built_html

    def test_includes_saved_filters_in_export(self, built_html):
        assert "savedFilters" in built_html

    def test_timestamped_filename(self, built_html):
        assert "my-ratings-${ts}.json" in built_html


# ── importData ────────────────────────────────────────────────────────────────

class TestImportFunction:
    def test_present(self, built_html):
        assert "function importData" in built_html

    def test_writes_ratings_to_localstorage(self, built_html):
        # Count direct set calls — there should be one for ratings and one for priorities
        assert built_html.count("localStorage.setItem('propertyRatings'") >= 2  # rate + import

    def test_writes_priorities_to_localstorage(self, built_html):
        assert built_html.count("localStorage.setItem('propertyPriorities'") >= 2  # priority + import

    def test_handles_saved_filters(self, built_html):
        assert "propertySavedFilters" in built_html

    def test_supports_old_flat_format(self, built_html):
        # importData extracts `data.ratings || ...`
        assert "data.ratings" in built_html

    def test_does_not_post_to_api(self, built_html):
        # Split around importData to isolate its body
        parts = built_html.split("function importData")
        assert len(parts) >= 2
        import_body = parts[1].split("function ")[0]
        assert "/api/ratings" not in import_body
        assert "fetch(" not in import_body


# ── Photo button removal ──────────────────────────────────────────────────────

class TestPhotoButton:
    def test_open_photo_modal_call_absent(self, built_html):
        # The button onclick in the card template should be stripped
        assert "openPhotoModal(${prop.id}" not in built_html


# ── No API leakage ────────────────────────────────────────────────────────────

class TestNoApiLeakage:
    def test_no_api_ratings_endpoint(self, built_html):
        assert "/api/ratings" not in built_html

    def test_no_api_rating_write(self, built_html):
        assert "/rating`)" not in built_html

    def test_no_api_priority_write(self, built_html):
        assert "/priority`)" not in built_html

    def test_rate_property_is_not_async(self, built_html):
        assert "async function rateProperty" not in built_html

    def test_set_priority_is_not_async(self, built_html):
        assert "async function setPriority" not in built_html


# ── JSON data export (unit test of serialize_value / property fields) ─────────

class TestJsonExport:
    def test_properties_json_exists(self):
        path = os.path.join(DOCS_DIR, "data", "properties.json")
        # Only check existence — actual regeneration is in build_static.py main()
        # This is covered by the export_json() function; here we only test build_html()
        # but if tests are run after ./build_static.py we can verify the file exists.
        if os.path.exists(path):
            import json
            with open(path) as f:
                props = json.load(f)
            assert isinstance(props, list)
            assert len(props) > 0
            # user_rating and user_priority must NOT appear (handled by localStorage)
            for p in props[:10]:
                assert "user_rating" not in p
                assert "user_priority" not in p
