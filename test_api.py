"""
Flask API tests — ratings, priorities, bulk import/export, properties, stats.

Run with:  ./venv/bin/pytest test_api.py -v
"""

import json
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor

from app import app as flask_app

DB_URL = "postgresql://localhost:5432/sandiego_auction"

# ── helpers ──────────────────────────────────────────────────────────────────

def db_conn():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


def get_prop_state(conn, prop_id):
    cur = conn.cursor()
    cur.execute(
        "SELECT user_rating, user_priority FROM auction_properties WHERE id = %s",
        (prop_id,),
    )
    row = cur.fetchone()
    cur.close()
    return dict(row) if row else None


def set_prop_state(conn, prop_id, rating, priority):
    cur = conn.cursor()
    cur.execute(
        "UPDATE auction_properties SET user_rating = %s, user_priority = %s WHERE id = %s",
        (rating, priority, prop_id),
    )
    conn.commit()
    cur.close()


# ── fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as c:
        yield c


@pytest.fixture
def db():
    conn = db_conn()
    yield conn
    conn.close()


@pytest.fixture
def clean_props(db):
    """
    Save, clear, and restore user_rating + user_priority for test properties 3 & 4.
    (IDs 3 and 4 have existing ratings/priorities that tests need a clean slate for.)
    """
    saved = {pid: get_prop_state(db, pid) for pid in (3, 4)}
    set_prop_state(db, 3, None, None)
    set_prop_state(db, 4, None, None)
    yield
    for pid, state in saved.items():
        set_prop_state(db, pid, state["user_rating"], state["user_priority"])


@pytest.fixture
def prop5_clean(db):
    """Save/clear/restore property id=5 (starts with no rating or priority)."""
    saved = get_prop_state(db, 5)
    set_prop_state(db, 5, None, None)
    yield
    set_prop_state(db, 5, saved["user_rating"], saved["user_priority"])


# ── GET /api/properties ───────────────────────────────────────────────────────

class TestGetProperties:
    def test_returns_list(self, client):
        r = client.get("/api/properties")
        assert r.status_code == 200
        data = r.get_json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_has_expected_fields(self, client):
        data = client.get("/api/properties").get_json()
        prop = data[0]
        for field in ("id", "item_number", "status", "opening_bid", "user_rating", "user_priority"):
            assert field in prop, f"Missing field: {field}"

    def test_ordered_by_item_number(self, client):
        data = client.get("/api/properties").get_json()
        nums = [p["item_number"] for p in data]
        assert nums == sorted(nums)

    def test_search_by_address(self, client):
        all_props = client.get("/api/properties").get_json()
        # Pick a real address fragment from the first property
        fragment = all_props[0]["address"][:6] if all_props[0].get("address") else None
        if fragment is None:
            pytest.skip("First property has no address")
        r = client.get(f"/api/properties?search={fragment}")
        assert r.status_code == 200
        results = r.get_json()
        assert all(fragment.lower() in p.get("address", "").lower() for p in results)

    def test_search_no_match_returns_empty(self, client):
        r = client.get("/api/properties?search=ZZZZZZZZZZZZNOTAREALADDRESS")
        assert r.status_code == 200
        assert r.get_json() == []

    def test_status_filter_upcoming(self, client):
        r = client.get("/api/properties?status=Upcoming")
        data = r.get_json()
        assert all(p["status"] == "Upcoming" for p in data)

    def test_status_filter_canceled(self, client):
        r = client.get("/api/properties?status=Canceled")
        data = r.get_json()
        assert all(p["status"] == "Canceled" for p in data)


# ── GET /api/property/<id> ────────────────────────────────────────────────────

class TestGetPropertyDetail:
    def test_returns_property_detail(self, client):
        r = client.get("/api/property/1")
        assert r.status_code == 200
        data = r.get_json()
        assert "property" in data
        assert data["property"]["id"] == "1"

    def test_includes_related_arrays(self, client):
        r = client.get("/api/property/1")
        data = r.get_json()
        assert "images" in data
        assert "tax_history" in data
        assert "sale_history" in data

    def test_not_found_returns_404(self, client):
        r = client.get("/api/property/999999")
        assert r.status_code == 404


# ── GET /api/stats ────────────────────────────────────────────────────────────

class TestGetStats:
    def test_returns_list(self, client):
        r = client.get("/api/stats")
        assert r.status_code == 200
        assert isinstance(r.get_json(), list)

    def test_has_status_and_count(self, client):
        data = client.get("/api/stats").get_json()
        assert len(data) > 0
        for row in data:
            assert "status" in row
            assert "count" in row


# ── GET /api/ratings ─────────────────────────────────────────────────────────

class TestGetRatings:
    def test_returns_dict(self, client):
        r = client.get("/api/ratings")
        assert r.status_code == 200
        assert isinstance(r.get_json(), dict)

    def test_format_is_id_to_rating_priority(self, client):
        data = client.get("/api/ratings").get_json()
        for prop_id, val in data.items():
            assert prop_id.isdigit(), f"Key '{prop_id}' is not a digit string"
            assert isinstance(val, dict), f"Value for id {prop_id} is not a dict"
            assert "rating" in val
            assert "priority" in val

    def test_only_includes_rated_or_prioritised(self, client, prop5_clean):
        # prop 5 has no rating or priority — should not appear in export
        data = client.get("/api/ratings").get_json()
        assert "5" not in data

    def test_includes_rated_property(self, client, db, prop5_clean):
        set_prop_state(db, 5, "thumbs_up", None)
        data = client.get("/api/ratings").get_json()
        assert "5" in data
        assert data["5"]["rating"] == "thumbs_up"

    def test_includes_prioritised_property(self, client, db, prop5_clean):
        set_prop_state(db, 5, None, "High")
        data = client.get("/api/ratings").get_json()
        assert "5" in data
        assert data["5"]["priority"] == "High"


# ── POST /api/property/<id>/rating ───────────────────────────────────────────

class TestSetRating:
    def test_set_thumbs_up(self, client, db, prop5_clean):
        r = client.post("/api/property/5/rating",
                        json={"rating": "thumbs_up"})
        assert r.status_code == 200
        assert r.get_json()["success"] is True
        assert get_prop_state(db, 5)["user_rating"] == "thumbs_up"

    def test_set_thumbs_down(self, client, db, prop5_clean):
        r = client.post("/api/property/5/rating",
                        json={"rating": "thumbs_down"})
        assert r.status_code == 200
        assert get_prop_state(db, 5)["user_rating"] == "thumbs_down"

    def test_clear_rating(self, client, db, prop5_clean):
        set_prop_state(db, 5, "thumbs_up", None)
        r = client.post("/api/property/5/rating", json={"rating": None})
        assert r.status_code == 200
        assert get_prop_state(db, 5)["user_rating"] is None

    def test_invalid_rating_returns_400(self, client, prop5_clean):
        r = client.post("/api/property/5/rating",
                        json={"rating": "definitely_not_valid"})
        assert r.status_code == 400

    def test_nonexistent_property_returns_200_zero_rows(self, client):
        # UPDATE on missing row is not an error in SQL — just 0 rows affected
        r = client.post("/api/property/999999/rating",
                        json={"rating": "thumbs_up"})
        assert r.status_code == 200


# ── POST /api/property/<id>/priority ─────────────────────────────────────────

class TestSetPriority:
    def test_set_high(self, client, db, prop5_clean):
        r = client.post("/api/property/5/priority", json={"priority": "High"})
        assert r.status_code == 200
        assert r.get_json()["success"] is True
        assert get_prop_state(db, 5)["user_priority"] == "High"

    def test_set_medium(self, client, db, prop5_clean):
        r = client.post("/api/property/5/priority", json={"priority": "Medium"})
        assert r.status_code == 200
        assert get_prop_state(db, 5)["user_priority"] == "Medium"

    def test_set_low(self, client, db, prop5_clean):
        r = client.post("/api/property/5/priority", json={"priority": "Low"})
        assert r.status_code == 200
        assert get_prop_state(db, 5)["user_priority"] == "Low"

    def test_clear_priority(self, client, db, prop5_clean):
        set_prop_state(db, 5, None, "High")
        r = client.post("/api/property/5/priority", json={"priority": None})
        assert r.status_code == 200
        assert get_prop_state(db, 5)["user_priority"] is None

    def test_invalid_priority_returns_400(self, client, prop5_clean):
        r = client.post("/api/property/5/priority",
                        json={"priority": "Critical"})
        assert r.status_code == 400


# ── POST /api/ratings (bulk import) ──────────────────────────────────────────

class TestBulkImport:
    def test_new_format_sets_rating_and_priority(self, client, db, clean_props):
        payload = {
            "3": {"rating": "thumbs_up", "priority": "High"},
            "4": {"rating": "thumbs_down", "priority": "Low"},
        }
        r = client.post("/api/ratings", json=payload)
        assert r.status_code == 200
        assert r.get_json()["imported"] == 2
        assert get_prop_state(db, 3) == {"user_rating": "thumbs_up", "user_priority": "High"}
        assert get_prop_state(db, 4) == {"user_rating": "thumbs_down", "user_priority": "Low"}

    def test_old_flat_format(self, client, db, clean_props):
        payload = {"3": "thumbs_up", "4": "thumbs_down"}
        r = client.post("/api/ratings", json=payload)
        assert r.status_code == 200
        assert r.get_json()["imported"] == 2
        assert get_prop_state(db, 3)["user_rating"] == "thumbs_up"
        assert get_prop_state(db, 4)["user_rating"] == "thumbs_down"

    def test_rating_only_preserves_existing_priority(self, client, db, clean_props):
        set_prop_state(db, 3, None, "Medium")
        payload = {"3": {"rating": "thumbs_up", "priority": None}}
        client.post("/api/ratings", json=payload)
        # priority=None with COALESCE should keep existing Medium
        assert get_prop_state(db, 3)["user_priority"] == "Medium"

    def test_invalid_rating_value_is_skipped(self, client, db, clean_props):
        payload = {"3": {"rating": "invalid", "priority": None}}
        r = client.post("/api/ratings", json=payload)
        assert r.status_code == 200
        assert get_prop_state(db, 3)["user_rating"] is None

    def test_invalid_priority_value_is_skipped(self, client, db, clean_props):
        payload = {"3": {"rating": None, "priority": "Urgent"}}
        r = client.post("/api/ratings", json=payload)
        assert r.status_code == 200
        assert get_prop_state(db, 3)["user_priority"] is None

    def test_non_dict_body_returns_400(self, client):
        r = client.post("/api/ratings", json=[1, 2, 3])
        assert r.status_code == 400

    def test_empty_payload_returns_zero_imported(self, client):
        r = client.post("/api/ratings", json={})
        assert r.status_code == 200
        assert r.get_json()["imported"] == 0


# ── Export → Import round-trip ────────────────────────────────────────────────

class TestExportImportRoundTrip:
    def test_round_trip_rating_only(self, client, db, clean_props):
        # Set a rating, export, clear, import, verify
        set_prop_state(db, 3, "thumbs_up", None)

        exported = client.get("/api/ratings").get_json()
        assert "3" in exported
        assert exported["3"]["rating"] == "thumbs_up"

        # Clear
        set_prop_state(db, 3, None, None)
        assert get_prop_state(db, 3)["user_rating"] is None

        # Re-import using the exported {id: {rating, priority}} format
        r = client.post("/api/ratings", json=exported)
        assert r.status_code == 200

        assert get_prop_state(db, 3)["user_rating"] == "thumbs_up"

    def test_round_trip_rating_and_priority(self, client, db, clean_props):
        set_prop_state(db, 3, "thumbs_down", "High")
        set_prop_state(db, 4, "thumbs_up", "Low")

        exported = client.get("/api/ratings").get_json()
        assert exported["3"]["rating"] == "thumbs_down"
        assert exported["3"]["priority"] == "High"
        assert exported["4"]["rating"] == "thumbs_up"
        assert exported["4"]["priority"] == "Low"

        # Clear both
        set_prop_state(db, 3, None, None)
        set_prop_state(db, 4, None, None)

        client.post("/api/ratings", json=exported)

        assert get_prop_state(db, 3) == {"user_rating": "thumbs_down", "user_priority": "High"}
        assert get_prop_state(db, 4) == {"user_rating": "thumbs_up", "user_priority": "Low"}

    def test_export_format_compatible_with_frontend_restructure(self, client, db, clean_props):
        """
        The JS exportData() transforms GET /api/ratings output
        {id: {rating, priority}} into {ratings: {id: rating}, priorities: {id: priority}}.
        Verify we can simulate that transform and re-import it.
        """
        set_prop_state(db, 3, "thumbs_up", "Medium")

        raw = client.get("/api/ratings").get_json()

        # Simulate JS exportData() restructure
        ratings = {k: v["rating"] for k, v in raw.items() if v.get("rating")}
        priorities = {k: v["priority"] for k, v in raw.items() if v.get("priority")}
        file_data = {"ratings": ratings, "priorities": priorities, "savedFilters": {}}

        # Simulate JS importData() reassembly before POST
        all_ids = set(ratings) | set(priorities)
        payload = {
            pid: {"rating": ratings.get(pid), "priority": priorities.get(pid)}
            for pid in all_ids
        }

        set_prop_state(db, 3, None, None)
        client.post("/api/ratings", json=payload)
        assert get_prop_state(db, 3) == {"user_rating": "thumbs_up", "user_priority": "Medium"}
