import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src import rules
from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError


class ReadingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _create_item(self, source_id="SRC-9", detected_at="2026-09-27T06:00:00+00:00", region=None):
        payload = {
            "source_id": source_id,
            "contaminant": "nitrate",
            "detected_at": detected_at,
            "concentration": 20,
            "limit": 10,
            "zone_ids": ["Z-1"],
            "population": 1000,
        }
        if region:
            payload["region"] = region
        return self.service.create_item(payload, "analyst-1", "analyst")

    def _drive_to_restored(self, item, region=None):
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "analyst-1", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-%s" % item["id"], "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-1", "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-%s" % item["id"], "zone_id": "Z-1", "concentration": 2}, "lab", "lab", item["version"])
        item = self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "coord", "coordinator", item["version"], region)
        return item

    def _reading(self, reading_id, source_id="SRC-9", observed_at="2026-10-01T00:00:00+00:00", concentration=50):
        return {
            "reading_id": reading_id,
            "source_id": source_id,
            "observed_at": observed_at,
            "concentration": concentration,
        }

    def test_reading_reassesses_tracked_items_by_source(self):
        item_a = self._create_item(source_id="SRC-A", detected_at="2026-09-27T06:00:00+00:00")
        item_b = self._create_item(source_id="SRC-A", detected_at="2026-09-27T07:00:00+00:00")
        item_c = self._create_item(source_id="SRC-C", detected_at="2026-09-27T08:00:00+00:00")
        result = self.service.ingest_reading(self._reading("R-1", "SRC-A"), "station-1", "monitor")
        self.assertFalse(result["discarded"])
        self.assertEqual(sorted(result["reassessed"]), [item_a["id"], item_b["id"]])
        self.assertEqual(result["rolled_back"], [])
        updated = self.service.get_item(item_a["id"])
        self.assertEqual(updated["payload"]["concentration"], 50)
        self.assertEqual(updated["payload"]["assessment"]["level"], "critical")
        self.assertEqual(updated["payload"]["last_reading_id"], "R-1")
        untouched = self.service.get_item(item_c["id"])
        self.assertEqual(untouched["payload"]["concentration"], 20)
        self.assertNotIn("last_reading_id", untouched["payload"])

    def test_restored_item_rolls_back_when_reading_exceeds(self):
        item = self._drive_to_restored(self._create_item())
        result = self.service.ingest_reading(self._reading("R-1"), "station-1", "monitor")
        self.assertEqual(result["rolled_back"], [item["id"]])
        updated = self.service.get_item(item["id"])
        self.assertEqual(updated["status"], "recheck")
        self.assertEqual(updated["payload"]["recheck"]["reading_id"], "R-1")
        self.assertNotIn("restoration", updated["payload"])
        history = updated["payload"]["previous_restorations"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["invalidated_by_reading"], "R-1")
        # 未超标的新读数不影响已恢复片区
        result = self.service.ingest_reading(
            self._reading("R-2", observed_at="2026-10-02T00:00:00+00:00", concentration=5), "station-1", "monitor"
        )
        self.assertEqual(result["rolled_back"], [])
        self.assertEqual(self.service.get_item(item["id"])["status"], "recheck")

    def test_invalidated_restoration_requires_fresh_confirmation(self):
        item = self._drive_to_restored(self._create_item())
        self.service.ingest_reading(self._reading("R-1"), "station-1", "monitor")
        item = self.service.get_item(item["id"])
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "release", {}, "f", "field_operator", item["version"])
        self.assertEqual(context.exception.code, "sample_required")
        item = self.service.act(
            item["id"], "sample", {"sample_id": "S-new", "zone_id": "Z-1", "concentration": 30}, "lab", "lab", item["version"]
        )
        self.assertEqual(item["status"], "recheck")
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "release", {}, "f", "field_operator", item["version"])
        self.assertEqual(context.exception.code, "quality_not_met")
        item = self.service.act(
            item["id"], "sample", {"sample_id": "S-ok", "zone_id": "Z-1", "concentration": 3}, "lab", "lab", item["version"]
        )
        item = self.service.act(item["id"], "release", {"note": "复检合格"}, "f", "field_operator", item["version"])
        self.assertEqual(item["status"], "restored")
        self.assertEqual(item["payload"]["restoration"]["confirmed_after_reading"], "R-1")
        events = [event["event_type"] for event in item["audit"]]
        self.assertIn("invalidate_restore", events)
        self.assertIn("release", events)
        self.assertEqual(len(item["payload"]["previous_restorations"]), 1)

    def test_recheck_capacity_queues_and_promotes(self):
        items = [
            self._drive_to_restored(self._create_item(detected_at="2026-09-27T0%d:00:00+00:00" % hour))
            for hour in range(rules.RECHECK_CAPACITY + 1)
        ]
        result = self.service.ingest_reading(self._reading("R-1"), "station-1", "monitor")
        self.assertEqual(len(result["rolled_back"]), rules.RECHECK_CAPACITY)
        self.assertEqual(len(result["queued"]), 1)
        queued_id = result["queued"][0]
        self.assertEqual(self.service.get_item(queued_id)["status"], "recheck_queued")
        target = result["rolled_back"][0]
        item = self.service.get_item(target)
        item = self.service.act(
            target, "sample", {"sample_id": "S-q", "zone_id": "Z-1", "concentration": 3}, "lab", "lab", item["version"]
        )
        item = self.service.act(target, "release", {}, "f", "field_operator", item["version"])
        self.assertEqual(item["status"], "restored")
        promoted = self.service.get_item(queued_id)
        self.assertEqual(promoted["status"], "recheck")
        events = [event["event_type"] for event in promoted["audit"]]
        self.assertIn("recheck_promoted", events)

    def test_duplicate_reading_recorded_once(self):
        self.service.ingest_reading(self._reading("R-1"), "station-1", "monitor")
        with self.assertRaises(ConflictError) as context:
            self.service.ingest_reading(self._reading("R-1"), "station-1", "monitor")
        self.assertEqual(context.exception.code, "duplicate_reading")
        self.assertEqual(len(self.service.list_readings("SRC-9")), 1)

    def test_stale_reading_discarded(self):
        item = self._create_item()
        self.service.ingest_reading(self._reading("R-1", observed_at="2026-10-01T10:00:00+00:00"), "s", "monitor")
        version = self.service.get_item(item["id"])["version"]
        result = self.service.ingest_reading(self._reading("R-2", observed_at="2026-10-01T09:00:00+00:00"), "s", "monitor")
        self.assertTrue(result["discarded"])
        self.assertEqual(result["reason"], "stale_reading")
        self.assertEqual(len(self.service.list_readings("SRC-9")), 1)
        self.assertEqual(self.service.get_item(item["id"])["version"], version)

    def test_reading_role_restricted(self):
        with self.assertRaises(DomainError) as context:
            self.service.ingest_reading(self._reading("R-1"), "f", "field_operator")
        self.assertEqual(context.exception.status, 403)

    def test_regulator_batch_reopen_by_reading(self):
        first = self._drive_to_restored(self._create_item(detected_at="2026-09-27T06:00:00+00:00"))
        second = self._drive_to_restored(self._create_item(detected_at="2026-09-27T07:00:00+00:00"))
        result = self.service.ingest_reading(self._reading("R-1", concentration=5), "station-1", "monitor")
        self.assertEqual(result["rolled_back"], [])
        reading_id = result["reading"]["id"]
        with self.assertRaises(DomainError) as context:
            self.service.reopen_by_reading(reading_id, "coord", "coordinator")
        self.assertEqual(context.exception.status, 403)
        result = self.service.reopen_by_reading(reading_id, "reg", "regulator")
        self.assertEqual(sorted(result["reopened"]), [first["id"], second["id"]])
        for item_id in result["reopened"]:
            item = self.service.get_item(item_id)
            self.assertEqual(item["status"], "recheck")
            self.assertEqual(item["payload"]["recheck"]["reason"], "regulator_reopen")

    def test_region_enforced_on_release_and_legacy_items_unassociated(self):
        regional = self._drive_to_restored(self._create_item(region="north"), region="north")
        legacy = self._drive_to_restored(self._create_item(detected_at="2026-09-27T07:00:00+00:00"))
        self.service.ingest_reading(self._reading("R-1"), "station-1", "monitor")
        for item_id in (regional["id"], legacy["id"]):
            item = self.service.get_item(item_id)
            self.service.act(
                item_id, "sample", {"sample_id": "S-%s" % item_id, "zone_id": "Z-1", "concentration": 3},
                "lab", "lab", item["version"],
            )
        item = self.service.get_item(regional["id"])
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "release", {}, "f-south", "field_operator", item["version"], "south")
        self.assertEqual(context.exception.code, "region_mismatch")
        self.assertEqual(context.exception.status, 403)
        item = self.service.act(item["id"], "release", {}, "f-north", "field_operator", item["version"], "north")
        self.assertEqual(item["status"], "restored")
        legacy = self.service.get_item(legacy["id"])
        legacy = self.service.act(legacy["id"], "release", {}, "f-any", "field_operator", legacy["version"])
        self.assertEqual(legacy["status"], "restored")

    def test_audit_trail_remains_queryable_after_rollback(self):
        item = self._drive_to_restored(self._create_item())
        self.service.ingest_reading(self._reading("R-1"), "station-1", "monitor")
        item = self.service.get_item(item["id"])
        events = [event["event_type"] for event in item["audit"]]
        for expected in ("created", "verify", "advise", "flush", "disinfect", "sample", "restore", "invalidate_restore"):
            self.assertIn(expected, events)
        self.assertEqual(item["payload"]["previous_restorations"][0]["actor"], "coord")


if __name__ == "__main__":
    unittest.main()
