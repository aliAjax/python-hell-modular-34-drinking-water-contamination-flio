import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError
from src import rules


class ReadingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)
        self._notice = 0
        self._sample = 0

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _create(self, source_id="SRC-1", contaminant="nitrate", detected_at="2026-09-27T06:00:00+00:00",
                concentration=20, limit=10, zones=None, population=1000):
        return self.service.create_item({
            "source_id": source_id,
            "contaminant": contaminant,
            "detected_at": detected_at,
            "concentration": concentration,
            "limit": limit,
            "zone_ids": zones or ["Z-1"],
            "population": population,
        }, "a", "analyst")

    def _restore(self, source_id="SRC-1", contaminant="nitrate", detected_at="2026-09-27T06:00:00+00:00",
                 zones=None, limit=10):
        zones = zones or ["Z-1"]
        item = self._create(source_id, contaminant, detected_at, concentration=20, limit=limit, zones=zones)
        self._notice += 1
        self._sample += 1
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-%d" % self._notice, "kind": "boil", "message": "x"}, "d", "dispatcher", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": zones[0]}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": zones[0], "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-%d" % self._sample, "zone_id": zones[0], "concentration": 2}, "l", "lab", item["version"])
        item = self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "c", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")
        return item

    def _reading(self, source_id="SRC-1", observed_at="2026-10-01T00:00:00+00:00", concentration=50, limit=10):
        return {"source_id": source_id, "observed_at": observed_at, "concentration": concentration, "limit": limit}

    def test_new_reading_recalculates_active_district_level(self):
        item = self._create(concentration=5, limit=10, population=100)
        before = self.service.get_item(item["id"])
        self.assertEqual(before["assessment"]["level"], "low")
        result = self.service.push_reading(self._reading(concentration=50), "station-1", "analyst")
        self.assertEqual(result["status"], "processed")
        self.assertEqual(len(result["affected"]), 1)
        affected = result["affected"][0]
        self.assertEqual(affected["payload"]["assessment"]["level"], "critical")
        self.assertEqual(affected["payload"]["latest_reading"]["concentration"], 50)

    def test_duplicate_reading_recorded_only_once(self):
        item = self._create()
        first = self.service.push_reading(self._reading(), "station-1", "analyst")
        self.assertEqual(first["status"], "processed")
        second = self.service.push_reading(self._reading(), "station-2", "analyst")
        self.assertEqual(second["status"], "duplicate")
        self.assertEqual(second["reading"]["id"], first["reading"]["id"])
        readings = self.repo.list_readings()
        self.assertEqual(len(readings), 1)
        # 重复推送不产生新的关联处置
        self.assertEqual(len(self.repo.list_readings("SRC-1")), 1)

    def test_older_observation_is_discarded(self):
        self._create()
        newer = self.service.push_reading(self._reading(observed_at="2026-10-05T00:00:00+00:00"), "s", "analyst")
        self.assertEqual(newer["status"], "processed")
        older = self.service.push_reading(self._reading(observed_at="2026-09-01T00:00:00+00:00"), "s", "analyst")
        self.assertEqual(older["status"], "discarded")
        self.assertIsNone(older["reading"])
        readings = self.repo.list_readings()
        self.assertEqual(len(readings), 1)
        self.assertEqual(readings[0]["observed_at"], "2026-10-05T00:00:00+00:00")

    def test_restored_district_exceeding_goes_to_pending_reinspection(self):
        item = self._restore(limit=10)
        result = self.service.push_reading(self._reading(concentration=50, limit=10), "s", "analyst")
        self.assertEqual(result["status"], "processed")
        affected = result["affected"][0]
        self.assertEqual(affected["status"], "pending_reinspection")
        self.assertEqual(affected["payload"]["reopen"]["reason"], "exceeded")
        # 关联留痕可查
        conn = self.repo.connect()
        link = conn.execute(
            "SELECT * FROM reading_item_links WHERE item_id=? AND action='reopen'", (item["id"],)
        ).fetchone()
        conn.close()
        self.assertIsNotNone(link)

    def test_reinspection_capacity_queues_then_promotes(self):
        rules.REINSPECTION_CAPACITY = 2
        items = [
            self._restore("SRC-1", "a", "2026-09-27T06:00:00+00:00", zones=["Z-1"]),
            self._restore("SRC-1", "b", "2026-09-27T07:00:00+00:00", zones=["Z-2"]),
            self._restore("SRC-1", "c", "2026-09-27T08:00:00+00:00", zones=["Z-3"]),
        ]
        result = self.service.push_reading(self._reading(concentration=50), "s", "analyst")
        statuses = sorted(a["status"] for a in result["affected"])
        self.assertEqual(statuses, ["pending_reinspection", "pending_reinspection", "queued"])

        # 恢复一个待复检片区，空出容量，排队的最旧片区自动补入待复检。
        pending = [a for a in result["affected"] if a["status"] == "pending_reinspection"][0]
        item = self.service.act(pending["id"], "sample", {"sample_id": "RS-1", "zone_id": "Z-1", "concentration": 2}, "l", "lab", pending["version"])
        item = self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "c", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")
        # 排队中的一个应被提升为待复检
        queued = [a for a in result["affected"] if a["status"] == "queued"][0]["id"]
        promoted = self.repo.get_item(queued)
        self.assertEqual(promoted["status"], "pending_reinspection")

    def test_regulator_batch_reopen(self):
        self._restore("SRC-1", "a", "2026-09-27T06:00:00+00:00")
        self._restore("SRC-1", "b", "2026-09-27T07:00:00+00:00")
        reading = self.service.push_reading(self._reading(concentration=5), "s", "analyst")["reading"]
        # 未超标不会自动重开
        self.assertTrue(all(a["status"] == "restored" for a in self.repo.list_items()))
        reopened = self.service.reopen_reading(reading["id"], "reg-1", "regulator")
        self.assertEqual(len(reopened), 2)
        self.assertTrue(all(a["status"] == "pending_reinspection" for a in reopened))
        # 非监管角色不能批量重开
        with self.assertRaises(DomainError) as ctx:
            self.service.reopen_reading(reading["id"], "f", "field_operator")
        self.assertEqual(ctx.exception.code, "forbidden")

    def test_field_operator_overzone_release_rejected(self):
        item = self._create(zones=["Z-1"])
        # 现场人员跨片区操作（放行/处置）被拒
        with self.assertRaises(DomainError) as ctx:
            self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "f", "field_operator", region="Z-2")
        self.assertEqual(ctx.exception.code, "region_mismatch")
        self.assertEqual(ctx.exception.status, 403)
        # 本片区则放行到状态校验
        with self.assertRaises(DomainError) as ctx2:
            self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "f", "field_operator", item["version"], region="Z-1")
        self.assertEqual(ctx2.exception.code, "invalid_state")
        # 协调员跨片区放行同样被拒
        with self.assertRaises(DomainError) as ctx3:
            self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "c", "coordinator", region="Z-2")
        self.assertEqual(ctx3.exception.code, "region_mismatch")

    def test_restoration_expiry_requires_reconfirm(self):
        item = self._restore()
        # 把恢复结论时间回拨到 TTL 之前，模拟失效。
        conn = self.repo.connect()
        row = conn.execute("SELECT payload FROM items WHERE id=?", (item["id"],)).fetchone()
        payload = json.loads(row["payload"])
        payload["restoration"]["at"] = "2020-01-01T00:00:00+00:00"
        conn.execute("UPDATE items SET payload=? WHERE id=?", (json.dumps(payload, ensure_ascii=False), item["id"]))
        conn.close()

        result = self.service.push_reading(self._reading(concentration=5), "s", "analyst")
        affected = result["affected"][0]
        self.assertEqual(affected["status"], "reconfirming")
        # 重新确认后恢复
        item = self.service.act(affected["id"], "reconfirm", {"note": "重新确认"}, "c", "coordinator", affected["version"])
        self.assertEqual(item["status"], "restored")

    def test_legacy_data_unassociated_but_records_queryable(self):
        item = self._create()
        # 模拟升级前的旧数据：未关联水源读数。
        conn = self.repo.connect()
        conn.execute("UPDATE items SET reading_linked=0 WHERE id=?", (item["id"],))
        conn.close()
        result = self.service.push_reading(self._reading(concentration=50), "s", "analyst")
        # 旧数据不被新读数自动改状态
        self.assertEqual(result["affected"], [])
        legacy = self.service.get_item(item["id"])
        self.assertEqual(legacy["status"], "detected")
        self.assertNotIn("latest_reading", legacy["payload"])
        # 原来的处置记录照旧可查
        self.assertIsInstance(legacy["audit"], list)
        self.assertGreaterEqual(len(legacy["audit"]), 1)
        # 新读数本身已入账
        self.assertEqual(len(self.repo.list_readings()), 1)


if __name__ == "__main__":
    unittest.main()
