import json
import sqlite3
from datetime import datetime, timezone

from . import rules
from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError, parse_iso


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_column(self, conn, table, column, ddl):
        cols = [row[1] for row in conn.execute("PRAGMA table_info(%s)" % table).fetchall()]
        if column not in cols:
            conn.execute("ALTER TABLE %s ADD COLUMN %s" % (table, ddl))

    def initialize(self):
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    stable_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, stable_key)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    role TEXT,
                    payload TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_readings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    concentration REAL NOT NULL,
                    contaminant TEXT,
                    limit_value REAL,
                    payload TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    UNIQUE(source_id, observed_at)
                );
                CREATE TABLE IF NOT EXISTS reading_item_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reading_id INTEGER NOT NULL,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(reading_id, item_id),
                    FOREIGN KEY(reading_id) REFERENCES source_readings(id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE INDEX IF NOT EXISTS idx_source_readings_source
                    ON source_readings(source_id, observed_at);
                """
            )
            # 升级兼容：旧数据按未关联处理；升级后新建的片区记录默认关联水源读数。
            self._ensure_column(conn, "items", "reading_linked", "reading_linked INTEGER NOT NULL DEFAULT 0")
        finally:
            conn.close()

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _last_hash(self, conn, item_id):
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE item_id IS ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        return row["event_hash"] if row else "GENESIS"

    def append_audit(self, conn, item_id, event_type, actor, role, payload):
        previous = self._last_hash(conn, item_id)
        event = {
            "item_id": item_id,
            "event_type": event_type,
            "actor": actor,
            "role": role,
            "payload": payload,
            "created_at": now_iso(),
        }
        event_hash = audit_hash(previous, event)
        conn.execute(
            "INSERT INTO audit_events(item_id,event_type,actor,role,payload,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (item_id, event_type, actor, role, canonical_json(payload), previous, event_hash, event["created_at"]),
        )

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at,reading_linked) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        entity_type,
                        stable_key,
                        initial_status,
                        1,
                        canonical_json(payload),
                        actor,
                        role,
                        now_iso(),
                        now_iso(),
                        1,
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_item", "同一业务实体已经存在")
            item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def list_items(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM items WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM items ORDER BY id DESC").fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_source", "同一来源记录已经提交")
            source_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM sources WHERE item_id=? ORDER BY id DESC", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            previous_status = row["status"]
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
            )
            self.append_audit(conn, item_id, action, actor, role, event_payload)
            if previous_status == "pending_reinspection" and new_status != "pending_reinspection":
                self._promote_queue(conn)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def audit_trail(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM audit_events WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            return {"counts": counts, "items": self.list_items()}
        finally:
            conn.close()

    def _row_to_reading(self, row):
        if row is None:
            return None
        value = dict(row)
        value["payload"] = json.loads(value["payload"])
        return value

    def record_reading(self, source_id, observed_at, concentration, contaminant, limit_value, payload):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            latest = conn.execute(
                "SELECT * FROM source_readings WHERE source_id=? ORDER BY observed_at DESC, id DESC LIMIT 1",
                (source_id,),
            ).fetchone()
            if latest is not None:
                if latest["observed_at"] == observed_at:
                    conn.execute("COMMIT")
                    return {"status": "duplicate", "reading": self._row_to_reading(latest), "affected": []}
                if parse_iso(observed_at) < parse_iso(latest["observed_at"]):
                    conn.execute("COMMIT")
                    return {"status": "discarded", "reading": None, "affected": []}
            conn.execute(
                "INSERT INTO source_readings(source_id,observed_at,concentration,contaminant,limit_value,payload,created_at) VALUES(?,?,?,?,?,?,?)",
                (source_id, observed_at, concentration, contaminant, limit_value, canonical_json(payload), now_iso()),
            )
            reading_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            conn.execute("COMMIT")
            reading = {
                "id": reading_id,
                "source_id": source_id,
                "observed_at": observed_at,
                "concentration": concentration,
                "contaminant": contaminant,
                "limit_value": limit_value,
                "payload": payload,
            }
            return {"status": "processed", "reading": reading, "affected": []}
        except sqlite3.IntegrityError:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            existing = conn.execute(
                "SELECT * FROM source_readings WHERE source_id=? AND observed_at=?", (source_id, observed_at)
            ).fetchone()
            return {"status": "duplicate", "reading": self._row_to_reading(existing), "affected": []}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def find_linked_items_by_source(self, source_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM items WHERE reading_linked=1 AND json_extract(payload,'$.source_id')=? ORDER BY id",
                (source_id,),
            ).fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def _promote_queue(self, conn):
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM items WHERE status='pending_reinspection'"
        ).fetchone()["c"]
        while pending < rules.REINSPECTION_CAPACITY:
            row = conn.execute(
                "SELECT id FROM items WHERE status='queued' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                break
            conn.execute(
                "UPDATE items SET status='pending_reinspection',updated_at=? WHERE id=?",
                (now_iso(), row["id"]),
            )
            self.append_audit(conn, row["id"], "queued_promoted", "system", "system", {})
            pending += 1

    def apply_reading_effects(self, reading_id, source_id, effects, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            pending = conn.execute(
                "SELECT COUNT(*) AS c FROM items WHERE status='pending_reinspection'"
            ).fetchone()["c"]
            updated = []
            for eff in effects:
                row = conn.execute("SELECT * FROM items WHERE id=?", (eff["item_id"],)).fetchone()
                if row is None:
                    continue
                kind = eff["kind"]
                new_status = row["status"]
                if kind == "reopen":
                    if pending < rules.REINSPECTION_CAPACITY:
                        new_status = "pending_reinspection"
                        pending += 1
                    else:
                        new_status = "queued"
                elif kind == "expire":
                    new_status = "reconfirming"
                version = int(row["version"]) + 1
                conn.execute(
                    "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                    (new_status, version, canonical_json(eff["new_payload"]), now_iso(), eff["item_id"]),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO reading_item_links(reading_id,item_id,action,created_at) VALUES(?,?,?,?)",
                    (reading_id, eff["item_id"], kind, now_iso()),
                )
                self.append_audit(
                    conn,
                    eff["item_id"],
                    "reading_" + kind,
                    actor,
                    role,
                    {**eff["event_payload"], "reading_id": reading_id, "source_id": source_id},
                )
                item = dict(row)
                item["payload"] = eff["new_payload"]
                item["status"] = new_status
                item["version"] = version
                updated.append(item)
            conn.execute("COMMIT")
            return updated
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def reopen_by_reading(self, reading_id, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM source_readings WHERE id=?", (reading_id,)).fetchone()
            if row is None:
                raise NotFoundError("reading_not_found", "水源读数不存在")
            reading = self._row_to_reading(row)
            items = conn.execute(
                "SELECT * FROM items WHERE reading_linked=1 AND status='restored' "
                "AND json_extract(payload,'$.source_id')=? ORDER BY id",
                (reading["source_id"],),
            ).fetchall()
            pending = conn.execute(
                "SELECT COUNT(*) AS c FROM items WHERE status='pending_reinspection'"
            ).fetchone()["c"]
            updated = []
            for item_row in items:
                payload = json.loads(item_row["payload"])
                payload["reopen"] = {
                    "reading_id": reading_id,
                    "observed_at": reading["observed_at"],
                    "concentration": reading["concentration"],
                    "reason": "regulator_reopen",
                    "at": now_iso(),
                }
                if pending < rules.REINSPECTION_CAPACITY:
                    new_status = "pending_reinspection"
                    pending += 1
                else:
                    new_status = "queued"
                version = int(item_row["version"]) + 1
                conn.execute(
                    "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                    (new_status, version, canonical_json(payload), now_iso(), item_row["id"]),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO reading_item_links(reading_id,item_id,action,created_at) VALUES(?,?,?,?)",
                    (reading_id, item_row["id"], "reopen", now_iso()),
                )
                self.append_audit(
                    conn,
                    item_row["id"],
                    "reading_reopen",
                    actor,
                    role,
                    {"reading_id": reading_id, "source_id": reading["source_id"], "status": new_status},
                )
                item = dict(item_row)
                item["payload"] = payload
                item["status"] = new_status
                item["version"] = version
                updated.append(item)
            conn.execute("COMMIT")
            return updated
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_readings(self, source_id=None):
        conn = self.connect()
        try:
            if source_id:
                rows = conn.execute(
                    "SELECT * FROM source_readings WHERE source_id=? ORDER BY observed_at DESC, id DESC",
                    (source_id,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM source_readings ORDER BY id DESC").fetchall()
            return [self._row_to_reading(row) for row in rows]
        finally:
            conn.close()
