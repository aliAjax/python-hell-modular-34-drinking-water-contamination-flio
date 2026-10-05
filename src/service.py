from . import domain, rules
from .domain import DomainError
from .repository import now_iso


class Service:
    def __init__(self, repository):
        self.repository = repository

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        if normalized.get("region") is None and region:
            normalized["region"] = region
        stable_key = normalized.pop("_stable_key")
        return self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized, actor, role
        )

    def add_source(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        item = self.repository.get_item(item_id)
        normalized = domain.normalize_source(payload)
        if region and rules.ENFORCE_REGION and role != "regulator" and normalized.get("region") and normalized["region"] != region:
            raise DomainError("region_mismatch", "来源记录不属于当前管辖区域", 403)
        result = self.repository.add_source(
            item_id,
            normalized.pop("source_type"),
            normalized.pop("external_id"),
            normalized,
            normalized.pop("observed_at"),
            actor,
            role,
        )
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and role != "regulator":
            item_region = item["payload"].get("region")
            if item_region and item_region != region:
                raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        previous_status = item["status"]
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        if action == "release" and previous_status == "recheck":
            self.repository.promote_queued(actor, role)
        return self.get_item(item_id)

    def ingest_reading(self, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.READING_ROLES:
            raise DomainError("forbidden", "当前角色不能提交水源读数", 403)
        reading = domain.normalize_reading(payload)
        latest = self.repository.latest_reading(reading["source_id"])
        if latest and domain.parse_instant(reading["observed_at"]) < domain.parse_instant(latest["observed_at"]):
            return {
                "discarded": True,
                "reason": "stale_reading",
                "source_id": reading["source_id"],
                "reading_id": reading["reading_id"],
            }
        plan, summary = self._reading_plan(reading)
        summary["reading"] = self.repository.record_reading(reading, plan, actor, role)
        summary["discarded"] = False
        return summary

    def _reading_plan(self, reading):
        plan = []
        reassessed, rolled_back, queued = [], [], []
        recheck_count = self.repository.count_items("recheck")
        items = sorted(self.repository.list_items(), key=lambda item: item["id"])
        for item in items:
            current = item["payload"]
            if current.get("source_id") != reading["source_id"]:
                continue
            status = item["status"]
            if status == "cancelled":
                continue
            if status == "restored":
                limit = float(current.get("limit", 0))
                if reading["concentration"] <= limit:
                    continue
                step, new_status = self._rollback_step(item, reading, "reading_exceeded", recheck_count)
                if new_status == "recheck":
                    recheck_count += 1
                    rolled_back.append(item["id"])
                else:
                    queued.append(item["id"])
                plan.append(step)
                continue
            updated = dict(current)
            updated["concentration"] = reading["concentration"]
            updated["assessment"] = rules.assess(updated)
            updated["last_reading_id"] = reading["reading_id"]
            plan.append({
                "item_id": item["id"],
                "action": "reassess",
                "new_status": status,
                "new_payload": updated,
                "event_payload": {
                    "reading_id": reading["reading_id"],
                    "source_id": reading["source_id"],
                    "concentration": reading["concentration"],
                    "assessment": updated["assessment"],
                },
            })
            reassessed.append(item["id"])
        return plan, {"reassessed": reassessed, "rolled_back": rolled_back, "queued": queued}

    def _rollback_step(self, item, reading, reason, recheck_count):
        updated = dict(item["payload"])
        restoration = updated.get("restoration")
        if restoration:
            history = updated.setdefault("previous_restorations", [])
            history.append(dict(restoration, invalidated_by_reading=reading["reading_id"], invalidated_reason=reason))
            updated.pop("restoration", None)
        updated["recheck"] = {
            "reading_id": reading["reading_id"],
            "source_id": reading["source_id"],
            "reason": reason,
            "sample_start": len(updated.get("sample_results", [])),
            "since": now_iso(),
        }
        new_status = "recheck" if recheck_count < rules.RECHECK_CAPACITY else "recheck_queued"
        action = "invalidate_restore" if reason == "reading_exceeded" else "reopen"
        step = {
            "item_id": item["id"],
            "action": action,
            "new_status": new_status,
            "new_payload": updated,
            "event_payload": {
                "reading_id": reading["reading_id"],
                "source_id": reading["source_id"],
                "reason": reason,
                "status": new_status,
            },
        }
        return step, new_status

    def reopen_by_reading(self, reading_pk, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role != "regulator":
            raise DomainError("forbidden", "只有监管角色可以按读数批量重开片区", 403)
        reading = self.repository.get_reading(reading_pk)
        plan = []
        reopened, queued = [], []
        recheck_count = self.repository.count_items("recheck")
        items = sorted(self.repository.list_items(), key=lambda item: item["id"])
        for item in items:
            if item["status"] != "restored":
                continue
            if item["payload"].get("source_id") != reading["source_id"]:
                continue
            step, new_status = self._rollback_step(item, reading, "regulator_reopen", recheck_count)
            if new_status == "recheck":
                recheck_count += 1
                reopened.append(item["id"])
            else:
                queued.append(item["id"])
            plan.append(step)
        if plan:
            self.repository.apply_plan(plan, actor, role)
        return {"reading": reading, "reopened": reopened, "queued": queued}

    def list_readings(self, source_id=None):
        return self.repository.list_readings(source_id)

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()
