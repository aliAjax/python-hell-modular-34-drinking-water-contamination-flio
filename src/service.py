from . import domain, rules
from .domain import DomainError


def _region_matches(item, region):
    payload = item["payload"]
    if payload.get("region") == region:
        return True
    return region in payload.get("zone_ids", [])


class Service:
    def __init__(self, repository):
        self.repository = repository

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
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
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            if not _region_matches(item, region):
                raise DomainError("region_mismatch", "不能给其他片区放行或处置", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        return self.get_item(item_id)

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

    def push_reading(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.READING_ROLES:
            raise DomainError("forbidden", "当前角色不能提交水源读数", 403)
        normalized = domain.normalize_reading(payload)
        result = self.repository.record_reading(
            normalized["source_id"],
            normalized["observed_at"],
            normalized["concentration"],
            normalized["contaminant"],
            normalized["limit_value"],
            normalized["payload"],
        )
        if result["status"] != "processed":
            return result
        reading = result["reading"]
        candidates = self.repository.find_linked_items_by_source(reading["source_id"])
        effects = []
        for item in candidates:
            effect = rules.assess_reading_effect(item, reading, domain.now_iso())
            if effect["kind"] == "skip":
                continue
            effects.append(
                {
                    "item_id": item["id"],
                    "kind": effect["kind"],
                    "new_payload": effect["new_payload"],
                    "event_payload": effect["event_payload"],
                }
            )
        result["affected"] = self.repository.apply_reading_effects(
            reading["id"], reading["source_id"], effects, actor, role
        )
        return result

    def reopen_reading(self, reading_id, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.REOPEN_ROLES:
            raise DomainError("forbidden", "当前角色不能批量重开片区", 403)
        return self.repository.reopen_by_reading(reading_id, actor, role)

    def list_readings(self, source_id=None):
        return self.repository.list_readings(source_id)
