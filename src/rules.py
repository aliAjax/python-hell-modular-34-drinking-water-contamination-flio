from datetime import datetime, timezone

from .domain import DomainError, parse_iso, now_iso

ENTITY_TYPE = "water_contamination"
INITIAL_STATUS = "detected"
CREATE_ROLES = {"analyst", "dispatcher"}
SOURCE_ROLES = {"analyst", "dispatcher", "field_operator", "lab"}
READING_ROLES = {"analyst", "dispatcher", "coordinator", "regulator", "lab", "field_operator"}
REOPEN_ROLES = {"regulator"}
ACTION_ROLES = {
    "verify": {"analyst", "dispatcher"},
    "advise": {"coordinator", "dispatcher"},
    "switch_source": {"coordinator"},
    "flush": {"field_operator"},
    "disinfect": {"field_operator"},
    "sample": {"lab", "field_operator"},
    "restore": {"coordinator", "regulator"},
    "reconfirm": {"coordinator", "regulator"},
    "cancel": {"coordinator"},
}
ENFORCE_REGION = True
REGION_SENSITIVE_ACTIONS = {"flush", "disinfect", "sample", "restore", "reconfirm"}
ACTION_REQUIRES_VERSION = {"advise", "switch_source", "flush", "disinfect", "sample", "restore", "reconfirm", "cancel"}

# 复检容量：同时处于待复检状态的片区上限，满了先排队。
REINSPECTION_CAPACITY = 2
# 恢复结论有效期（天），超期未重新确认则结论失效。
RESTORATION_TTL_DAYS = 30

# 仍在跟进（未办结）的状态；已恢复与已取消单独处理。
ACTIVE_STATUSES = {
    "detected", "verified", "advisory", "flushing", "disinfected", "sampled",
    "switched", "pending_reinspection", "queued", "reconfirming",
}
TERMINAL_STATUSES = {"cancelled"}


def assess(payload):
    concentration = float(payload.get("concentration", 0))
    limit = max(float(payload.get("limit", 0.000001)), 0.000001)
    ratio = concentration / limit
    population = int(payload.get("population", 0))
    score = min(100.0, ratio * 35.0 + min(population / 1000.0, 40.0))
    if score >= 80:
        level = "critical"
    elif score >= 50:
        level = "high"
    elif score >= 20:
        level = "medium"
    else:
        level = "low"
    return {"score": round(score, 2), "level": level, "ratio": round(ratio, 3)}


def _restoration_expired(restored_at, now_str):
    if not restored_at:
        return False
    try:
        restored = parse_iso(restored_at)
        now = parse_iso(now_str)
    except (TypeError, ValueError):
        return False
    if restored.tzinfo is None:
        restored = restored.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    age = now - restored
    return age.total_seconds() > RESTORATION_TTL_DAYS * 86400


def assess_reading_effect(item, reading, now_str):
    """判断新水源读数对一个片区的影响。

    返回 dict: kind / new_status / new_payload / event_payload。
    kind:
      - recalculate: 仍在跟进，按新读数重算等级
      - reopen: 已恢复且新读数超标，退回待复检（容量由仓库决定排队）
      - expire: 恢复结论超期失效，需重新确认
      - observed: 已恢复且未超标，仅留痕
      - skip: 已取消，不处理
    """
    payload = item["payload"]
    status = item["status"]
    limit = max(float(payload.get("limit", 0.000001)), 0.000001)
    concentration = float(reading["concentration"])
    new_payload = dict(payload)
    new_payload["latest_reading"] = {
        "reading_id": reading["id"],
        "observed_at": reading["observed_at"],
        "concentration": concentration,
    }

    if status in TERMINAL_STATUSES:
        return {"kind": "skip", "new_status": None, "new_payload": new_payload, "event_payload": {}}

    if status == "restored":
        if concentration > limit:
            new_payload["assessment"] = assess({**payload, "concentration": concentration})
            new_payload["reopen"] = {
                "reading_id": reading["id"],
                "observed_at": reading["observed_at"],
                "concentration": concentration,
                "limit": limit,
                "reason": "exceeded",
                "at": now_str,
            }
            return {
                "kind": "reopen",
                "new_status": None,
                "new_payload": new_payload,
                "event_payload": {"reading_id": reading["id"], "concentration": concentration, "limit": limit, "reason": "exceeded"},
            }
        restoration = payload.get("restoration") or {}
        if _restoration_expired(restoration.get("at"), now_str):
            return {
                "kind": "expire",
                "new_status": "reconfirming",
                "new_payload": new_payload,
                "event_payload": {"reading_id": reading["id"], "restored_at": restoration.get("at"), "reason": "restoration_expired"},
            }
        return {
            "kind": "observed",
            "new_status": None,
            "new_payload": new_payload,
            "event_payload": {"reading_id": reading["id"], "concentration": concentration, "limit": limit},
        }

    # 仍在跟进：按新读数重算等级。
    new_payload["assessment"] = assess({**payload, "concentration": concentration})
    return {
        "kind": "recalculate",
        "new_status": None,
        "new_payload": new_payload,
        "event_payload": {"reading_id": reading["id"], "assessment": new_payload["assessment"]},
    }


def _need_status(item, allowed):
    if item["status"] not in allowed:
        raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % item["status"])


def _text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def apply_action(item, action, payload, actor, role):
    status = item["status"]
    current = dict(item["payload"])

    if action == "verify":
        _need_status(item, {"detected", "verified"})
        sample_count = int(payload.get("sample_count", 0) or 0)
        if sample_count < 1:
            raise DomainError("sample_required", "需要至少一份复检样本", 409)
        current["assessment"] = assess(current)
        current["verification"] = {"sample_count": sample_count, "note": payload.get("note", "")}
        return "verified", current, {"assessment": current["assessment"], "verification": current["verification"]}

    if action == "advise":
        _need_status(item, {"verified", "advisory"})
        notice_id = _text(payload, "notice_id")
        notice = {
            "notice_id": notice_id,
            "kind": _text(payload, "kind"),
            "message": _text(payload, "message"),
        }
        notices = current.setdefault("notifications", [])
        if any(existing.get("notice_id") == notice_id for existing in notices):
            raise DomainError("duplicate_notification", "同一通知编号不能重复发送", 409)
        notices.append(notice)
        return "advisory", current, {"notice": notice}

    if action == "switch_source":
        _need_status(item, {"verified", "advisory", "flushing", "disinfected", "sampled", "switched"})
        alternate = _text(payload, "alternate_source_id")
        current["alternate_source_id"] = alternate
        return "switched", current, {"alternate_source_id": alternate}

    if action == "flush":
        _need_status(item, {"advisory", "flushing", "switched"})
        zone_id = _text(payload, "zone_id")
        current.setdefault("response_actions", []).append({"type": "flush", "zone_id": zone_id})
        return "flushing", current, {"zone_id": zone_id, "type": "flush"}

    if action == "disinfect":
        _need_status(item, {"flushing", "disinfected"})
        if not payload.get("completed"):
            raise DomainError("disinfection_incomplete", "消毒尚未完成", 409)
        zone_id = _text(payload, "zone_id")
        current.setdefault("response_actions", []).append({"type": "disinfect", "zone_id": zone_id})
        return "disinfected", current, {"zone_id": zone_id, "type": "disinfect"}

    if action == "sample":
        _need_status(item, {"disinfected", "sampled", "pending_reinspection"})
        result = {
            "sample_id": _text(payload, "sample_id"),
            "zone_id": _text(payload, "zone_id"),
            "concentration": float(payload.get("concentration", 0)),
        }
        if result["concentration"] < 0:
            raise DomainError("invalid_concentration", "浓度不能为负数")
        current.setdefault("sample_results", []).append(result)
        return "sampled", current, {"sample_result": result}

    if action == "restore":
        _need_status(item, {"sampled", "pending_reinspection"})
        if not payload.get("all_zones_cleared"):
            raise DomainError("zones_not_cleared", "仍有区域未完成水质恢复", 409)
        limit = float(current.get("limit", 0))
        results = current.get("sample_results", [])
        if not results or any(float(result["concentration"]) > limit for result in results):
            raise DomainError("quality_not_met", "复检结果未全部达到限值", 409)
        current["restoration"] = {"actor": actor, "note": payload.get("note", ""), "at": now_iso()}
        current.pop("reopen", None)
        return "restored", current, {"restoration": current["restoration"]}

    if action == "reconfirm":
        _need_status(item, {"reconfirming"})
        current["restoration"] = {"actor": actor, "note": payload.get("note", ""), "at": now_iso()}
        return "restored", current, {"restoration": current["restoration"]}

    if action == "cancel":
        _need_status(item, {"detected", "verified"})
        reason = _text(payload, "reason")
        current["cancellation"] = {"reason": reason, "actor": actor}
        return "cancelled", current, {"reason": reason}

    raise DomainError("unknown_action", "不支持的操作")
