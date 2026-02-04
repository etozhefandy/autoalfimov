from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fb_report.constants import ALMATY_TZ, DATA_DIR


def _atomic_write_json(path: str, obj: dict) -> None:
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _plans_path() -> str:
    return os.path.join(DATA_DIR, "budget_plans.json")


def _ensure_schema(st: Any) -> Dict[str, Any]:
    if not isinstance(st, dict):
        st = {}
    plans = st.get("plans")
    if not isinstance(plans, list):
        plans = []
    norm: List[Dict[str, Any]] = []
    for it in plans:
        if isinstance(it, dict):
            norm.append(dict(it))
    st["plans"] = norm
    return st


def load_budget_plans() -> Dict[str, Any]:
    path = _plans_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception:
        obj = {}
    return _ensure_schema(obj)


def save_budget_plans(st: Dict[str, Any]) -> None:
    path = _plans_path()
    _atomic_write_json(path, _ensure_schema(st))


def _now_ts() -> int:
    try:
        return int(time.time())
    except Exception:
        return 0


def _now_iso() -> str:
    try:
        return datetime.now(ALMATY_TZ).isoformat()
    except Exception:
        try:
            return datetime.utcnow().isoformat()
        except Exception:
            return ""


def new_plan_id() -> str:
    return uuid.uuid4().hex[:12]


def _ensure_plan_defaults(plan: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(plan or {})
    p.setdefault("plan_id", "")
    p.setdefault("scope_type", "ACCOUNT")
    p.setdefault("account_id", "")
    p.setdefault("name", "")
    p.setdefault("period_type", "MONTH")
    p.setdefault("budget_total_usd", None)
    p.setdefault("is_enabled", True)
    p.setdefault("excluded_campaign_ids", [])
    p.setdefault("excluded_adset_ids", [])
    p.setdefault("bundle_campaign_ids", [])
    p.setdefault("locked_adset_limits", {})
    p.setdefault("created_at", None)
    p.setdefault("updated_at", None)

    if not isinstance(p.get("excluded_campaign_ids"), list):
        p["excluded_campaign_ids"] = []
    if not isinstance(p.get("excluded_adset_ids"), list):
        p["excluded_adset_ids"] = []
    if not isinstance(p.get("bundle_campaign_ids"), list):
        p["bundle_campaign_ids"] = []
    if not isinstance(p.get("locked_adset_limits"), dict):
        p["locked_adset_limits"] = {}

    p["excluded_campaign_ids"] = [str(x) for x in p.get("excluded_campaign_ids") or [] if str(x).strip()]
    p["excluded_adset_ids"] = [str(x) for x in p.get("excluded_adset_ids") or [] if str(x).strip()]
    p["bundle_campaign_ids"] = [str(x) for x in p.get("bundle_campaign_ids") or [] if str(x).strip()]

    lock = p.get("locked_adset_limits") or {}
    norm_lock: Dict[str, Any] = {}
    if isinstance(lock, dict):
        for k, v in lock.items():
            adset_id = str(k or "").strip()
            if not adset_id or not isinstance(v, dict):
                continue
            item = dict(v)
            item.setdefault("locked", True)
            item.setdefault("min_usd_day", None)
            item.setdefault("max_usd_day", None)
            norm_lock[adset_id] = item
    p["locked_adset_limits"] = norm_lock

    return p


def list_budget_plans(*, account_id: Optional[str] = None) -> List[Dict[str, Any]]:
    st = load_budget_plans()
    plans = st.get("plans") or []
    out: List[Dict[str, Any]] = []
    for it in plans:
        if not isinstance(it, dict):
            continue
        p = _ensure_plan_defaults(it)
        if account_id and str(p.get("account_id") or "") != str(account_id):
            continue
        out.append(p)
    out.sort(key=lambda x: str(x.get("updated_at") or x.get("created_at") or ""), reverse=True)
    return out


def get_budget_plan(plan_id: str) -> Optional[Dict[str, Any]]:
    pid = str(plan_id or "").strip()
    if not pid:
        return None
    for p in list_budget_plans():
        if str(p.get("plan_id") or "") == pid:
            return dict(p)
    return None


def upsert_budget_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    p = _ensure_plan_defaults(plan)
    pid = str(p.get("plan_id") or "").strip()
    if not pid:
        pid = new_plan_id()
        p["plan_id"] = pid

    now_iso = _now_iso()
    now_ts = _now_ts()
    if not p.get("created_at"):
        p["created_at"] = now_iso
        p["created_at_ts"] = now_ts
    p["updated_at"] = now_iso
    p["updated_at_ts"] = now_ts

    st = load_budget_plans()
    plans = st.get("plans") if isinstance(st.get("plans"), list) else []

    replaced = False
    new_plans: List[Dict[str, Any]] = []
    for it in plans:
        if not isinstance(it, dict):
            continue
        if str(it.get("plan_id") or "") == pid:
            new_plans.append(dict(p))
            replaced = True
        else:
            new_plans.append(dict(it))

    if not replaced:
        new_plans.append(dict(p))

    st["plans"] = new_plans
    save_budget_plans(st)
    return dict(p)


def set_budget_plan_enabled(plan_id: str, enabled: bool) -> Optional[Dict[str, Any]]:
    p = get_budget_plan(plan_id)
    if not isinstance(p, dict):
        return None
    p["is_enabled"] = bool(enabled)
    return upsert_budget_plan(p)


def delete_budget_plan(plan_id: str) -> bool:
    pid = str(plan_id or "").strip()
    if not pid:
        return False

    st = load_budget_plans()
    plans = st.get("plans") if isinstance(st.get("plans"), list) else []

    new_plans: List[Dict[str, Any]] = []
    removed = False
    for it in plans:
        if not isinstance(it, dict):
            continue
        if str(it.get("plan_id") or "") == pid:
            removed = True
            continue
        new_plans.append(dict(it))

    if not removed:
        return False

    st["plans"] = new_plans
    save_budget_plans(st)
    return True
