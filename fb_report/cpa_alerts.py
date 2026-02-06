from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dt_time
from typing import Any, Dict, List, Optional, Tuple

from fb_report.constants import ALMATY_TZ, DATA_DIR, SUPERADMIN_USER_ID
from fb_report.storage import get_account_name, load_accounts
from services.ai_focus import ask_deepseek, sanitize_ai_text
from services.analytics import (
    count_leads_from_actions,
    count_started_conversations_from_actions,
)
from services.facebook_api import allow_fb_api_calls, fetch_insights_bulk
from services.heatmap_store import (
    find_latest_ready_snapshots,
    get_heatmap_dataset,
    prev_full_hour_window,
)

_LOG = logging.getLogger(__name__)

CPA_ALERTS_FILE = os.path.join(DATA_DIR, "cpa_alerts.json")
CPA_ALERTS_DAILY_CACHE_FILE = os.path.join(DATA_DIR, "cpa_alerts_daily_cache.json")


def _atomic_write_json(path: str, obj: dict) -> None:
    tmp = f"{path}.tmp"
    bak = f"{path}.bak"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    try:
        if os.path.exists(path):
            shutil.copy2(path, bak)
    except Exception:
        pass
    os.replace(tmp, path)


def _load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _ensure_state_schema(st: dict) -> dict:
    if not isinstance(st, dict):
        st = {}
    st.setdefault("enabled", True)
    st.setdefault("timezone", "Asia/Almaty")
    if not isinstance(st.get("targets"), list):
        st["targets"] = []
    return st


def load_cpa_alerts_state() -> dict:
    return _ensure_state_schema(_load_json(CPA_ALERTS_FILE))


def save_cpa_alerts_state(st: dict) -> None:
    try:
        _atomic_write_json(CPA_ALERTS_FILE, _ensure_state_schema(st))
    except Exception:
        pass


def _new_rule_id(prefix: str, parts: List[str]) -> str:
    raw = prefix + ":" + ":".join([str(x) for x in (parts or [])])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def ensure_cpa_alerts_state_initialized() -> None:
    st = load_cpa_alerts_state()
    if not isinstance(st.get("targets"), list):
        st["targets"] = []
    if st.get("enabled") is None:
        st["enabled"] = True
    if not st.get("timezone"):
        st["timezone"] = "Asia/Almaty"
    save_cpa_alerts_state(st)


def ensure_default_rules_from_legacy_accounts() -> None:
    """Best-effort migration from legacy per-account alerts settings.

    Creates ACCOUNT-scope rules if there are no rules yet.
    """

    st = load_cpa_alerts_state()
    targets = st.get("targets") if isinstance(st.get("targets"), list) else []
    if targets:
        return

    store = load_accounts() or {}
    new_targets: List[Dict[str, Any]] = []
    for aid, row in (store or {}).items():
        if not isinstance(row, dict):
            continue
        alerts = (row or {}).get("alerts") or {}
        if not isinstance(alerts, dict):
            continue
        if not bool(alerts.get("enabled", False)):
            continue

        target = 0.0
        try:
            target = float(alerts.get("account_cpa", alerts.get("target_cpl", 0.0)) or 0.0)
        except Exception:
            target = 0.0
        if target <= 0:
            continue

        freq = str(alerts.get("freq", "3x") or "3x")
        schedule = "HOURLY" if freq == "hourly" else "DAILY"
        rid = _new_rule_id("legacy", ["ACCOUNT", str(aid), schedule, "BLENDED"])
        nm = str(get_account_name(str(aid)) or aid)
        name = f"{nm} ({schedule.lower()})"

        new_targets.append(
            {
                "id": rid,
                "name": name,
                "scope_type": "ACCOUNT",
                "scope_id": str(aid),
                "result_type": "BLENDED",
                "target_cpa_usd": float(target),
                "schedule": schedule,
                "active_hours": {"from": "10:30", "to": "21:30"},
                "send_time": "10:45",
                "min_spend_to_trigger_usd": 0.0,
                "top_ads_limit": 5,
                "enabled": True,
                "last_run_at": None,
            }
        )

    if new_targets:
        st["targets"] = new_targets
        save_cpa_alerts_state(st)


def _ensure_rule_defaults(r: Dict[str, Any]) -> Dict[str, Any]:
    rule = dict(r or {})
    rule.setdefault("id", "")
    rule.setdefault("name", "")
    rule.setdefault("scope_type", "ACCOUNT")
    rule.setdefault("scope_id", "")
    rule.setdefault("account_id", None)
    rule.setdefault("result_type", "BLENDED")
    rule.setdefault("target_cpa_usd", 0.0)
    rule.setdefault("schedule", "DAILY")
    rule.setdefault("active_hours", {"from": "10:30", "to": "21:30"})
    rule.setdefault("send_time", "10:45")
    rule.setdefault("min_spend_to_trigger_usd", 0.0)
    rule.setdefault("top_ads_limit", 5)
    rule.setdefault("enabled", True)
    rule.setdefault("last_run_at", None)
    return rule


def list_rules(*, enabled_only: bool = False) -> List[Dict[str, Any]]:
    st = load_cpa_alerts_state()
    out: List[Dict[str, Any]] = []
    for r in st.get("targets") or []:
        if not isinstance(r, dict):
            continue
        rr = _ensure_rule_defaults(r)
        if enabled_only and not bool(rr.get("enabled") is True):
            continue
        out.append(rr)
    return out


def get_rule(rule_id: str) -> Optional[Dict[str, Any]]:
    rid = str(rule_id or "").strip()
    if not rid:
        return None
    for r in list_rules(enabled_only=False):
        if str(r.get("id") or "").strip() == rid:
            return r
    return None


def create_default_rule(*, name: str = "") -> Dict[str, Any]:
    rid = hashlib.sha1(f"new_rule:{time.time()}".encode("utf-8")).hexdigest()[:12]
    rule = {
        "id": rid,
        "name": str(name or "").strip() or f"Rule {rid}",
        "scope_type": "ACCOUNT",
        "scope_id": "",
        "account_id": None,
        "result_type": "BLENDED",
        "target_cpa_usd": 0.0,
        "schedule": "DAILY",
        "active_hours": {"from": "10:30", "to": "21:30"},
        "send_time": "10:45",
        "min_spend_to_trigger_usd": 0.0,
        "top_ads_limit": 5,
        "enabled": True,
        "last_run_at": None,
    }
    return _ensure_rule_defaults(rule)


def upsert_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    rr = _ensure_rule_defaults(rule)
    rid = str(rr.get("id") or "").strip()
    if not rid:
        rr["id"] = hashlib.sha1(f"new_rule:{time.time()}".encode("utf-8")).hexdigest()[:12]
        rid = str(rr.get("id") or "").strip()

    st = load_cpa_alerts_state()
    targets = st.get("targets") if isinstance(st.get("targets"), list) else []
    out_targets: List[Dict[str, Any]] = []
    replaced = False
    for it in targets:
        if not isinstance(it, dict):
            continue
        if str(it.get("id") or "").strip() == rid:
            out_targets.append(rr)
            replaced = True
        else:
            out_targets.append(it)
    if not replaced:
        out_targets.append(rr)
    st["targets"] = out_targets
    save_cpa_alerts_state(st)
    return rr


def delete_rule(rule_id: str) -> bool:
    rid = str(rule_id or "").strip()
    if not rid:
        return False
    st = load_cpa_alerts_state()
    targets = st.get("targets") if isinstance(st.get("targets"), list) else []
    out_targets: List[Dict[str, Any]] = []
    removed = False
    for it in targets:
        if not isinstance(it, dict):
            continue
        if str(it.get("id") or "").strip() == rid:
            removed = True
            continue
        out_targets.append(it)
    st["targets"] = out_targets
    save_cpa_alerts_state(st)
    return bool(removed)


def toggle_rule_enabled(rule_id: str) -> Optional[Dict[str, Any]]:
    rr = get_rule(rule_id)
    if not rr:
        return None
    rr2 = dict(rr)
    rr2["enabled"] = not bool(rr.get("enabled") is True)
    return upsert_rule(rr2)


def set_global_enabled(enabled: bool) -> None:
    st = load_cpa_alerts_state()
    st["enabled"] = bool(enabled)
    save_cpa_alerts_state(st)


def _daily_cache_load() -> dict:
    st = _load_json(CPA_ALERTS_DAILY_CACHE_FILE)
    if not isinstance(st, dict):
        st = {}
    st.setdefault("items", {})
    if not isinstance(st.get("items"), dict):
        st["items"] = {}
    return st


def _daily_cache_save(st: dict) -> None:
    try:
        _atomic_write_json(CPA_ALERTS_DAILY_CACHE_FILE, st if isinstance(st, dict) else {})
    except Exception:
        pass


def _version_hash() -> str:
    # Bump if cache format changes.
    raw = "cpa_alerts_v1"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]


def _daily_cache_key(
    *,
    scope_type: str,
    scope_id: str,
    period_key: str,
    result_type: str,
    level: str,
    kind: str,
) -> str:
    return f"daily:{scope_type}:{scope_id}:{period_key}:{result_type}:{level}:{kind}:{_version_hash()}"


def _result_count_from_actions(actions: Dict[str, float], *, result_type: str) -> int:
    rt = str(result_type or "BLENDED").upper().strip()
    msgs = int(count_started_conversations_from_actions(actions or {}) or 0)
    leads = int(count_leads_from_actions(actions or {}, aid=None, lead_action_type=None) or 0)
    if rt == "MESSAGES":
        return int(msgs)
    if rt == "SUBMIT_APPLICATION":
        return int(leads)
    return int(msgs + leads)


def _parse_hhmm(s: str) -> Tuple[int, int]:
    try:
        hh, mm = str(s).split(":", 1)
        return int(hh), int(mm)
    except Exception:
        return 0, 0


def _in_active_hours(rule: dict, *, now: datetime) -> bool:
    ah = rule.get("active_hours") or {}
    if not isinstance(ah, dict):
        ah = {}
    fr = str(ah.get("from") or "10:30")
    to = str(ah.get("to") or "21:30")
    fh, fm = _parse_hhmm(fr)
    th, tm = _parse_hhmm(to)
    cur = now.replace(second=0, microsecond=0)
    start = cur.replace(hour=fh, minute=fm)
    end = cur.replace(hour=th, minute=tm)
    return bool(start <= cur <= end)


def _rule_scope_label(rule: dict) -> str:
    st = str(rule.get("scope_type") or "ACCOUNT").upper().strip()
    sid = str(rule.get("scope_id") or "").strip()
    if st == "ACCOUNT":
        return str(get_account_name(sid) or sid)
    return sid


def _find_aid_for_entity_from_snapshots(*, entity_kind: str, entity_id: str, now: datetime) -> Optional[str]:
    ek = str(entity_kind or "").lower().strip()
    eid = str(entity_id or "").strip()
    if not ek or not eid:
        return None

    store = load_accounts() or {}
    aids = [str(aid) for aid, row in (store or {}).items() if isinstance(row, dict) and bool(row.get("enabled", True))]
    # Deterministic order.
    aids.sort()

    for aid in aids:
        try:
            snaps = find_latest_ready_snapshots(str(aid), max_hours=96, now=now) or []
        except Exception:
            snaps = []
        if not snaps:
            continue
        for s in snaps:
            rows = (s or {}).get("rows") or []
            if not isinstance(rows, list):
                continue
            for r in rows:
                if not isinstance(r, dict):
                    continue
                if ek == "campaign" and str(r.get("campaign_id") or "") == eid:
                    return str(aid)
                if ek == "adset" and str(r.get("adset_id") or "") == eid:
                    return str(aid)
    return None


def _resolve_entity_group_campaigns(group_id: str) -> Tuple[Optional[str], List[str], str]:
    gid = str(group_id or "").strip()
    if not gid:
        return None, [], "group_id_empty"

    st = load_accounts() or {}
    for aid, row in (st or {}).items():
        ap = (row or {}).get("autopilot") or {}
        groups = ap.get("campaign_groups") or {}
        if not isinstance(groups, dict):
            continue
        grp = groups.get(gid)
        if not isinstance(grp, dict):
            continue
        cids = grp.get("campaign_ids") or []
        if not isinstance(cids, list):
            cids = []
        ids = [str(x) for x in cids if str(x).strip()]
        if ids:
            return str(aid), ids, "ok"
    return None, [], "group_not_found"


def _period_key_since_until(since: str, until: str) -> str:
    return f"{str(since)}..{str(until)}"


@dataclass
class PeriodDef:
    label: str
    since: str
    until: str


def _periods_for_mode(*, mode: str, now: datetime) -> Tuple[PeriodDef, Optional[PeriodDef]]:
    m = str(mode or "DAILY").lower().strip()

    def _fmt(d: datetime) -> str:
        return d.date().strftime("%Y-%m-%d")

    # Full days: end at yesterday.
    yday = (now.date() - timedelta(days=1))

    if m == "daily":
        p1 = PeriodDef(label="вчера", since=str(yday), until=str(yday))
        p0d = (now.date() - timedelta(days=2))
        p0 = PeriodDef(label="позавчера", since=str(p0d), until=str(p0d))
        return p1, p0

    if m in {"days_3", "3days", "days3"}:
        end = yday
        start = end - timedelta(days=2)
        main = PeriodDef(label="последние 3 дня", since=str(start), until=str(end))
        # аналогичные дни прошлой недели
        cmp_end = end - timedelta(days=7)
        cmp_start = start - timedelta(days=7)
        cmp = PeriodDef(label="3 дня прошлой недели", since=str(cmp_start), until=str(cmp_end))
        return main, cmp

    if m == "weekly":
        end = yday
        start = end - timedelta(days=6)
        main = PeriodDef(label="последние 7 дней", since=str(start), until=str(end))
        cmp_end = end - timedelta(days=7)
        cmp_start = start - timedelta(days=7)
        cmp = PeriodDef(label="предыдущие 7 дней", since=str(cmp_start), until=str(cmp_end))
        return main, cmp

    # hourly handled separately.
    main = PeriodDef(label="hourly", since="", until="")
    return main, None


def _collect_snapshots_for_dates(*, aid: str, dates: set[str], now: datetime) -> List[Dict[str, Any]]:
    if not dates:
        return []
    # Fetch enough history.
    try:
        days = len(dates)
        max_hours = max(24, int(days) * 24 + 72)
    except Exception:
        max_hours = 120

    snaps = find_latest_ready_snapshots(str(aid), max_hours=max_hours, now=now) or []
    out: List[Dict[str, Any]] = []
    for s in snaps:
        d = str((s or {}).get("date") or "")
        if d and d in dates:
            out.append(s)
    return out


def _rows_for_scope_from_snapshot_rows(
    rows: List[Dict[str, Any]],
    *,
    scope_type: str,
    scope_id: str,
    campaign_ids_for_group: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    st = str(scope_type or "ACCOUNT").upper().strip()
    sid = str(scope_id or "").strip()

    if st == "ACCOUNT":
        return list(rows or [])

    if st == "CAMPAIGN":
        return [r for r in (rows or []) if str((r or {}).get("campaign_id") or "") == sid]

    if st == "ADSET":
        return [r for r in (rows or []) if str((r or {}).get("adset_id") or "") == sid]

    if st == "ENTITY_GROUP":
        cset = campaign_ids_for_group or set()
        if not cset:
            return []
        return [r for r in (rows or []) if str((r or {}).get("campaign_id") or "") in cset]

    return []


def _sum_snapshot_rows(rows: List[Dict[str, Any]], *, result_type: str) -> Tuple[float, int]:
    spend = 0.0
    results = 0
    rt = str(result_type or "BLENDED").upper().strip()
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        try:
            spend += float(r.get("spend") or 0.0)
        except Exception:
            pass

        if rt == "MESSAGES":
            try:
                results += int(r.get("msgs") or r.get("started_conversations") or 0)
            except Exception:
                pass
        elif rt == "SUBMIT_APPLICATION":
            try:
                results += int(r.get("leads") or r.get("website_submit_applications") or 0)
            except Exception:
                pass
        else:
            try:
                t = r.get("total")
                if t is None:
                    t = int(r.get("msgs") or 0) + int(r.get("leads") or 0)
                results += int(t or 0)
            except Exception:
                pass
    return float(spend), int(results)


def _fetch_overall_via_fb(
    *,
    aid: str,
    scope_type: str,
    scope_id: str,
    result_type: str,
    since: str,
    until: str,
    campaign_ids_for_group: Optional[List[str]] = None,
) -> Tuple[float, int, str]:
    st = str(scope_type or "ACCOUNT").upper().strip()
    sid = str(scope_id or "").strip()
    aid_s = str(aid or "").strip()
    if not aid_s:
        return 0.0, 0, "account_id_missing"

    if st == "ENTITY_GROUP":
        cids = [str(x) for x in (campaign_ids_for_group or []) if str(x).strip()]
        if not cids:
            return 0.0, 0, "group_empty"
        # We need account_id to call FB; for entity group we assume scope_id resolution done.
        # Caller passes resolved account_id via aid.
        aid = aid_s
        params_extra = {
            "filtering": [{"field": "campaign.id", "operator": "IN", "value": cids}],
            "action_report_time": "conversion",
            "use_unified_attribution_setting": True,
        }
        level = "campaign"
        fields = ["campaign_id", "spend", "actions"]
    else:
        if st == "ACCOUNT":
            aid = aid_s
            level = "account"
            fields = ["spend", "actions"]
            params_extra = {
                "action_report_time": "conversion",
                "use_unified_attribution_setting": True,
            }
        elif st == "CAMPAIGN":
            aid = aid_s
            cid = sid
            level = "campaign"
            fields = ["campaign_id", "spend", "actions"]
            params_extra = {
                "filtering": [{"field": "campaign.id", "operator": "IN", "value": [str(cid)]}],
                "action_report_time": "conversion",
                "use_unified_attribution_setting": True,
            }
        elif st == "ADSET":
            aid = aid_s
            adset_id = sid
            level = "adset"
            fields = ["adset_id", "spend", "actions"]
            params_extra = {
                "filtering": [{"field": "adset.id", "operator": "IN", "value": [str(adset_id)]}],
                "action_report_time": "conversion",
                "use_unified_attribution_setting": True,
            }
        else:
            return 0.0, 0, "scope_not_supported"

    with allow_fb_api_calls(reason="cpa_alert:overall"):
        rows = fetch_insights_bulk(
            str(aid),
            period={"since": str(since), "until": str(until)},
            level=str(level),
            fields=list(fields),
            params_extra=dict(params_extra),
        )

    spend_total = 0.0
    results_total = 0
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        try:
            spend_total += float(r.get("spend") or 0.0)
        except Exception:
            pass

        actions = r.get("actions") or []
        amap: Dict[str, float] = {}
        try:
            for a in actions or []:
                if not isinstance(a, dict):
                    continue
                at = str(a.get("action_type") or "")
                if not at:
                    continue
                try:
                    v = float(a.get("value") or 0.0)
                except Exception:
                    v = 0.0
                amap[at] = amap.get(at, 0.0) + float(v)
        except Exception:
            amap = {}

        results_total += _result_count_from_actions(amap, result_type=result_type)

    return float(spend_total), int(results_total), "ok"


def _ads_manager_ad_url(*, account_id: str, ad_id: str) -> str:
    aid = str(account_id or "")
    act_num = aid.replace("act_", "")
    return (
        "https://www.facebook.com/adsmanager/manage/ads"
        f"?act={act_num}&selected_ad_ids={str(ad_id)}"
    )


def _fetch_ads_via_fb(
    *,
    aid: str,
    scope_type: str,
    scope_id: str,
    result_type: str,
    since: str,
    until: str,
    campaign_ids_for_group: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    st = str(scope_type or "ACCOUNT").upper().strip()
    sid = str(scope_id or "").strip()

    params_extra = {
        "action_report_time": "conversion",
        "use_unified_attribution_setting": True,
    }

    if st == "CAMPAIGN":
        params_extra["filtering"] = [{"field": "campaign.id", "operator": "IN", "value": [sid]}]
    elif st == "ADSET":
        params_extra["filtering"] = [{"field": "adset.id", "operator": "IN", "value": [sid]}]
    elif st == "ENTITY_GROUP":
        cids = [str(x) for x in (campaign_ids_for_group or []) if str(x).strip()]
        if not cids:
            return [], "group_empty"
        params_extra["filtering"] = [{"field": "campaign.id", "operator": "IN", "value": cids}]

    fields = [
        "ad_id",
        "ad_name",
        "adset_id",
        "adset_name",
        "campaign_id",
        "campaign_name",
        "spend",
        "actions",
    ]

    with allow_fb_api_calls(reason="cpa_alert:ads"):
        rows = fetch_insights_bulk(
            str(aid),
            period={"since": str(since), "until": str(until)},
            level="ad",
            fields=fields,
            params_extra=params_extra,
        )

    out: List[Dict[str, Any]] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        ad_id = str(r.get("ad_id") or "").strip()
        if not ad_id:
            continue

        try:
            spend = float(r.get("spend") or 0.0)
        except Exception:
            spend = 0.0

        actions = r.get("actions") or []
        amap: Dict[str, float] = {}
        try:
            for a in actions or []:
                if not isinstance(a, dict):
                    continue
                at = str(a.get("action_type") or "")
                if not at:
                    continue
                try:
                    v = float(a.get("value") or 0.0)
                except Exception:
                    v = 0.0
                amap[at] = amap.get(at, 0.0) + float(v)
        except Exception:
            amap = {}

        results = _result_count_from_actions(amap, result_type=result_type)
        cpa = (float(spend) / float(results)) if results > 0 and spend > 0 else None

        out.append(
            {
                "ad_id": ad_id,
                "ad_name": str(r.get("ad_name") or ad_id),
                "adset_id": str(r.get("adset_id") or ""),
                "adset_name": str(r.get("adset_name") or ""),
                "campaign_id": str(r.get("campaign_id") or ""),
                "campaign_name": str(r.get("campaign_name") or ""),
                "spend": float(spend),
                "results": int(results),
                "cpa": float(cpa) if cpa is not None else None,
                "url": _ads_manager_ad_url(account_id=str(aid), ad_id=str(ad_id)),
            }
        )

    return out, "ok"


def _pick_top_ads(
    *,
    ads: List[Dict[str, Any]],
    target_cpa_usd: float,
    min_spend_to_trigger_usd: float,
    limit: int,
) -> List[Dict[str, Any]]:
    tgt = float(target_cpa_usd or 0.0)
    min_sp = float(min_spend_to_trigger_usd or 0.0)

    def _is_candidate(a: dict) -> bool:
        sp = float(a.get("spend") or 0.0)
        res = int(a.get("results") or 0)
        cpa = a.get("cpa")
        if sp < max(min_sp, tgt):
            return False
        if res <= 0:
            return True
        if cpa is None:
            return False
        return float(cpa) > tgt

    cand = [a for a in (ads or []) if isinstance(a, dict) and _is_candidate(a)]

    def _sort_key(a: dict) -> Tuple[float, float]:
        res = int(a.get("results") or 0)
        cpa = float(a.get("cpa") or 0.0)
        sp = float(a.get("spend") or 0.0)
        # For results>0 sort by cpa desc; for 0-results, cpa=0 and spend desc dominates.
        cpa_sort = cpa if res > 0 else 0.0
        return (cpa_sort, sp)

    cand.sort(key=_sort_key, reverse=True)
    return cand[: max(0, int(limit or 5))]


async def _ai_compare_summary(*, rule_name: str, main: dict, comp: dict) -> str:
    try:
        system_msg = (
            "Ты — аналитик по Facebook Ads. "
            "Пиши коротко, 3–6 строк, в стиле Telegram. "
            "Нельзя автодействий. Только объяснение причин и что изменилось."
        )
        user_msg = (
            f"CPA Alert: {rule_name}\n"
            "Сравни периоды и дай вывод.\n\n"
            f"MAIN: {json.dumps(main, ensure_ascii=False)}\n"
            f"COMP: {json.dumps(comp, ensure_ascii=False)}\n"
        )
        ds = await ask_deepseek(
            [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            json_mode=False,
            andrey_tone=True,
            temperature=0.4,
            max_tokens=300,
        )
        ch = (ds.get("choices") or [{}])[0]
        txt = (ch.get("message") or {}).get("content") or ""
        cleaned = sanitize_ai_text(str(txt))
        return cleaned.strip()
    except Exception:
        return ""


def _calc_cpa(spend: float, results: int) -> Optional[float]:
    try:
        if int(results) <= 0:
            return None
        if float(spend) <= 0:
            return None
        return float(spend) / float(results)
    except Exception:
        return None


def _trigger_status(*, spend: float, results: int, cpa: Optional[float], target: float, min_spend: float) -> Tuple[bool, str]:
    tgt = float(target or 0.0)
    ms = float(min_spend or 0.0)
    if int(results) > 0 and cpa is not None and float(cpa) > tgt:
        return True, "❗️ превышение"
    # 0 results
    if int(results) <= 0 and float(spend) >= max(tgt, ms):
        return True, "⚠️ 0 результатов"
    return False, "OK"


async def build_cpa_alert_message(
    *,
    rule: Dict[str, Any],
    mode: str,
    now: datetime,
    test: bool,
) -> Tuple[Optional[str], Dict[str, Any]]:
    r = _ensure_rule_defaults(rule)
    if not bool(r.get("enabled") is True):
        return None, {"skip": "disabled"}

    schedule = str(r.get("schedule") or "DAILY").upper().strip()
    if not test and schedule != str(mode).upper().strip():
        return None, {"skip": "mode_mismatch"}

    scope_type = str(r.get("scope_type") or "ACCOUNT").upper().strip()
    scope_id = str(r.get("scope_id") or "").strip()
    result_type = str(r.get("result_type") or "BLENDED").upper().strip()
    target = float(r.get("target_cpa_usd") or 0.0)
    min_spend = float(r.get("min_spend_to_trigger_usd") or 0.0)
    top_limit = int(r.get("top_ads_limit") or 5)

    if target <= 0:
        return None, {"skip": "target_cpa_missing"}

    if scope_type == "ACCOUNT" and not scope_id:
        return None, {"skip": "scope_id_missing"}

    group_aid = None
    group_campaigns: List[str] = []
    group_reason = ""
    campaign_set: Optional[set[str]] = None

    # Resolve aid for all modes.
    aid = ""
    if scope_type == "ACCOUNT":
        aid = scope_id
    elif scope_type == "ENTITY_GROUP":
        group_aid, group_campaigns, group_reason = _resolve_entity_group_campaigns(scope_id)
        if not group_aid:
            return None, {"skip": f"entity_group_resolve:{group_reason}"}
        aid = str(group_aid)
        campaign_set = set([str(x) for x in (group_campaigns or []) if str(x).strip()])
    elif scope_type == "CAMPAIGN":
        aid = str(r.get("account_id") or "").strip()
        if not aid:
            aid = str(_find_aid_for_entity_from_snapshots(entity_kind="campaign", entity_id=scope_id, now=now) or "")
        if not aid:
            return None, {"skip": "campaign_account_not_resolved"}
    elif scope_type == "ADSET":
        aid = str(r.get("account_id") or "").strip()
        if not aid:
            aid = str(_find_aid_for_entity_from_snapshots(entity_kind="adset", entity_id=scope_id, now=now) or "")
        if not aid:
            return None, {"skip": "adset_account_not_resolved"}
    else:
        return None, {"skip": "scope_type_not_supported"}

    source = ""

    if str(mode).upper().strip() == "HOURLY":
        if not test and not _in_active_hours(r, now=now):
            return None, {"skip": "outside_active_hours"}

        win = prev_full_hour_window(now=now) or {}
        date_str = str(win.get("date") or "")
        hour_int = int(win.get("hour") or 0)

        ds, st, reason, _meta = get_heatmap_dataset(str(aid), date_str=date_str, hours=[hour_int])
        if st != "ready" or not ds:
            return None, {"skip": f"no_snapshot:{st}:{reason}"}

        rows = list((ds or {}).get("rows") or [])
        scoped = _rows_for_scope_from_snapshot_rows(
            rows,
            scope_type=scope_type,
            scope_id=scope_id,
            campaign_ids_for_group=campaign_set,
        )
        spend, results = _sum_snapshot_rows(scoped, result_type=result_type)
        cpa = _calc_cpa(spend, results)
        triggered, status = _trigger_status(
            spend=spend,
            results=results,
            cpa=cpa,
            target=target,
            min_spend=min_spend,
        )
        source = "почасовой кэш"

        meta = {
            "caller": "cpa_alert",
            "rule_id": r.get("id"),
            "scope_type": scope_type,
            "scope_id": scope_id,
            "schedule": schedule,
            "result_type": result_type,
            "target_cpa": target,
            "mode": "hourly",
            "source": "hourly_cache",
            "spend": spend,
            "results": results,
            "cpa": cpa,
            "triggered": triggered,
            "top_ads_found": 0,
        }

        _LOG.info(
            "caller=cpa_alert rule_id=%s scope_type=%s scope_id=%s schedule=%s result_type=%s target_cpa=%s mode=hourly source=hourly_cache spend=%.2f results=%s cpa=%s triggered=%s top_ads_found=0",
            str(r.get("id") or ""),
            scope_type,
            scope_id,
            schedule,
            result_type,
            f"{target:.2f}",
            float(spend),
            int(results),
            f"{cpa:.4f}" if cpa is not None else "None",
            str(triggered),
        )

        if not triggered and not test:
            return None, meta

        scope_label = _rule_scope_label(r)
        header = f"🚨 CPA Alert — {str(r.get('name') or r.get('id') or '')}".strip()
        if test:
            header = "🧪 TEST\n" + header

        window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"
        period_line = f"Период: последний час ({date_str} {window_label})"

        lines = [
            header,
            period_line,
            "",
            f"Scope: {scope_type} • {scope_label}",
            f"Spend: {spend:.2f} $" if spend > 0 else "Spend: —",
            f"Results: {int(results)} ({result_type})",
            (
                f"CPA / Target CPA: {(cpa or 0):.2f} $ / {target:.2f} $"
                if cpa is not None
                else f"CPA / Target CPA: — / {target:.2f} $"
            ),
            f"Статус триггера: {status}",
            "",
            f"ℹ️ Источник данных: {source}",
        ]
        return "\n".join(lines), meta

    # Non-hourly modes: daily / 3days / weekly.
    main_pd, cmp_pd = _periods_for_mode(mode=str(mode).lower(), now=now)
    if not main_pd.since:
        return None, {"skip": "mode_not_supported"}

    dates: set[str] = set()
    try:
        d0 = datetime.fromisoformat(str(main_pd.since)).date()
        d1 = datetime.fromisoformat(str(main_pd.until)).date()
        cur = d0
        while cur <= d1:
            dates.add(cur.strftime("%Y-%m-%d"))
            cur = cur + timedelta(days=1)
    except Exception:
        dates = set([str(main_pd.since), str(main_pd.until)])

    snaps = _collect_snapshots_for_dates(aid=str(aid), dates=dates, now=now)

    spend = 0.0
    results = 0
    if snaps:
        scoped_rows: List[Dict[str, Any]] = []
        for s in snaps:
            rr = list((s or {}).get("rows") or [])
            scoped_rows.extend(
                _rows_for_scope_from_snapshot_rows(
                    rr,
                    scope_type=scope_type,
                    scope_id=scope_id,
                    campaign_ids_for_group=campaign_set,
                )
            )
        spend, results = _sum_snapshot_rows(scoped_rows, result_type=result_type)
        source = "почасовой кэш"
        source_code = "hourly_cache"
    else:
        # Daily cache fallback.
        cache = _daily_cache_load()
        items = cache.get("items") if isinstance(cache.get("items"), dict) else {}
        pkey = _period_key_since_until(main_pd.since, main_pd.until)
        ck = _daily_cache_key(
            scope_type=scope_type,
            scope_id=scope_id,
            period_key=pkey,
            result_type=result_type,
            level="overall",
            kind="metrics",
        )
        hit = items.get(ck)
        if isinstance(hit, dict):
            try:
                spend = float(hit.get("spend") or 0.0)
            except Exception:
                spend = 0.0
            try:
                results = int(hit.get("results") or 0)
            except Exception:
                results = 0
            source = "дневной кэш"
            source_code = "daily_cache"
        else:
            # FB request + write cache.
            fb_scope_id = scope_id
            fb_scope_type = scope_type
            fb_campaigns = None
            if scope_type == "ENTITY_GROUP":
                fb_scope_type = "ENTITY_GROUP"
                fb_scope_id = aid
                fb_campaigns = group_campaigns

            spend, results, st_fb = _fetch_overall_via_fb(
                aid=str(aid),
                scope_type=fb_scope_type,
                scope_id=fb_scope_id,
                result_type=result_type,
                since=main_pd.since,
                until=main_pd.until,
                campaign_ids_for_group=fb_campaigns,
            )
            source = "прямой запрос Facebook API"
            source_code = "fb_request"

            if st_fb == "ok":
                items[ck] = {
                    "ts": int(time.time()),
                    "spend": float(spend),
                    "results": int(results),
                }
                cache["items"] = items
                _daily_cache_save(cache)

    cpa = _calc_cpa(spend, results)
    triggered, status = _trigger_status(
        spend=spend,
        results=results,
        cpa=cpa,
        target=target,
        min_spend=min_spend,
    )

    # Compare period metrics (best-effort; if missing -> skip AI).
    comp_block = None
    ai_text = ""
    if cmp_pd is not None:
        try:
            cache2 = _daily_cache_load()
            items2 = cache2.get("items") if isinstance(cache2.get("items"), dict) else {}
            pkey2 = _period_key_since_until(cmp_pd.since, cmp_pd.until)
            ck2 = _daily_cache_key(
                scope_type=scope_type,
                scope_id=scope_id,
                period_key=pkey2,
                result_type=result_type,
                level="overall",
                kind="metrics",
            )
            hit2 = items2.get(ck2)
            if isinstance(hit2, dict):
                sp2 = float(hit2.get("spend") or 0.0)
                re2 = int(hit2.get("results") or 0)
                cpa2 = _calc_cpa(sp2, re2)
                comp_block = {"spend": sp2, "results": re2, "cpa": cpa2, "label": cmp_pd.label}
            else:
                # FB request for comp period if no cache.
                fb_scope_id2 = scope_id
                fb_scope_type2 = scope_type
                fb_campaigns2 = None
                if scope_type == "ENTITY_GROUP":
                    fb_scope_type2 = "ENTITY_GROUP"
                    fb_scope_id2 = aid
                    fb_campaigns2 = group_campaigns
                sp2, re2, st_fb2 = _fetch_overall_via_fb(
                    aid=str(aid),
                    scope_type=fb_scope_type2,
                    scope_id=fb_scope_id2,
                    result_type=result_type,
                    since=cmp_pd.since,
                    until=cmp_pd.until,
                    campaign_ids_for_group=fb_campaigns2,
                )
                if st_fb2 == "ok":
                    items2[ck2] = {
                        "ts": int(time.time()),
                        "spend": float(sp2),
                        "results": int(re2),
                    }
                    cache2["items"] = items2
                    _daily_cache_save(cache2)
                cpa2 = _calc_cpa(sp2, re2)
                comp_block = {"spend": sp2, "results": re2, "cpa": cpa2, "label": cmp_pd.label}
        except Exception:
            comp_block = None

        if comp_block is not None:
            main_block = {"spend": spend, "results": results, "cpa": cpa, "label": main_pd.label}
            ai_text = await _ai_compare_summary(rule_name=str(r.get("name") or r.get("id") or ""), main=main_block, comp=comp_block)

    # Top ads block (always from daily cache or FB; does not use snapshots).
    ads: List[Dict[str, Any]] = []
    try:
        cache3 = _daily_cache_load()
        items3 = cache3.get("items") if isinstance(cache3.get("items"), dict) else {}
        pkey_ads = _period_key_since_until(main_pd.since, main_pd.until)
        ck_ads = _daily_cache_key(
            scope_type=scope_type,
            scope_id=scope_id,
            period_key=pkey_ads,
            result_type=result_type,
            level="ad",
            kind="ads",
        )
        hit_ads = items3.get(ck_ads)
        if isinstance(hit_ads, dict) and isinstance(hit_ads.get("items"), list):
            ads = list(hit_ads.get("items") or [])
        else:
            ads, st_ads = _fetch_ads_via_fb(
                aid=str(aid),
                scope_type=scope_type,
                scope_id=scope_id,
                result_type=result_type,
                since=main_pd.since,
                until=main_pd.until,
                campaign_ids_for_group=group_campaigns,
            )
            if st_ads == "ok":
                items3[ck_ads] = {"ts": int(time.time()), "items": list(ads)}
                cache3["items"] = items3
                _daily_cache_save(cache3)
    except Exception:
        ads = []

    top_ads = _pick_top_ads(
        ads=ads,
        target_cpa_usd=target,
        min_spend_to_trigger_usd=min_spend,
        limit=top_limit,
    )

    meta = {
        "caller": "cpa_alert",
        "rule_id": r.get("id"),
        "scope_type": scope_type,
        "scope_id": scope_id,
        "schedule": schedule,
        "result_type": result_type,
        "target_cpa": target,
        "mode": str(mode).lower(),
        "source": source_code,
        "spend": spend,
        "results": results,
        "cpa": cpa,
        "triggered": triggered,
        "top_ads_found": int(len(top_ads)),
    }

    _LOG.info(
        "caller=cpa_alert rule_id=%s scope_type=%s scope_id=%s schedule=%s result_type=%s target_cpa=%s mode=%s source=%s spend=%.2f results=%s cpa=%s triggered=%s top_ads_found=%s",
        str(r.get("id") or ""),
        scope_type,
        scope_id,
        schedule,
        result_type,
        f"{target:.2f}",
        str(mode).lower(),
        str(source_code),
        float(spend),
        int(results),
        f"{cpa:.4f}" if cpa is not None else "None",
        str(triggered),
        int(len(top_ads)),
    )

    if not triggered and not test:
        return None, meta

    header = f"🚨 CPA Alert — {str(r.get('name') or r.get('id') or '')}".strip()
    if test:
        header = "🧪 TEST\n" + header

    scope_label = _rule_scope_label(r)
    period_line = f"Период: {main_pd.label} ({main_pd.since}–{main_pd.until})"

    lines = [
        header,
        period_line,
        "",
        f"Scope: {scope_type} • {scope_label}",
        f"Spend: {spend:.2f} $" if spend > 0 else "Spend: —",
        f"Results: {int(results)} ({result_type})",
        (
            f"CPA / Target CPA: {(cpa or 0):.2f} $ / {target:.2f} $"
            if cpa is not None
            else f"CPA / Target CPA: — / {target:.2f} $"
        ),
        f"Статус триггера: {status}",
    ]

    def _cpa_str(v: Any) -> str:
        if v is None:
            return "—"
        try:
            vf = float(v)
        except Exception:
            return "—"
        return f"{vf:.2f}$" if vf > 0 else "—"

    if comp_block is not None:
        lines.extend(
            [
                "",
                "Сравнение периодов:",
                f"- MAIN spend={spend:.2f}$ results={int(results)} cpa={_cpa_str(cpa)}",
                f"- COMP spend={float(comp_block.get('spend') or 0):.2f}$ results={int(comp_block.get('results') or 0)} cpa={_cpa_str(comp_block.get('cpa'))}",
            ]
        )
        if ai_text:
            lines.extend(["", ai_text.strip()])

    if top_ads:
        lines.append("")
        lines.append("Виновники (TOP):")
        i = 1
        for a in top_ads:
            camp = str(a.get("campaign_name") or "")
            adset = str(a.get("adset_name") or "")
            adname = str(a.get("ad_name") or "")
            sp = float(a.get("spend") or 0.0)
            res = int(a.get("results") or 0)
            cpa_a = a.get("cpa")
            cpa_str = "—" if cpa_a is None else f"{float(cpa_a):.2f}$"
            url = str(a.get("url") or "")
            title = " / ".join([x for x in [camp, adset, adname] if x])
            if len(title) > 70:
                title = title[:67] + "…"
            lines.append(f"{i}. {title}")
            lines.append(f"   spend={sp:.2f}$ results={res} cpa={cpa_str}")
            if url:
                lines.append(f"   {url}")
            i += 1

    lines.extend(["", f"ℹ️ Источник данных: {source}"])

    return "\n".join(lines), meta


async def _send_admin_message(context: Any, text: str) -> None:
    try:
        await context.bot.send_message(chat_id=int(SUPERADMIN_USER_ID), text=str(text))
    except Exception:
        # fallback: nothing
        pass


async def run_cpa_alerts_for_mode(
    context: Any,
    *,
    mode: str,
    rule_id: str | None = None,
    test: bool = False,
) -> None:
    now = datetime.now(ALMATY_TZ)

    ensure_cpa_alerts_state_initialized()
    ensure_default_rules_from_legacy_accounts()

    st = load_cpa_alerts_state()
    if not bool(st.get("enabled") is True):
        _LOG.info("caller=cpa_alert mode=%s enabled=false", str(mode))
        return

    rules = list_rules(enabled_only=True)
    if rule_id:
        rr = get_rule(str(rule_id))
        rules = [rr] if rr else []

    for r in rules:
        if not isinstance(r, dict):
            continue
        msg, meta = await build_cpa_alert_message(rule=r, mode=mode, now=now, test=bool(test))
        if msg:
            await _send_admin_message(context, msg)
            await asyncio.sleep(0.3)

        # Update last_run_at for the rule (best-effort) only if message was sent.
        if msg and not test:
            try:
                rid = str((r or {}).get("id") or "").strip()
                if rid:
                    st2 = load_cpa_alerts_state()
                    targets = st2.get("targets") if isinstance(st2.get("targets"), list) else []
                    out_targets = []
                    for it in targets:
                        if not isinstance(it, dict):
                            continue
                        if str(it.get("id") or "").strip() == rid:
                            it2 = dict(it)
                            it2["last_run_at"] = now.isoformat()
                            out_targets.append(it2)
                        else:
                            out_targets.append(it)
                    st2["targets"] = out_targets
                    save_cpa_alerts_state(st2)
            except Exception:
                pass


async def cpa_alerts_hourly_job(context: Any) -> None:
    log = logging.getLogger(__name__)
    log.info("job_start name=cpa_alerts_hourly_job")
    await run_cpa_alerts_for_mode(context, mode="HOURLY")
    log.info("job_done name=cpa_alerts_hourly_job")


async def cpa_alerts_daily_job(context: Any) -> None:
    log = logging.getLogger(__name__)
    log.info("job_start name=cpa_alerts_daily_job")
    await run_cpa_alerts_for_mode(context, mode="DAILY")
    log.info("job_done name=cpa_alerts_daily_job")


async def cpa_alerts_3days_job(context: Any) -> None:
    log = logging.getLogger(__name__)
    log.info("job_start name=cpa_alerts_3days_job")

    # Only rules that haven't run in last 72h.
    now = datetime.now(ALMATY_TZ)
    rules = list_rules(enabled_only=True)
    for r in rules:
        if not isinstance(r, dict):
            continue
        if str(r.get("schedule") or "").upper().strip() != "DAYS_3":
            continue
        last = str(r.get("last_run_at") or "")
        ok = False
        if not last:
            ok = True
        else:
            try:
                dt = datetime.fromisoformat(last)
                if not dt.tzinfo:
                    dt = ALMATY_TZ.localize(dt)
                ok = (now - dt) >= timedelta(hours=72)
            except Exception:
                ok = True
        if not ok:
            continue
        rid = str(r.get("id") or "").strip()
        if rid:
            await run_cpa_alerts_for_mode(context, mode="DAYS_3", rule_id=rid, test=False)

    log.info("job_done name=cpa_alerts_3days_job")


async def cpa_alerts_weekly_job(context: Any) -> None:
    log = logging.getLogger(__name__)
    log.info("job_start name=cpa_alerts_weekly_job")
    now = datetime.now(ALMATY_TZ)
    if int(now.weekday()) != 0:
        log.info("caller=cpa_alert mode=weekly skip=not_monday")
        return
    await run_cpa_alerts_for_mode(context, mode="WEEKLY")
    log.info("job_done name=cpa_alerts_weekly_job")


def _job_next_run_str(job: Any) -> str:
    try:
        return str(job.next_t)
    except Exception:
        return str(job)


def schedule_cpa_alerts(app: Any) -> None:
    """Registers CPA alerts jobs (new system)."""
    log = logging.getLogger(__name__)

    ensure_cpa_alerts_state_initialized()
    ensure_default_rules_from_legacy_accounts()

    # Hourly: run every hour at :30, internal guard for active hours.
    try:
        now = datetime.now(ALMATY_TZ)
        first = now.replace(minute=30, second=0, microsecond=0)
        if first <= now:
            first = first + timedelta(hours=1)
    except Exception:
        first = timedelta(minutes=30)

    jh = app.job_queue.run_repeating(
        cpa_alerts_hourly_job,
        interval=timedelta(hours=1),
        first=first,
        name="cpa_alerts_hourly_job",
    )
    log.info("job_registered name=cpa_alerts_hourly_job next_run_at=%s", _job_next_run_str(jh))

    jd = app.job_queue.run_daily(
        cpa_alerts_daily_job,
        time=dt_time(hour=10, minute=45, tzinfo=ALMATY_TZ),
        name="cpa_alerts_daily_job",
    )
    log.info("job_registered name=cpa_alerts_daily_job next_run_at=%s", _job_next_run_str(jd))

    j3 = app.job_queue.run_daily(
        cpa_alerts_3days_job,
        time=dt_time(hour=10, minute=45, tzinfo=ALMATY_TZ),
        name="cpa_alerts_3days_job",
    )
    log.info("job_registered name=cpa_alerts_3days_job next_run_at=%s", _job_next_run_str(j3))

    jw = app.job_queue.run_daily(
        cpa_alerts_weekly_job,
        time=dt_time(hour=10, minute=45, tzinfo=ALMATY_TZ),
        name="cpa_alerts_weekly_job",
    )
    log.info("job_registered name=cpa_alerts_weekly_job next_run_at=%s", _job_next_run_str(jw))
