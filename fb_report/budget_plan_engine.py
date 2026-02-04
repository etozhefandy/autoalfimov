from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from facebook_business.adobjects.adset import AdSet

from fb_report.constants import ALMATY_TZ
from services.facebook_api import (
    allow_fb_api_calls,
    fetch_adsets,
    fetch_insights,
    fetch_insights_bulk,
    safe_api_call,
)


_LOG = logging.getLogger(__name__)


@dataclass
class PeriodInfo:
    since: str
    until: str
    days_left_including_today: int


def _today_local() -> date:
    return datetime.now(ALMATY_TZ).date()


def _iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def period_info(period_type: str, *, today: Optional[date] = None) -> PeriodInfo:
    t = (today or _today_local())
    pt = str(period_type or "").upper().strip()

    if pt == "DAY":
        return PeriodInfo(since=_iso(t), until=_iso(t), days_left_including_today=1)

    if pt == "WEEK":
        monday = t - timedelta(days=t.weekday())
        sunday = monday + timedelta(days=6)
        days_left = (sunday - t).days + 1
        if days_left < 1:
            days_left = 1
        return PeriodInfo(since=_iso(monday), until=_iso(t), days_left_including_today=days_left)

    first = t.replace(day=1)
    if first.month == 12:
        next_month = date(first.year + 1, 1, 1)
    else:
        next_month = date(first.year, first.month + 1, 1)
    last = next_month - timedelta(days=1)
    days_left = (last - t).days + 1
    if days_left < 1:
        days_left = 1
    return PeriodInfo(since=_iso(first), until=_iso(t), days_left_including_today=days_left)


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return 0.0


def _clamp(v: float, mn: Optional[float], mx: Optional[float]) -> float:
    out = float(v)
    if mn is not None and out < float(mn):
        out = float(mn)
    if mx is not None and out > float(mx):
        out = float(mx)
    return out


def _get_limits(plan: Dict[str, Any], adset_id: str) -> Tuple[bool, Optional[float], Optional[float]]:
    lock = (plan or {}).get("locked_adset_limits")
    if not isinstance(lock, dict):
        return False, None, None
    it = lock.get(str(adset_id))
    if not isinstance(it, dict):
        return False, None, None
    locked = bool(it.get("locked"))
    mn = it.get("min_usd_day")
    mx = it.get("max_usd_day")
    mn_f = None if mn is None else _to_float(mn)
    mx_f = None if mx is None else _to_float(mx)
    return locked, mn_f, mx_f


def _campaign_filter_params(campaign_ids: List[str]) -> Dict[str, Any]:
    return {
        "filtering": [
            {
                "field": "campaign.id",
                "operator": "IN",
                "value": [str(x) for x in (campaign_ids or []) if str(x).strip()],
            }
        ]
    }


def fetch_spend_usd(
    aid: str,
    *,
    since: str,
    until: str,
    scope_type: str,
    bundle_campaign_ids: Optional[List[str]] = None,
) -> float:
    period = {"since": str(since), "until": str(until)}
    st = str(scope_type or "ACCOUNT").upper().strip()

    if st == "BUNDLE":
        cids = [str(x) for x in (bundle_campaign_ids or []) if str(x).strip()]
        if not cids:
            return 0.0
        rows = fetch_insights_bulk(
            aid,
            period=period,
            level="campaign",
            fields=["spend", "campaign_id"],
            params_extra=_campaign_filter_params(cids),
        )
        total = 0.0
        for r in (rows or []):
            total += _to_float((r or {}).get("spend"))
        return float(total)

    ins = fetch_insights(aid, period)
    return _to_float((ins or {}).get("spend"))


def _eligible_adsets(plan: Dict[str, Any], *, force: bool = True) -> List[Dict[str, Any]]:
    aid = str((plan or {}).get("account_id") or "").strip()
    if not aid:
        return []

    scope_type = str((plan or {}).get("scope_type") or "ACCOUNT").upper().strip()
    bundle_campaign_ids = [str(x) for x in ((plan or {}).get("bundle_campaign_ids") or []) if str(x).strip()]

    excl_c = set([str(x) for x in ((plan or {}).get("excluded_campaign_ids") or []) if str(x).strip()])
    excl_a = set([str(x) for x in ((plan or {}).get("excluded_adset_ids") or []) if str(x).strip()])

    adsets = fetch_adsets(aid, force=bool(force))
    out: List[Dict[str, Any]] = []
    for a in (adsets or []):
        if not isinstance(a, dict):
            continue
        adset_id = str(a.get("id") or "").strip()
        if not adset_id:
            continue

        status = str(a.get("effective_status") or a.get("status") or "").upper().strip()
        if status != "ACTIVE":
            continue

        if adset_id in excl_a:
            continue

        campaign_id = str(a.get("campaign_id") or "").strip()
        if campaign_id and campaign_id in excl_c:
            continue

        if scope_type == "BUNDLE":
            if not campaign_id or campaign_id not in set(bundle_campaign_ids):
                continue

        daily = a.get("daily_budget")
        if daily is None:
            continue
        if _to_float(daily) <= 0:
            continue

        lifetime = a.get("lifetime_budget")
        if lifetime is not None and _to_float(lifetime) > 0:
            continue

        out.append(dict(a))

    return out


def _redistribute_with_limits(
    *,
    target_total: float,
    unlocked: List[Dict[str, Any]],
    locked_fixed: Dict[str, float],
    limits: Dict[str, Tuple[Optional[float], Optional[float]]],
) -> Tuple[Dict[str, float], List[str]]:
    warnings: List[str] = []

    fixed_sum = sum(float(x) for x in (locked_fixed or {}).values())
    remaining = float(target_total) - float(fixed_sum)
    if remaining < 0:
        remaining = 0.0

    weights: Dict[str, float] = {}
    sum_w = 0.0
    for a in (unlocked or []):
        aid = str(a.get("id") or "")
        cur = _to_float(a.get("daily_budget"))
        w = cur if cur > 0 else 0.0
        weights[aid] = w
        sum_w += w

    if not unlocked:
        return {}, warnings

    if sum_w <= 0:
        for a in unlocked:
            weights[str(a.get("id") or "")] = 1.0
        sum_w = float(len(unlocked))

    values: Dict[str, float] = {}
    for a in unlocked:
        adset_id = str(a.get("id") or "")
        base = remaining * (weights.get(adset_id, 0.0) / sum_w)
        mn, mx = limits.get(adset_id, (None, None))
        values[adset_id] = _clamp(base, mn, mx)

    def total_now() -> float:
        return fixed_sum + sum(values.values())

    delta = float(target_total) - total_now()

    for _ in range(12):
        if abs(delta) < 0.005:
            break

        pool: List[str] = []
        pool_w = 0.0
        for a in unlocked:
            adset_id = str(a.get("id") or "")
            v = float(values.get(adset_id, 0.0))
            mn, mx = limits.get(adset_id, (None, None))
            if delta > 0:
                if mx is None or v < float(mx) - 1e-9:
                    pool.append(adset_id)
                    pool_w += max(0.0, weights.get(adset_id, 0.0))
            else:
                if mn is None or v > float(mn) + 1e-9:
                    pool.append(adset_id)
                    pool_w += max(0.0, weights.get(adset_id, 0.0))

        if not pool:
            warnings.append("Не удалось распределить остаток из-за ограничений min/max")
            break

        if pool_w <= 0:
            pool_w = float(len(pool))
            for adset_id in pool:
                weights[adset_id] = 1.0

        new_delta = delta
        for adset_id in pool:
            w = weights.get(adset_id, 0.0)
            if w <= 0:
                w = 1.0
            share = (w / pool_w)
            step = delta * share

            old_v = float(values.get(adset_id, 0.0))
            mn, mx = limits.get(adset_id, (None, None))
            cand = old_v + step
            capped = _clamp(cand, mn, mx)
            values[adset_id] = capped
            new_delta -= (capped - old_v)

        if abs(new_delta - delta) < 0.0001:
            warnings.append("Остаток не распределён: ограничения слишком жёсткие")
            break

        delta = new_delta

    return values, warnings


def build_budget_plan_preview(plan: Dict[str, Any], *, force: bool = True) -> Dict[str, Any]:
    p = dict(plan or {})
    aid = str(p.get("account_id") or "").strip()
    if not aid:
        return {"ok": False, "error": "missing_account_id"}

    if not p.get("is_enabled", True):
        return {"ok": False, "error": "plan_disabled"}

    try:
        total_budget = float(p.get("budget_total_usd"))
    except Exception:
        return {"ok": False, "error": "invalid_budget_total"}

    if total_budget < 0:
        total_budget = 0.0

    pt = str(p.get("period_type") or "MONTH").upper().strip()
    pi = period_info(pt)

    spend = fetch_spend_usd(
        aid,
        since=pi.since,
        until=pi.until,
        scope_type=str(p.get("scope_type") or "ACCOUNT"),
        bundle_campaign_ids=p.get("bundle_campaign_ids") or [],
    )

    remaining = max(0.0, float(total_budget) - float(spend))
    target_per_day = float(total_budget) if pt == "DAY" else (remaining / max(1, int(pi.days_left_including_today)))

    eligible = _eligible_adsets(p, force=bool(force))

    locked_fixed: Dict[str, float] = {}
    unlocked: List[Dict[str, Any]] = []
    limits: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

    for a in eligible:
        adset_id = str(a.get("id") or "")
        locked, mn, mx = _get_limits(p, adset_id)
        limits[adset_id] = (mn, mx)

        cur = _to_float(a.get("daily_budget"))
        if locked:
            if mn is None and mx is None:
                locked_fixed[adset_id] = cur
            else:
                locked_fixed[adset_id] = _clamp(cur, mn, mx)
        else:
            unlocked.append(a)

    new_unlocked, warnings = _redistribute_with_limits(
        target_total=float(target_per_day),
        unlocked=unlocked,
        locked_fixed=locked_fixed,
        limits=limits,
    )

    changes: List[Dict[str, Any]] = []
    total_new = 0.0
    for a in eligible:
        adset_id = str(a.get("id") or "")
        name = str(a.get("name") or "")
        campaign_id = str(a.get("campaign_id") or "")
        status = str(a.get("effective_status") or a.get("status") or "")

        old_usd = _to_float(a.get("daily_budget"))
        locked, mn, mx = _get_limits(p, adset_id)

        if locked:
            new_usd = float(locked_fixed.get(adset_id, old_usd))
        else:
            new_usd = float(new_unlocked.get(adset_id, old_usd))

        old_cents = int(round(old_usd * 100.0))
        new_cents = int(round(new_usd * 100.0))

        old_usd_r = old_cents / 100.0
        new_usd_r = new_cents / 100.0

        total_new += new_usd_r

        changes.append(
            {
                "adset_id": adset_id,
                "name": name,
                "campaign_id": campaign_id,
                "status": status,
                "old_usd": old_usd_r,
                "new_usd": new_usd_r,
                "delta_usd": new_usd_r - old_usd_r,
                "old_cents": old_cents,
                "new_cents": new_cents,
                "locked": bool(locked),
                "min_usd_day": mn,
                "max_usd_day": mx,
            }
        )

    try:
        changes.sort(key=lambda x: abs(float(x.get("delta_usd") or 0.0)), reverse=True)
    except Exception:
        pass

    preview = {
        "ok": True,
        "plan_id": str(p.get("plan_id") or ""),
        "account_id": aid,
        "scope_type": str(p.get("scope_type") or "ACCOUNT"),
        "period_type": pt,
        "budget_total_usd": float(total_budget),
        "period_since": pi.since,
        "period_until": pi.until,
        "spend_usd": float(spend),
        "remaining_usd": float(remaining),
        "days_left_including_today": int(pi.days_left_including_today),
        "target_per_day_usd": float(target_per_day),
        "target_total_new_usd": float(total_new),
        "warnings": warnings,
        "changes": changes,
    }

    _LOG.info(
        "caller=budget_plan action=preview aid=%s plan_id=%s scope=%s period=%s budget_total=%.2f spend=%.2f remaining=%.2f target_day=%.2f eligible=%s warnings=%s",
        str(aid),
        str(p.get("plan_id") or ""),
        str(p.get("scope_type") or "ACCOUNT"),
        str(pt),
        float(total_budget),
        float(spend),
        float(remaining),
        float(target_per_day),
        str(len(eligible)),
        str(len(warnings)),
    )

    return preview


def apply_budget_plan_preview(preview: Dict[str, Any]) -> Dict[str, Any]:
    pv = dict(preview or {})
    if not pv.get("ok"):
        return {"ok": False, "error": "invalid_preview"}

    aid = str(pv.get("account_id") or "").strip()
    if not aid:
        return {"ok": False, "error": "missing_account_id"}

    changes = pv.get("changes")
    if not isinstance(changes, list) or not changes:
        return {"ok": False, "error": "no_changes"}

    results: List[Dict[str, Any]] = []
    updated = 0
    skipped = 0
    failed = 0

    with allow_fb_api_calls(reason="budget_plan:apply"):
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            adset_id = str(ch.get("adset_id") or "").strip()
            if not adset_id:
                continue

            old_cents = int(ch.get("old_cents") or 0)
            new_cents = int(ch.get("new_cents") or 0)
            if old_cents == new_cents:
                skipped += 1
                results.append({"adset_id": adset_id, "status": "skipped", "old_cents": old_cents, "new_cents": new_cents})
                continue

            obj = AdSet(adset_id)
            res, info = safe_api_call(
                obj.api_update,
                params={"daily_budget": int(new_cents)},
                _caller="budget_plan",
                _aid=aid,
                _return_error_info=True,
            )

            ok = res is not None
            if ok:
                updated += 1
                results.append({"adset_id": adset_id, "status": "ok", "old_cents": old_cents, "new_cents": new_cents})
            else:
                failed += 1
                results.append(
                    {
                        "adset_id": adset_id,
                        "status": "error",
                        "old_cents": old_cents,
                        "new_cents": new_cents,
                        "error": info,
                    }
                )

    _LOG.info(
        "caller=budget_plan action=apply aid=%s plan_id=%s updated=%s skipped=%s failed=%s",
        str(aid),
        str(pv.get("plan_id") or ""),
        str(updated),
        str(skipped),
        str(failed),
    )

    return {
        "ok": failed == 0,
        "account_id": aid,
        "plan_id": str(pv.get("plan_id") or ""),
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }
