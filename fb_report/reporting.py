# fb_report/reporting.py

import json
import os
from datetime import datetime, timedelta
import re
import time
import hashlib
import logging
from typing import Any

from telegram.ext import ContextTypes

from .constants import (
    ALMATY_TZ,
    REPORT_CACHE_FILE,
    REPORT_CACHE_TTL,
    DEFAULT_REPORT_CHAT,
    MORNING_REPORT_CACHE_FILE,
    MORNING_REPORT_CACHE_TTL,
    DAILY_REPORT_CACHE_FILE,
)
from .storage import (
    get_account_name,
    metrics_flags,
    is_active,
    load_accounts,
)
from services.storage import period_key
from .insights import (
    load_local_insights,
    save_local_insights,
    extract_actions,
    extract_costs,
    _blend_totals,
)

from services.analytics import count_leads_from_actions, count_started_conversations_from_actions

from services.facebook_api import allow_fb_api_calls, fetch_insights_bulk, safe_api_call
from services.facebook_api import deny_fb_api_calls
from services.heatmap_store import load_snapshot, list_snapshot_hours


REPORT_TEXT_CACHE_VERSION = 3


def _load_daily_report_cache() -> dict:
    try:
        with open(DAILY_REPORT_CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_daily_report_cache(d: dict) -> None:
    try:
        os.makedirs(os.path.dirname(DAILY_REPORT_CACHE_FILE), exist_ok=True)
    except Exception:
        pass
    tmp = str(DAILY_REPORT_CACHE_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, DAILY_REPORT_CACHE_FILE)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _daily_cache_key(*, scope: str, scope_id: str, date_str: str, level: str, metrics_hash: str) -> str:
    return f"daily:{str(scope)}:{str(scope_id)}:{str(date_str)}:{str(level)}:{str(metrics_hash)}"


def _daily_ttl_seconds(*, date_str: str) -> int:
    try:
        today = datetime.now(ALMATY_TZ).date().strftime("%Y-%m-%d")
    except Exception:
        today = ""
    if str(date_str) == str(today):
        return int(60 * 60 * 2)
    return int(60 * 60 * 48)


def _daily_cache_get(key: str, *, ttl_seconds: int) -> tuple[Any | None, bool]:
    store = _load_daily_report_cache() or {}
    item = store.get(str(key))
    now_ts = time.time()
    if isinstance(item, dict):
        try:
            ts = float(item.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        if ts and (now_ts - ts) <= float(ttl_seconds):
            return item.get("value"), True
    return None, False


def _daily_cache_set(key: str, value: Any) -> None:
    store = _load_daily_report_cache() or {}
    store[str(key)] = {"ts": time.time(), "value": value}
    _save_daily_report_cache(store)


def _report_source_footer_lines(*, mode: str, cache_state: str) -> list[str]:
    lines: list[str] = []
    if str(mode) != "hourly_cache":
        lines.append("ℹ️ почасовых данных нет — показан дневной отчёт (кэширован)")
    if str(mode) == "hourly_cache":
        lines.append("ℹ️ Источник данных: почасовой кэш")
    else:
        if str(cache_state) == "hit":
            lines.append("ℹ️ Источник данных: дневной кэш")
        else:
            lines.append("ℹ️ Источник данных: прямой запрос Facebook API")
    return lines


def _strip_source_footer(text: str) -> tuple[str, list[str]]:
    if not text:
        return text, []
    lines = [ln for ln in str(text).split("\n")]
    tail: list[str] = []
    while lines and (
        str(lines[-1]).startswith("ℹ️ Источник данных:")
        or str(lines[-1]).startswith("ℹ️ почасовых данных нет")
        or str(lines[-1]).strip() == ""
    ):
        ln = lines.pop()
        if str(ln).strip():
            tail.append(str(ln))
    tail.reverse()
    return "\n".join(lines).rstrip(), tail


def _actions_list_from_map(actions_map: dict[str, float]) -> list[dict]:
    out: list[dict] = []
    for k, v in (actions_map or {}).items():
        try:
            out.append({"action_type": str(k), "value": float(v or 0.0)})
        except Exception:
            continue
    return out


def _aggregate_account_day_from_hourly_snapshots(aid: str, *, date_str: str) -> dict | None:
    with deny_fb_api_calls(reason="reporting_hourly_account"):
        hours = list_snapshot_hours(str(aid), date_str=str(date_str))
        if not hours:
            return None

        spend = 0.0
        impressions = 0
        clicks = 0
        actions_map: dict[str, float] = {}
        any_ready = False

        for h in hours:
            snap = load_snapshot(str(aid), date_str=str(date_str), hour=int(h)) or {}
            if str(snap.get("status") or "") not in {"ready", "ready_low_confidence"}:
                continue
            rows = snap.get("rows") or []
            if not isinstance(rows, list) or not rows:
                continue
            any_ready = True
            for r in rows:
                if not isinstance(r, dict):
                    continue
                try:
                    spend += float(r.get("spend") or 0.0)
                except Exception:
                    pass
                try:
                    impressions += int((r or {}).get("impressions") or 0)
                except Exception:
                    pass
                try:
                    clicks += int((r or {}).get("clicks") or 0)
                except Exception:
                    pass
                acts = (r or {}).get("actions")
                if isinstance(acts, dict) and acts:
                    for k, v in acts.items():
                        try:
                            actions_map[str(k)] = float(actions_map.get(str(k), 0.0) or 0.0) + float(v or 0.0)
                        except Exception:
                            continue

        if not any_ready:
            return None

        cpm = (float(spend) / float(impressions) * 1000.0) if int(impressions) > 0 else 0.0
        cpc = (float(spend) / float(clicks)) if int(clicks) > 0 else 0.0
        return {
            "impressions": int(impressions),
            "cpm": float(cpm),
            "clicks": int(clicks),
            "cpc": float(cpc),
            "spend": float(spend),
            "actions": _actions_list_from_map(actions_map),
            "cost_per_action_type": [],
            "_source": "hourly_cache",
            "_meta": {"date": str(date_str), "level": "account"},
        }


def _fetch_account_day_insight(
    *,
    aid: str,
    kind: str,
    caller: str,
) -> tuple[dict | None, str, str, str]:
    log = logging.getLogger(__name__)
    now = datetime.now(ALMATY_TZ)
    date_str = now.strftime("%Y-%m-%d") if str(kind) == "today" else (now - timedelta(days=1)).strftime("%Y-%m-%d")
    mh = _metrics_hash("account", None)
    key = _daily_cache_key(scope="account", scope_id=str(aid), date_str=str(date_str), level="ACCOUNT", metrics_hash=mh)

    hourly = _aggregate_account_day_from_hourly_snapshots(str(aid), date_str=str(date_str))
    if isinstance(hourly, dict) and hourly:
        try:
            log.info(
                "caller=%s mode=hourly_cache cache=hit scope=account scope_id=%s date=%s level=ACCOUNT",
                str(caller),
                str(aid),
                str(date_str),
            )
        except Exception:
            pass
        return hourly, "hourly_cache", "hit", str(date_str)

    ttl = _daily_ttl_seconds(date_str=str(date_str))
    cached, hit = _daily_cache_get(key, ttl_seconds=int(ttl))
    if hit and isinstance(cached, dict):
        try:
            log.info(
                "caller=%s mode=daily_fallback cache=hit scope=account scope_id=%s date=%s level=ACCOUNT",
                str(caller),
                str(aid),
                str(date_str),
            )
        except Exception:
            pass
        cached["_source"] = "daily_cache"
        return cached, "daily_fallback", "hit", str(date_str)

    try:
        log.info(
            "caller=%s mode=daily_fallback cache=miss scope=account scope_id=%s date=%s level=ACCOUNT",
            str(caller),
            str(aid),
            str(date_str),
        )
    except Exception:
        pass

    fields = [
        "impressions",
        "cpm",
        "clicks",
        "cpc",
        "spend",
        "actions",
        "cost_per_action_type",
    ]
    params_extra = {
        "action_report_time": "conversion",
        "use_unified_attribution_setting": True,
    }
    rows: list[dict] = []
    with allow_fb_api_calls(reason="reporting_daily_fallback"):
        rows = fetch_insights_bulk(
            str(aid),
            period=str(kind),
            level="account",
            fields=list(fields),
            params_extra=dict(params_extra),
        )

    ins_dict = (rows[0] if rows else None) or {}
    if not isinstance(ins_dict, dict):
        ins_dict = {}
    try:
        ins_dict["_source"] = "fb_api"
        ins_dict["_meta"] = {
            "period_input": str(kind),
            "period_effective": str(kind),
            "level": "account",
            "fields": list(fields),
            "params_extra": dict(params_extra),
            "date": str(date_str),
        }
    except Exception:
        pass

    _daily_cache_set(key, dict(ins_dict))
    try:
        log.info(
            "caller=%s mode=daily_fallback cache=write scope=account scope_id=%s date=%s level=ACCOUNT",
            str(caller),
            str(aid),
            str(date_str),
        )
    except Exception:
        pass
    return ins_dict, "daily_fallback", "write", str(date_str)


def _load_morning_report_cache() -> dict:
    try:
        with open(MORNING_REPORT_CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_morning_report_cache(d: dict) -> None:
    try:
        os.makedirs(os.path.dirname(MORNING_REPORT_CACHE_FILE), exist_ok=True)
    except Exception:
        pass
    tmp = str(MORNING_REPORT_CACHE_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, MORNING_REPORT_CACHE_FILE)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _metrics_hash(metrics_set: str, lead_action_type: str | None) -> str:
    s = str(metrics_set or "") + "|" + (str(lead_action_type or "").strip() or "")
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:10]


def _cache_key(*, scope: str, scope_id: str, since: str, until: str, mode: str, metrics_hash: str) -> str:
    return f"{str(scope)}:{str(scope_id)}:{str(since)}:{str(until)}:{str(mode)}:{str(metrics_hash)}"


def _cache_get_morning(key: str) -> tuple[dict | None, bool]:
    store = _load_morning_report_cache() or {}
    item = store.get(str(key))
    now_ts = time.time()
    hit = False
    if isinstance(item, dict):
        try:
            ts = float(item.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        if ts and (now_ts - ts) <= float(MORNING_REPORT_CACHE_TTL):
            val = item.get("value")
            if isinstance(val, dict):
                hit = True
                logging.getLogger(__name__).info("cache_read key=%s hit=true", str(key))
                return val, True
    logging.getLogger(__name__).info("cache_read key=%s hit=false", str(key))
    return None, False


def _cache_set_morning(key: str, value: dict) -> None:
    store = _load_morning_report_cache() or {}
    store[str(key)] = {"ts": time.time(), "value": value}
    try:
        size_bytes = len(json.dumps(store[str(key)], ensure_ascii=False).encode("utf-8"))
    except Exception:
        size_bytes = 0
    _save_morning_report_cache(store)
    logging.getLogger(__name__).info(
        "cache_write key=%s size_bytes=%s",
        str(key),
        str(int(size_bytes)) if size_bytes else "",
    )


def _yesterday_range_almaty() -> tuple[str, str]:
    now = datetime.now(ALMATY_TZ)
    ds = (now.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    return ds, ds


def _autopilot_tracked_group_ids_from_row(row: dict) -> list[str]:
    ap = (row or {}).get("autopilot") or {}
    if not isinstance(ap, dict):
        ap = {}
    ids = ap.get("active_group_ids")
    if not isinstance(ids, list):
        ids = []
    out = []
    seen = set()
    for x in ids:
        s = str(x).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    gid = str(ap.get("active_group_id") or "").strip()
    if gid and gid not in seen:
        out.append(gid)
    return out


def _autopilot_group_from_row(row: dict, gid: str) -> dict:
    ap = (row or {}).get("autopilot") or {}
    if not isinstance(ap, dict):
        ap = {}
    groups = ap.get("campaign_groups") or {}
    if not isinstance(groups, dict):
        groups = {}
    grp = groups.get(str(gid))
    return grp if isinstance(grp, dict) else {}


def _parse_group_lead_action_type(grp: dict) -> str | None:
    lm = (grp or {}).get("lead_metric")
    if isinstance(lm, dict):
        at = lm.get("action_type")
    else:
        at = lm
    at = str(at or "").strip()
    return at or None


def _sum_metrics_from_insight_rows(rows: list[dict], *, aid: str, lead_action_type: str | None) -> dict:
    spend = 0.0
    msgs = 0
    leads = 0
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        try:
            spend += float(r.get("spend", 0) or 0)
        except Exception:
            pass
        try:
            actions_map = extract_actions(r)
        except Exception:
            actions_map = {}
        try:
            msgs += int(count_started_conversations_from_actions(actions_map) or 0)
        except Exception:
            pass
        try:
            leads += int(count_leads_from_actions(actions_map, aid=str(aid), lead_action_type=lead_action_type) or 0)
        except Exception:
            pass

    blended = int(msgs) + int(leads)
    cpa = (float(spend) / float(blended)) if blended > 0 else None
    return {
        "spend": float(spend),
        "msgs": int(msgs),
        "leads": int(leads),
        "blended": int(blended),
        "blended_cpa": float(cpa) if cpa is not None else None,
    }


def build_morning_report_text(*, period: str = "yesterday") -> tuple[str, dict]:
    t0 = time.time()
    if str(period or "") != "yesterday":
        period = "yesterday"

    since, until = _yesterday_range_almaty()
    store = load_accounts() or {}
    enabled_ids = [aid for aid, row in (store or {}).items() if (row or {}).get("enabled", True)]

    cache_hit = 0
    cache_miss = 0
    any_fb_calls = False

    entities_out: list[dict] = []
    accounts_out: list[dict] = []

    for aid in enabled_ids:
        row = (store or {}).get(str(aid)) or {}
        mr = (row or {}).get("morning_report") or {}
        if not isinstance(mr, dict):
            mr = {}
        level = str(mr.get("level", "ACCOUNT") or "ACCOUNT").upper()
        if level == "OFF":
            continue

        tracked_gids = _autopilot_tracked_group_ids_from_row(row)
        for gid in tracked_gids:
            grp = _autopilot_group_from_row(row, gid)
            name = str((grp or {}).get("name") or gid)
            cids = (grp or {}).get("campaign_ids") or []
            if not isinstance(cids, list):
                cids = []
            cset = {str(x) for x in cids if str(x).strip()}
            if not cset:
                continue

            lat = _parse_group_lead_action_type(grp)
            mh = _metrics_hash("blended", lat)
            key = _cache_key(
                scope="entity",
                scope_id=f"{str(aid)}:{str(gid)}",
                since=since,
                until=until,
                mode="ENTITY",
                metrics_hash=mh,
            )
            cached, hit = _cache_get_morning(key)
            if hit and cached is not None:
                cache_hit += 1
                metrics = cached
                try:
                    logging.getLogger(__name__).info(
                        "caller=morning_report mode=daily_fallback cache=hit scope=entity_group scope_id=%s date=%s level=ENTITY",
                        str(f"{aid}:{gid}"),
                        str(since),
                    )
                except Exception:
                    pass
            else:
                cache_miss += 1
                any_fb_calls = True
                try:
                    logging.getLogger(__name__).info(
                        "caller=morning_report mode=daily_fallback cache=miss scope=entity_group scope_id=%s date=%s level=ENTITY",
                        str(f"{aid}:{gid}"),
                        str(since),
                    )
                except Exception:
                    pass
                period_for_api: Any = {"since": since, "until": until}
                params_extra = {"action_report_time": "conversion", "use_unified_attribution_setting": True}
                with allow_fb_api_calls(reason="morning_report_entity"):
                    rows = fetch_insights_bulk(
                        str(aid),
                        period=period_for_api,
                        level="campaign",
                        fields=["campaign_id", "campaign_name", "spend", "actions"],
                        params_extra=dict(params_extra),
                    )
                rows_f = [r for r in (rows or []) if str((r or {}).get("campaign_id") or "") in cset]
                metrics = _sum_metrics_from_insight_rows(rows_f, aid=str(aid), lead_action_type=lat)
                _cache_set_morning(key, dict(metrics))
                try:
                    logging.getLogger(__name__).info(
                        "caller=morning_report mode=daily_fallback cache=write scope=entity_group scope_id=%s date=%s level=ENTITY",
                        str(f"{aid}:{gid}"),
                        str(since),
                    )
                except Exception:
                    pass

            entities_out.append(
                {
                    "aid": str(aid),
                    "gid": str(gid),
                    "name": name,
                    **{k: metrics.get(k) for k in ["spend", "msgs", "leads", "blended", "blended_cpa"]},
                }
            )

        if level == "ACCOUNT":
            mh = _metrics_hash("blended", None)
            key = _cache_key(
                scope="account",
                scope_id=str(aid),
                since=since,
                until=until,
                mode="ACCOUNT",
                metrics_hash=mh,
            )
            cached, hit = _cache_get_morning(key)
            if hit and cached is not None:
                cache_hit += 1
                metrics = cached
                try:
                    logging.getLogger(__name__).info(
                        "caller=morning_report mode=daily_fallback cache=hit scope=account scope_id=%s date=%s level=ACCOUNT",
                        str(aid),
                        str(since),
                    )
                except Exception:
                    pass
            else:
                cache_miss += 1
                any_fb_calls = True
                try:
                    logging.getLogger(__name__).info(
                        "caller=morning_report mode=daily_fallback cache=miss scope=account scope_id=%s date=%s level=ACCOUNT",
                        str(aid),
                        str(since),
                    )
                except Exception:
                    pass
                period_for_api = {"since": since, "until": until}
                params_extra = {"action_report_time": "conversion", "use_unified_attribution_setting": True}
                with allow_fb_api_calls(reason="morning_report_account"):
                    rows = fetch_insights_bulk(
                        str(aid),
                        period=period_for_api,
                        level="account",
                        fields=["spend", "actions"],
                        params_extra=dict(params_extra),
                    )
                metrics = _sum_metrics_from_insight_rows(list(rows or []), aid=str(aid), lead_action_type=None)
                _cache_set_morning(key, dict(metrics))
                try:
                    logging.getLogger(__name__).info(
                        "caller=morning_report mode=daily_fallback cache=write scope=account scope_id=%s date=%s level=ACCOUNT",
                        str(aid),
                        str(since),
                    )
                except Exception:
                    pass

            accounts_out.append(
                {
                    "aid": str(aid),
                    "name": get_account_name(str(aid)),
                    **{k: metrics.get(k) for k in ["spend", "msgs", "leads", "blended", "blended_cpa"]},
                }
            )

    def _fmt_money(v: float | None) -> str:
        try:
            return f"{float(v or 0):.2f} $"
        except Exception:
            return "0.00 $"

    def _fmt_int(v: int | None) -> str:
        try:
            return str(int(v or 0))
        except Exception:
            return "0"

    def _fmt_cpa(v: float | None) -> str:
        if v is None:
            return "—"
        try:
            return f"{float(v):.2f} $"
        except Exception:
            return "—"

    lines: list[str] = []
    lines.append(f"🌅 Утренний отчёт — {since}")
    lines.append(f"Период: вчера ({since}–{until})")
    lines.append("")

    if entities_out:
        lines.append("🏙 Группы/Сущности")
        for e in entities_out:
            lines.append(f"\n🏙 {str(e.get('name') or '')}")
            lines.append(f"💵 Spend: {_fmt_money(e.get('spend'))}")
            lines.append(f"💬 Переписки: {_fmt_int(e.get('msgs'))}")
            lines.append(f"📩 Заявки с сайта: {_fmt_int(e.get('leads'))}")
            lines.append(f"🧮 Blended: {_fmt_int(e.get('blended'))}")
            lines.append(f"🎯 Blended CPA: {_fmt_cpa(e.get('blended_cpa'))}")
        lines.append("")

    if accounts_out:
        lines.append("🏷 Остальные аккаунты")
        for a in accounts_out:
            lines.append(f"\n🏷 {str(a.get('name') or '')}")
            lines.append(f"💵 Spend: {_fmt_money(a.get('spend'))}")
            lines.append(f"💬 Переписки: {_fmt_int(a.get('msgs'))}")
            lines.append(f"📩 Заявки с сайта: {_fmt_int(a.get('leads'))}")
            lines.append(f"🧮 Blended: {_fmt_int(a.get('blended'))}")
            lines.append(f"🎯 Blended CPA: {_fmt_cpa(a.get('blended_cpa'))}")
        lines.append("")

    dur_ms = int((time.time() - t0) * 1000.0)
    lines.append(
        "debug: mode=ACCOUNT entities={e} accounts={a} cache_hit={h} cache_miss={m} duration_ms={d}".format(
            e=int(len(entities_out)),
            a=int(len(accounts_out)),
            h=int(cache_hit),
            m=int(cache_miss),
            d=int(dur_ms),
        )
    )

    mr_cache_state = "write" if bool(any_fb_calls) else "hit"
    foot = _report_source_footer_lines(mode="daily_fallback", cache_state=str(mr_cache_state))
    lines.extend([str(x) for x in foot if str(x).strip()])

    debug = {
        "since": since,
        "until": until,
        "entities": int(len(entities_out)),
        "accounts": int(len(accounts_out)),
        "cache_hit": int(cache_hit),
        "cache_miss": int(cache_miss),
        "duration_ms": int(dur_ms),
    }
    return "\n".join(lines), debug


def _strip_fb_technical_lines(text: str) -> str:
    if not text:
        return text
    out: list[str] = []
    for line in str(text).split("\n"):
        s = str(line)
        s_strip = s.strip()
        if s_strip.startswith("⚙"):
            continue
        low = s_strip.lower()
        if "action_report_time" in low:
            continue
        if "attribution" in low:
            continue
        if "timezone" in low or " tz=" in low:
            continue
        out.append(s)
    return "\n".join(out).strip()


def build_morning_account_message(*, aid: str, date_str: str, period: dict) -> str:
    try:
        name, ins = fetch_insight(str(aid), period)
    except Exception as e:
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return ""

    if not isinstance(ins, dict) or not ins:
        return ""

    impressions = 0
    clicks_all = 0
    link_clicks = 0
    spend = 0.0
    cpm = 0.0
    cpc = 0.0
    try:
        impressions = int(ins.get("impressions") or 0)
    except Exception:
        impressions = 0
    try:
        clicks_all = int(ins.get("clicks") or 0)
    except Exception:
        clicks_all = 0
    try:
        spend = float(ins.get("spend") or 0.0)
    except Exception:
        spend = 0.0
    try:
        cpm = float(ins.get("cpm") or 0.0)
    except Exception:
        cpm = 0.0
    try:
        cpc = float(ins.get("cpc") or 0.0)
    except Exception:
        cpc = 0.0

    acts = extract_actions(ins)
    try:
        link_clicks = int((acts or {}).get("link_click", 0) or 0)
    except Exception:
        link_clicks = 0

    ctr_all = (float(clicks_all) / float(impressions) * 100.0) if impressions > 0 else 0.0
    ctr_link = (float(link_clicks) / float(impressions) * 100.0) if impressions > 0 else 0.0

    msgs = 0
    try:
        msgs = int(count_started_conversations_from_actions(acts) or 0)
    except Exception:
        msgs = 0

    msg_cpa = None
    try:
        _msgs_cnt, _spend_msgs_only, cps = _msg_cps_account_level(str(aid), period)
        msg_cpa = cps
    except Exception:
        msg_cpa = None

    all_zero = (
        int(impressions) <= 0
        and int(clicks_all) <= 0
        and int(link_clicks) <= 0
        and float(spend) <= 0.0
        and int(msgs) <= 0
    )
    if all_zero:
        return ""

    lines: list[str] = []
    lines.append(f"🟢 {str(name or get_account_name(str(aid)))} ({str(date_str)})")
    lines.append("")
    lines.append(f"👁 Показы: {fmt_int(impressions)}")
    lines.append(f"🎯 CPM: {float(cpm):.2f} $")
    lines.append(f"🖱 Клики (все): {fmt_int(clicks_all)}")
    lines.append(f"📈 CTR (все клики): {float(ctr_all):.2f} %")
    lines.append(f"🔗 Клики (по ссылке): {fmt_int(link_clicks)}")
    lines.append(f"📈 CTR (по ссылке): {float(ctr_link):.2f} %")
    lines.append(f"💸 CPC: {float(cpc):.2f} $" if float(cpc) > 0 else "💸 CPC: —")
    lines.append(f"💵 Затраты: {float(spend):.2f} $" if float(spend) > 0 else "💵 Затраты: —")
    lines.append("")
    lines.append(f"✉️ Переписки: {fmt_int(int(msgs))}")
    if msg_cpa is not None and float(msg_cpa) > 0:
        lines.append(f"💬💲 Цена переписки: {float(msg_cpa):.2f} $")
    else:
        lines.append("💬💲 Цена переписки: —")

    return _strip_fb_technical_lines("\n".join(lines))


def build_morning_blended_message(*, aid: str, name: str, period: dict) -> str:
    flags = metrics_flags(str(aid))
    if not (bool(flags.get("messaging")) and bool(flags.get("leads"))):
        return ""

    try:
        _, ins = fetch_insight(str(aid), period)
    except Exception:
        return ""
    if not isinstance(ins, dict) or not ins:
        return ""

    spend = 0.0
    msgs = 0
    leads = 0
    total = 0
    cpa = None
    try:
        spend, msgs, leads, total, cpa = _blend_totals(ins, aid=str(aid))
    except Exception:
        spend, msgs, leads, total, cpa = (0.0, 0, 0, 0, None)

    if int(msgs) <= 0 and int(leads) <= 0 and float(spend) <= 0.0:
        return ""

    lines: list[str] = []
    lines.append(f"🧮 Blended отчёт — {str(name or get_account_name(str(aid)))}")
    lines.append("")
    lines.append(f"💬 Переписки: {fmt_int(int(msgs))}")
    lines.append(f"📩 Заявки с сайта: {fmt_int(int(leads))}")
    lines.append(f"👥 Всего заявок: {fmt_int(int(total))}")
    lines.append("")
    lines.append(f"💵 Затраты: {float(spend):.2f} $" if float(spend) > 0 else "💵 Затраты: —")
    if cpa is not None and float(cpa) > 0:
        lines.append(f"🎯 Blended CPA: {float(cpa):.2f} $")
    else:
        lines.append("🎯 Blended CPA: —")
    return _strip_fb_technical_lines("\n".join(lines))


def build_morning_group_message(*, aid: str, gid: str, grp: dict, since: str, until: str) -> str:
    cids = (grp or {}).get("campaign_ids") or []
    if not isinstance(cids, list):
        cids = []
    cset = {str(x) for x in cids if str(x).strip()}
    if not cset:
        return ""

    name = str((grp or {}).get("name") or gid)
    lat = _parse_group_lead_action_type(grp)
    mh = _metrics_hash("blended", lat)
    key = _cache_key(
        scope="entity",
        scope_id=f"{str(aid)}:{str(gid)}",
        since=str(since),
        until=str(until),
        mode="ENTITY",
        metrics_hash=mh,
    )
    cached, hit = _cache_get_morning(key)
    if hit and cached is not None:
        metrics = cached
    else:
        period_for_api: Any = {"since": str(since), "until": str(until)}
        params_extra = {"action_report_time": "conversion", "use_unified_attribution_setting": True}
        with allow_fb_api_calls(reason="morning_report_entity"):
            rows = fetch_insights_bulk(
                str(aid),
                period=period_for_api,
                level="campaign",
                fields=["campaign_id", "campaign_name", "spend", "actions"],
                params_extra=dict(params_extra),
            )
        rows_f = [r for r in (rows or []) if str((r or {}).get("campaign_id") or "") in cset]
        metrics = _sum_metrics_from_insight_rows(rows_f, aid=str(aid), lead_action_type=lat)
        _cache_set_morning(key, dict(metrics))

    spend = float((metrics or {}).get("spend") or 0.0)
    msgs = int((metrics or {}).get("msgs") or 0)
    leads = int((metrics or {}).get("leads") or 0)
    total = int((metrics or {}).get("blended") or (msgs + leads))
    cpa = (float(spend) / float(total)) if total > 0 else None

    if int(msgs) <= 0 and int(leads) <= 0 and float(spend) <= 0.0:
        return ""

    lines: list[str] = []
    lines.append(f"🧮 Blended отчёт — {str(name)}")
    lines.append("")
    lines.append(f"💬 Переписки: {fmt_int(int(msgs))}")
    lines.append(f"📩 Заявки с сайта: {fmt_int(int(leads))}")
    lines.append(f"👥 Всего заявок: {fmt_int(int(total))}")
    lines.append("")
    lines.append(f"💵 Затраты: {float(spend):.2f} $" if float(spend) > 0 else "💵 Затраты: —")
    if cpa is not None and float(cpa) > 0:
        lines.append(f"🎯 Blended CPA: {float(cpa):.2f} $")
    else:
        lines.append("🎯 Blended CPA: —")
    return _strip_fb_technical_lines("\n".join(lines))



def _account_timezone_info(aid: str) -> dict:
    try:
        from facebook_business.adobjects.adaccount import AdAccount

        acc = AdAccount(str(aid))
        with allow_fb_api_calls(reason="reporting_account_timezone"):
            info = safe_api_call(
                acc.api_get,
                fields=["timezone_name", "timezone_offset_hours_utc"],
                params={},
                _meta={"endpoint": "adaccount", "params": {"fields": "timezone_name,timezone_offset_hours_utc"}},
                _caller="reporting_account_timezone",
            )
        if isinstance(info, dict):
            return info
    except Exception:
        pass
    return {}


def _account_now(aid: str) -> tuple[datetime, str, float | None]:
    tz_name = ""
    off = None
    try:
        info = _account_timezone_info(str(aid)) or {}
        tz_name = str(info.get("timezone_name") or "")
        try:
            off = float(info.get("timezone_offset_hours_utc"))
        except Exception:
            off = None
    except Exception:
        tz_name = ""
        off = None

    try:
        if off is not None:
            now = datetime.utcnow() + timedelta(hours=float(off))
            return now, tz_name, float(off)
    except Exception:
        pass
    return datetime.now(ALMATY_TZ), tz_name, off


# ========= Утилиты форматирования =========
def fmt_int(n) -> str:
    try:
        return f"{int(float(n)):,}".replace(",", " ")
    except Exception:
        return "0"


def _load_report_cache() -> dict:
    try:
        with open(REPORT_CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_report_cache(d: dict) -> None:
    try:
        os.makedirs(os.path.dirname(REPORT_CACHE_FILE), exist_ok=True)
    except Exception:
        pass
    tmp = str(REPORT_CACHE_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, REPORT_CACHE_FILE)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


# ========== ИНСАЙТЫ (сырые данные) ==========
def fetch_insight(aid: str, period):
    """
    Достаёт инсайты:
    - сначала из локального кэша (load_local_insights)
    - если нет — запрашивает у Facebook
    - важно: ВСЕГДА приводим AdsInsights к обычному dict
    """
    store = load_local_insights(aid) or {}
    key = period_key(period)

    # Для периода "today" всегда берём свежие данные из API,
    # игнорируя имеющуюся запись в локальном кеше.
    use_cache = not (isinstance(period, str) and period == "today")

    if use_cache and key in store:
        name = get_account_name(aid)
        cached = store.get(key)
        # Old implementation stored snapshots-based aggregates. Do not reuse them.
        try:
            if isinstance(cached, dict) and str(cached.get("_source") or "") == "fb_api":
                return name, cached
        except Exception:
            pass

    now_acc, tz_name, tz_offset = _account_now(str(aid))

    # Strict calendar day in ad account TZ for today/yesterday.
    period_for_api: Any = period
    try:
        p = str(period or "")
        if p in {"today", "yesterday"}:
            day = now_acc.date() if p == "today" else (now_acc.date() - timedelta(days=1))
            ds = day.strftime("%Y-%m-%d")
            period_for_api = {"since": ds, "until": ds}
    except Exception:
        period_for_api = period

    fields = [
        "impressions",
        "cpm",
        "clicks",
        "cpc",
        "spend",
        "actions",
        "cost_per_action_type",
    ]

    params_extra = {
        # Match Ads Manager default as close as we can.
        # Spend/impressions/clicks should match regardless of attribution, but actions can differ.
        "action_report_time": "conversion",
        "use_unified_attribution_setting": True,
    }

    rows: list[dict] = []
    with allow_fb_api_calls(reason="reporting_fetch_insight"):
        rows = fetch_insights_bulk(
            str(aid),
            period=period_for_api,
            level="account",
            fields=list(fields),
            params_extra=dict(params_extra),
        )

    ins_dict = (rows[0] if rows else None) or {}
    if isinstance(ins_dict, dict):
        try:
            ins_dict["_source"] = "fb_api"
            ins_dict["_meta"] = {
                "timezone_name": tz_name,
                "timezone_offset_hours_utc": tz_offset,
                "period_input": period,
                "period_effective": period_for_api,
                "level": "account",
                "fields": list(fields),
                "params_extra": dict(params_extra),
            }
        except Exception:
            pass

    name = get_account_name(aid)

    store[key] = ins_dict
    save_local_insights(aid, store)

    return name, ins_dict


def build_report_debug(aid: str, kind: str, mode: str = "general") -> str:
    now_acc, tz_name, tz_offset = _account_now(str(aid))
    p = str(kind or "")
    if p in {"yday", "yesterday"}:
        period = "yesterday"
    elif p in {"today"}:
        period = "today"
    else:
        period = p

    period_for_api: Any = period
    try:
        if str(period) in {"today", "yesterday"}:
            day = now_acc.date() if str(period) == "today" else (now_acc.date() - timedelta(days=1))
            ds = day.strftime("%Y-%m-%d")
            period_for_api = {"since": ds, "until": ds}
    except Exception:
        period_for_api = period

    fields = [
        "impressions",
        "clicks",
        "spend",
        "actions",
    ]
    params_extra = {
        "action_report_time": "conversion",
        "use_unified_attribution_setting": True,
    }

    rows: list[dict] = []
    with allow_fb_api_calls(reason="report_debug"):
        rows = fetch_insights_bulk(
            str(aid),
            period=period_for_api,
            level="account",
            fields=list(fields),
            params_extra=dict(params_extra),
        )

    spend = 0.0
    impr = 0
    clicks = 0
    started = 0
    leads_sel = 0
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        try:
            spend += float(r.get("spend") or 0.0)
        except Exception:
            pass
        try:
            impr += int(r.get("impressions") or 0)
        except Exception:
            pass
        try:
            clicks += int(r.get("clicks") or 0)
        except Exception:
            pass
        acts = extract_actions(r)
        try:
            started += int(acts.get("onsite_conversion.messaging_conversation_started_7d", 0) or 0)
        except Exception:
            pass
        try:
            leads_sel += int(count_leads_from_actions(acts, aid=str(aid), lead_action_type=None) or 0)
        except Exception:
            pass

    lines = []
    lines.append("🧪 report_debug")
    lines.append(f"aid={str(aid)}")
    lines.append(f"kind={str(kind)} mode={str(mode)}")
    lines.append(f"timezone_name={str(tz_name or 'unknown')}")
    lines.append(f"timezone_offset_hours_utc={str(tz_offset) if tz_offset is not None else 'unknown'}")
    lines.append(f"account_now={now_acc.isoformat()}")
    lines.append(f"period_input={str(period)}")
    lines.append(f"period_effective={str(period_for_api)}")
    lines.append("level=account")
    lines.append(f"fields={','.join([str(x) for x in fields])}")
    lines.append(f"action_report_time={str(params_extra.get('action_report_time'))}")
    lines.append("attribution=unified(account_default)")
    lines.append("filters=none")
    lines.append(f"rows_count={int(len(rows or []))}")
    lines.append(f"sum_spend={float(spend):.2f}")
    lines.append(f"sum_impressions={int(impr)}")
    lines.append(f"sum_clicks={int(clicks)}")
    lines.append(f"started_conversations_7d={int(started)}")
    lines.append(f"leads_selected={int(leads_sel)}")
    return "\n".join(lines)


def _lead_cpl_account_level(aid: str, period_for_api: Any) -> tuple[int, float, float | None]:
    """Returns (leads_total, spend_leads_only, cpl)."""
    rows: list[dict] = []
    params_extra = {
        "action_report_time": "conversion",
        "use_unified_attribution_setting": True,
    }
    with allow_fb_api_calls(reason="reporting_lead_cpl"):
        rows = fetch_insights_bulk(
            str(aid),
            period=period_for_api,
            level="campaign",
            fields=["spend", "actions"],
            params_extra=dict(params_extra),
        )

    leads_total = 0
    spend_leads_only = 0.0
    for r in (rows or []):
        if not isinstance(r, dict):
            continue
        acts = extract_actions(r)
        leads = 0
        try:
            leads = int(count_leads_from_actions(acts, aid=str(aid), lead_action_type=None) or 0)
        except Exception:
            leads = 0
        if leads <= 0:
            continue
        leads_total += int(leads)
        try:
            spend_leads_only += float(r.get("spend") or 0.0)
        except Exception:
            pass

    cpl = (spend_leads_only / float(leads_total)) if (leads_total > 0 and spend_leads_only > 0) else None
    return int(leads_total), float(spend_leads_only), cpl


def _msg_cps_account_level(aid: str, period_for_api: Any) -> tuple[int, float, float | None]:
    """Returns (msgs_total, spend_msgs_only, cps)."""
    rows: list[dict] = []
    params_extra = {
        "action_report_time": "conversion",
        "use_unified_attribution_setting": True,
    }
    with allow_fb_api_calls(reason="reporting_msg_cps"):
        rows = fetch_insights_bulk(
            str(aid),
            period=period_for_api,
            level="campaign",
            fields=["spend", "actions"],
            params_extra=dict(params_extra),
        )

    msgs_total = 0
    spend_msgs_only = 0.0
    for r in (rows or []):
        if not isinstance(r, dict):
            continue
        acts = extract_actions(r)
        msgs = 0
        try:
            msgs = int(count_started_conversations_from_actions(acts) or 0)
        except Exception:
            msgs = 0
        if msgs <= 0:
            continue
        msgs_total += int(msgs)
        try:
            spend_msgs_only += float(r.get("spend") or 0.0)
        except Exception:
            pass

    cps = (spend_msgs_only / float(msgs_total)) if (msgs_total > 0 and spend_msgs_only > 0) else None
    return int(msgs_total), float(spend_msgs_only), cps


# ========== КЭШ ТЕКСТОВЫХ ОТЧЁТОВ ==========
def get_cached_report(aid: str, period, label: str = "") -> str:
    """
    Возвращает текст отчёта из кеша, если свежий,
    иначе строит заново и обновляет кеш.
    """
    # Для "today" всегда считаем отчёт на лету, без использования кэша.
    if period == "today":
        return build_report(aid, period, label)

    key = f"v{int(REPORT_TEXT_CACHE_VERSION)}:{period_key(period)}"
    now_ts = datetime.now().timestamp()

    cache = _load_report_cache()
    acc_cache = cache.get(aid, {})
    item = acc_cache.get(key)

    if item and (now_ts - float(item.get("ts", 0))) <= REPORT_CACHE_TTL:
        return item.get("text", "")

    text = build_report(aid, period, label)

    cache.setdefault(aid, {})
    cache[aid][key] = {"text": text, "ts": now_ts}
    _save_report_cache(cache)

    return text


# ========== СБОРКА ОТЧЁТА ПО АККАУНТУ ==========
def build_report(aid: str, period, label: str = "") -> str:
    """
    Базовый отчёт по аккаунту:
    - показы, CPM
    - клики (все) + CTR по всем кликам
    - "Клики" по ссылке (link_click) + CTR по ссылке
    - CPC / затраты
    - переписки / лиды / blended CPA (как в старом боте)
    """
    caller = "report"
    try:
        if isinstance(period, dict):
            caller = "report"
        elif str(period) == "today":
            caller = "rep_today"
        elif str(period) == "yesterday":
            caller = "rep_yday"
    except Exception:
        caller = "report"

    return build_report_with_caller(aid, period, label=label, caller=caller)


def build_report_with_caller(aid: str, period, label: str = "", *, caller: str) -> str:
    mode = ""
    cache_state = ""
    date_str = ""
    try:
        if isinstance(period, str) and str(period) in {"today", "yesterday"}:
            ins, mode, cache_state, date_str = _fetch_account_day_insight(aid=str(aid), kind=str(period), caller=str(caller))
            name = get_account_name(aid)
        else:
            name, ins = fetch_insight(aid, period)
    except Exception as e:
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return f"⚠ Ошибка по {get_account_name(aid)}:\n\n{e}"

    badge = "🟢"
    try:
        if float((ins or {}).get("spend") or 0.0) <= 0.0 and int((ins or {}).get("impressions") or 0) <= 0:
            badge = "🔴"
    except Exception:
        badge = "🟢"
    hdr = f"{badge} <b>{name}</b>{(' (' + label + ')') if label else ''}\n"
    if not ins:
        return hdr + "Нет данных за выбранный период"

    # Базовые метрики
    impressions = int(ins.get("impressions", 0) or 0)
    cpm = float(ins.get("cpm", 0) or 0)
    clicks_all = int(ins.get("clicks", 0) or 0)
    cpc = float(ins.get("cpc", 0) or 0)
    spend = float(ins.get("spend", 0) or 0)

    acts = extract_actions(ins)
    costs = extract_costs(ins)
    flags = metrics_flags(aid)

    # link_click берём из actions (action_type="link_click"),
    # не трогая fields=... у запроса, чтобы не ломать insights.
    link_clicks = int(acts.get("link_click", 0) or 0)

    # CTR'ы считаем сами
    ctr_all = (clicks_all / impressions * 100.0) if impressions > 0 else 0.0
    ctr_link = (link_clicks / impressions * 100.0) if impressions > 0 else 0.0

    # Заявки (как в старом боте)
    # msgs + leads и blended CPA рассчитываем через _blend_totals
    # (он уже использует те же action_type, что и раньше).
    _, msgs, leads, total_conv, blended_cpa = _blend_totals(ins, aid=aid)

    lead_cpa = None
    msg_cpa = None
    try:
        meta = ins.get("_meta") if isinstance(ins, dict) else None
        period_for_api = None
        if isinstance(meta, dict):
            period_for_api = meta.get("period_effective")
        if period_for_api is None:
            period_for_api = period

        _msgs_cnt, _spend_msgs_only, cps = _msg_cps_account_level(str(aid), period_for_api)
        msg_cpa = cps
        _leads_cnt, _spend_leads_only, cpl = _lead_cpl_account_level(str(aid), period_for_api)
        lead_cpa = cpl
    except Exception:
        lead_cpa = None
        msg_cpa = None

    body = []
    body.append(f"👁 Показы: {fmt_int(impressions)}")
    body.append(f"🎯 CPM: {cpm:.2f} $")
    body.append(f"🖱 Клики (все): {fmt_int(clicks_all)}")
    body.append(f"📈 CTR (все клики): {ctr_all:.2f} %")

    body.append(f"🔗 Клики (по ссылке): {fmt_int(link_clicks)}")
    body.append(f"📈 CTR (по ссылке): {ctr_link:.2f} %")

    if cpc > 0:
        body.append(f"💸 CPC: {cpc:.2f} $")
    else:
        body.append("💸 CPC: —")

    if spend > 0:
        body.append(f"💵 Затраты: {spend:.2f} $")
    else:
        body.append("💵 Затраты: —")

    if flags["messaging"]:
        body.append(f"✉️ Переписки: {msgs}")
        if msg_cpa is not None and float(msg_cpa) > 0:
            body.append(f"💬💲 Цена переписки: {float(msg_cpa):.2f} $")
        else:
            body.append("💬💲 Цена переписки: —")

    if flags["leads"]:
        body.append(f"♿️ Лиды: {leads}")
        if lead_cpa is not None and float(lead_cpa) > 0 and int(leads or 0) > 0:
            body.append(f"♿️💲 Цена лида: $ {float(lead_cpa):.2f}")
        else:
            body.append("♿️💲 Цена лида: —")

    # Blended CPA показываем только при включённых переписках и лидах одновременно
    # и когда обе метрики реально > 0.
    if flags.get("messaging") and flags.get("leads") and msgs > 0 and leads > 0:
        body.extend(format_blended_block(spend, msgs, leads).split("\n"))

    out = hdr + "\n".join(body)
    if isinstance(period, str) and str(period) in {"today", "yesterday"}:
        foot = _report_source_footer_lines(mode=str(mode or "daily_fallback"), cache_state=str(cache_state or "write"))
        out = (out.rstrip() + "\n\n" + "\n".join([str(x) for x in foot if str(x).strip()])).rstrip()
    return out


def format_blended_block(total_spend: float, msgs: int, leads: int) -> str:
    total_actions = int(msgs or 0) + int(leads or 0)
    spend = float(total_spend or 0.0)
    if total_actions > 0:
        blended_cpa = spend / float(total_actions)
        cpa_line = f"CPA: $ {blended_cpa:.2f}"
    else:
        cpa_line = "CPA: —"

    return "\n".join(
        [
            "────────────",
            "🧮 Blended CPA",
            f"Заявок: {total_actions}",
            cpa_line,
            f"Затраты: $ {spend:.2f}",
        ]
    )


def _strip_leading_separator(block: str) -> str:
    if not block:
        return block
    lines = block.split("\n")
    if lines and lines[0].strip() == "────────────":
        lines = lines[1:]
    return "\n".join(lines)


def _collapse_double_separators(text: str) -> str:
    if not text:
        return text
    out: list[str] = []
    for line in text.split("\n"):
        if out and out[-1].strip() == "────────────" and line.strip() == "────────────":
            continue
        out.append(line)
    return "\n".join(out)


def get_account_blended_totals(aid: str, period) -> tuple[float, int, int]:
    try:
        _, ins = fetch_insight(aid, period)
    except Exception:
        return (0.0, 0, 0)

    spend = float((ins or {}).get("spend", 0) or 0)
    _, msgs, leads, _, _ = _blend_totals(ins or {})
    return (spend, int(msgs or 0), int(leads or 0))


def format_entity_line(
    idx: int,
    name: str,
    spend: float,
    msgs: int,
    leads: int,
    msg_cpa: float | None,
    lead_cpa: float | None,
    flags: dict,
) -> str | None:
    eff_msgs = int(msgs or 0) if flags.get("messaging") else 0
    eff_leads = int(leads or 0) if flags.get("leads") else 0

    # Если обе цели выключены или по ним 0 — строку всё равно показываем,
    # чтобы кампании/адсеты со spend>0 не пропадали из отчёта.

    spend_f = float(spend or 0.0)
    parts = [f"{idx}) {name}", f"$ {spend_f:.2f}"]

    # Одна цель на строку: доминирующая по количеству, при равенстве — лиды.
    if eff_leads >= eff_msgs:
        # При равенстве (в т.ч. 0/0) — лиды.
        parts.append(f"♿️ лиды {eff_leads}")
        if lead_cpa is not None and float(lead_cpa) > 0:
            parts.append(f"цена лида $ {float(lead_cpa):.2f}")
        else:
            parts.append("цена лида —")
    else:
        parts.append(f"переписки {eff_msgs}")
        if msg_cpa is not None and float(msg_cpa) > 0:
            parts.append(f"цена переписки $ {float(msg_cpa):.2f}")
        else:
            parts.append("цена переписки —")

    return " — ".join(parts)


def _format_entity_block(
    name: str,
    spend: float,
    msgs: int,
    leads: int,
    msg_cpa: float | None,
    lead_cpa: float | None,
    flags: dict,
) -> str:
    lines: list[str] = []
    lines.append(str(name or "<без названия>"))
    lines.append(f"Затраты: $ {float(spend or 0.0):.2f}")

    if flags.get("messaging") and int(msgs or 0) > 0:
        cpa_part = f" (CPA $ {float(msg_cpa):.2f})" if msg_cpa is not None and float(msg_cpa) > 0 else " (CPA —)"
        lines.append(f"Переписки: {int(msgs or 0)}{cpa_part}")

    if flags.get("leads") and int(leads or 0) > 0:
        cpa_part = f" (CPA $ {float(lead_cpa):.2f})" if lead_cpa is not None and float(lead_cpa) > 0 else " (CPA —)"
        lines.append(f"Лиды: {int(leads or 0)}{cpa_part}")

    return "\n".join(lines)


def _truncate_entity_blocks(
    *,
    header: str,
    entities: list[dict],
    flags: dict,
    max_chars: int,
    current_chars: int,
    kind: str,
) -> tuple[str, int]:
    shown_blocks: list[str] = []
    for e in entities:
        name = str((e or {}).get("name") or "<без названия>")
        spend = float((e or {}).get("spend", 0.0) or 0.0)
        msgs = int((e or {}).get("msgs", 0) or 0)
        leads = int((e or {}).get("leads", 0) or 0)
        block = _format_entity_block(
            name,
            spend,
            msgs,
            leads,
            (e or {}).get("msg_cpa"),
            (e or {}).get("lead_cpa"),
            flags,
        )

        candidate = header
        if shown_blocks:
            candidate += "\n\n" + "\n\n".join(shown_blocks)
        candidate += "\n\n" + block

        if current_chars + len(candidate) > max_chars:
            break
        shown_blocks.append(block)

    remaining = max(0, len(entities) - len(shown_blocks))
    if not shown_blocks:
        return header + "\nнет данных за период", remaining

    text = header + "\n\n" + "\n\n".join(shown_blocks)
    if remaining > 0:
        tail = f"\n\n…и ещё {remaining} {kind}"
        if current_chars + len(text) + len(tail) <= max_chars:
            text += tail
    return text, remaining


def build_account_report(
    aid: str,
    period,
    level: str,
    label: str = "",
    top_n: int = 5,
):
    lvl = str(level or "ACCOUNT").upper()
    if lvl == "OFF":
        return ""

    caller = "report"
    try:
        if isinstance(period, str) and str(period) == "today":
            caller = "rep_today"
        elif isinstance(period, str) and str(period) == "yesterday":
            caller = "rep_yday"
    except Exception:
        caller = "report"

    base = build_report_with_caller(aid, period, label, caller=caller)
    if not base:
        return ""

    base_no_footer, footer_lines = _strip_source_footer(base)

    flags = metrics_flags(aid)

    acc_spend, acc_msgs, acc_leads = get_account_blended_totals(aid, period)
    acc_blended_block = format_blended_block(acc_spend, acc_msgs, acc_leads)
    acc_blended_after_sections = _strip_leading_separator(acc_blended_block)

    from .storage import load_accounts

    store = load_accounts()
    mr = (store.get(aid, {}) or {}).get("morning_report", {}) or {}
    show_blended_after_sections = mr.get("show_blended_after_sections", True)

    # Blended показываем только при включённых переписках и лидах одновременно
    # и когда обе метрики реально > 0.
    show_blended = (
        bool(flags.get("messaging"))
        and bool(flags.get("leads"))
        and int(acc_msgs or 0) > 0
        and int(acc_leads or 0) > 0
    )

    if lvl == "ACCOUNT":
        return base

    sep = "\n────────────\n"

    tg_max_chars = 3900
    current_chars = len(base)

    chunks: list[str] = []

    now = datetime.now(ALMATY_TZ)
    dates: list[str] = []
    if isinstance(period, dict):
        since = str(period.get("since") or "")
        until = str(period.get("until") or "")
        if since and until and since <= until:
            try:
                d1 = datetime.strptime(since, "%Y-%m-%d").date()
                d2 = datetime.strptime(until, "%Y-%m-%d").date()
                cur = d1
                while cur <= d2:
                    dates.append(cur.strftime("%Y-%m-%d"))
                    cur = cur + timedelta(days=1)
            except Exception:
                dates = []
    else:
        p = str(period or "")
        if p == "today":
            dates = [now.strftime("%Y-%m-%d")]
        elif p == "yesterday":
            dates = [(now - timedelta(days=1)).strftime("%Y-%m-%d")]
        elif p.startswith("last_") and p.endswith("d"):
            try:
                n = int(p.replace("last_", "").replace("d", ""))
            except Exception:
                n = 0
            if n > 0:
                end = (now - timedelta(days=1)).date()
                start = end - timedelta(days=max(0, n - 1))
                cur = start
                while cur <= end:
                    dates.append(cur.strftime("%Y-%m-%d"))
                    cur = cur + timedelta(days=1)

    all_rows: list[dict] = []
    with deny_fb_api_calls(reason="reporting_entities"):
        for d in dates:
            for h in list_snapshot_hours(str(aid), date_str=str(d)):
                snap = load_snapshot(str(aid), date_str=str(d), hour=int(h)) or {}
                if str(snap.get("status") or "") not in {"ready", "ready_low_confidence"}:
                    continue
                for r in (snap.get("rows") or []):
                    if isinstance(r, dict):
                        all_rows.append(r)

    entity_mode = "hourly_cache" if all_rows else "daily_fallback"
    entity_cache_state = "hit" if all_rows else "write"
    entity_date_str = ""
    if not all_rows:
        if isinstance(period, str) and str(period) in {"today", "yesterday"}:
            entity_date_str = now.strftime("%Y-%m-%d") if str(period) == "today" else (now - timedelta(days=1)).strftime("%Y-%m-%d")
            mh = _metrics_hash("entities_" + str(lvl), None)
            key = _daily_cache_key(scope="account", scope_id=str(aid), date_str=str(entity_date_str), level=str(lvl), metrics_hash=mh)
            ttl = _daily_ttl_seconds(date_str=str(entity_date_str))
            cached, hit = _daily_cache_get(key, ttl_seconds=int(ttl))
            if hit and isinstance(cached, list):
                all_rows = [r for r in (cached or []) if isinstance(r, dict)]
                entity_cache_state = "hit"
            else:
                fields = ["spend", "actions", "campaign_id", "campaign_name"]
                if lvl == "ADSET":
                    fields.extend(["adset_id", "adset_name"])
                params_extra = {"action_report_time": "conversion", "use_unified_attribution_setting": True}
                with allow_fb_api_calls(reason="reporting_daily_entities"):
                    rows = fetch_insights_bulk(
                        str(aid),
                        period=str(period),
                        level=str(lvl).lower(),
                        fields=list(fields),
                        params_extra=dict(params_extra),
                    )

                out_rows: list[dict] = []
                for rr in (rows or []):
                    if not isinstance(rr, dict):
                        continue
                    acts = extract_actions(rr)
                    try:
                        started = int(count_started_conversations_from_actions(acts) or 0)
                    except Exception:
                        started = 0
                    try:
                        website = int(count_leads_from_actions(acts, aid=str(aid), lead_action_type=None) or 0)
                    except Exception:
                        website = 0
                    try:
                        spend_v = float(rr.get("spend") or 0.0)
                    except Exception:
                        spend_v = 0.0
                    row_out = {
                        "campaign_id": rr.get("campaign_id"),
                        "campaign_name": rr.get("campaign_name"),
                        "adset_id": rr.get("adset_id"),
                        "name": rr.get("adset_name") or rr.get("name"),
                        "spend": spend_v,
                        "started_conversations": int(started),
                        "website_submit_applications": int(website),
                        "actions": dict(acts or {}),
                        "msgs": int(started),
                        "leads": int(website),
                        "total": int(started + website),
                    }
                    out_rows.append(row_out)

                all_rows = out_rows
                _daily_cache_set(key, list(out_rows))
                entity_cache_state = "write"

        else:
            fields = ["spend", "actions", "campaign_id", "campaign_name"]
            if lvl == "ADSET":
                fields.extend(["adset_id", "adset_name"])
            params_extra = {"action_report_time": "conversion", "use_unified_attribution_setting": True}
            with allow_fb_api_calls(reason="reporting_range_entities"):
                rows = fetch_insights_bulk(
                    str(aid),
                    period=period,
                    level=str(lvl).lower(),
                    fields=list(fields),
                    params_extra=dict(params_extra),
                )
            out_rows = []
            for rr in (rows or []):
                if not isinstance(rr, dict):
                    continue
                acts = extract_actions(rr)
                try:
                    started = int(count_started_conversations_from_actions(acts) or 0)
                except Exception:
                    started = 0
                try:
                    website = int(count_leads_from_actions(acts, aid=str(aid), lead_action_type=None) or 0)
                except Exception:
                    website = 0
                try:
                    spend_v = float(rr.get("spend") or 0.0)
                except Exception:
                    spend_v = 0.0
                row_out = {
                    "campaign_id": rr.get("campaign_id"),
                    "campaign_name": rr.get("campaign_name"),
                    "adset_id": rr.get("adset_id"),
                    "name": rr.get("adset_name") or rr.get("name"),
                    "spend": spend_v,
                    "started_conversations": int(started),
                    "website_submit_applications": int(website),
                    "actions": dict(acts or {}),
                    "msgs": int(started),
                    "leads": int(website),
                    "total": int(started + website),
                }
                out_rows.append(row_out)
            all_rows = out_rows
            entity_cache_state = "write"

    def _group_rows(key_field: str, name_field: str) -> list[dict]:
        agg: dict[str, dict] = {}
        for r in all_rows:
            k = str((r or {}).get(key_field) or "")
            if not k:
                continue
            nm = str((r or {}).get(name_field) or (r or {}).get("name") or k)
            a = agg.setdefault(
                k,
                {
                    "id": k,
                    "name": nm,
                    "spend": 0.0,
                    "spend_for_msgs": 0.0,
                    "spend_for_leads": 0.0,
                    "msgs": 0,
                    "leads": 0,
                    "total": 0,
                    "msg_cpa": None,
                    "lead_cpa": None,
                },
            )
            try:
                a["spend"] = float(a.get("spend") or 0.0) + float((r or {}).get("spend") or 0.0)
            except Exception:
                pass

            row_msgs = 0
            try:
                row_msgs = int((r or {}).get("started_conversations") or (r or {}).get("msgs") or 0)
            except Exception:
                row_msgs = 0
            row_leads = 0
            try:
                actions_map = (r or {}).get("actions")
                if isinstance(actions_map, dict) and actions_map:
                    row_leads = int(count_leads_from_actions(actions_map, aid=str(aid), lead_action_type=None) or 0)
                else:
                    row_leads = int((r or {}).get("website_submit_applications") or (r or {}).get("leads") or 0)
            except Exception:
                row_leads = 0

            try:
                a["msgs"] = int(a.get("msgs") or 0) + int(row_msgs or 0)
            except Exception:
                pass
            try:
                a["leads"] = int(a.get("leads") or 0) + int(row_leads or 0)
            except Exception:
                pass

            # TЗ-3: attribute spend only to objectives that happened in this row.
            try:
                row_spend = float((r or {}).get("spend") or 0.0)
            except Exception:
                row_spend = 0.0
            if row_msgs > 0 and row_spend > 0:
                try:
                    a["spend_for_msgs"] = float(a.get("spend_for_msgs") or 0.0) + float(row_spend)
                except Exception:
                    pass
            if row_leads > 0 and row_spend > 0:
                try:
                    a["spend_for_leads"] = float(a.get("spend_for_leads") or 0.0) + float(row_spend)
                except Exception:
                    pass
            try:
                t = (r or {}).get("total")
                if t is None:
                    t = int(row_msgs or 0) + int(row_leads or 0)
                a["total"] = int(a.get("total") or 0) + int(t or 0)
            except Exception:
                pass

        out = list(agg.values())
        for it in out:
            try:
                sp_msgs = float(it.get("spend_for_msgs") or 0.0)
            except Exception:
                sp_msgs = 0.0
            try:
                sp_leads = float(it.get("spend_for_leads") or 0.0)
            except Exception:
                sp_leads = 0.0
            try:
                ms = int(it.get("msgs") or 0)
            except Exception:
                ms = 0
            try:
                ld = int(it.get("leads") or 0)
            except Exception:
                ld = 0
            it["msg_cpa"] = (sp_msgs / float(ms)) if (ms > 0 and sp_msgs > 0) else None
            it["lead_cpa"] = (sp_leads / float(ld)) if (ld > 0 and sp_leads > 0) else None
        return out

    camps = _group_rows("campaign_id", "campaign_name")

    # Единственный обязательный фильтр: spend > 0
    camps_spend = [c for c in (camps or []) if float((c or {}).get("spend", 0.0) or 0.0) > 0]
    camps_text, _ = _truncate_entity_blocks(
        header="📣 Кампании",
        entities=camps_spend,
        flags=flags,
        max_chars=tg_max_chars,
        current_chars=current_chars + len(sep),
        kind="кампаний",
    )
    chunks.append(camps_text)
    current_chars += len(sep) + len(camps_text)
    if show_blended_after_sections and show_blended:
        if current_chars + len(sep) + len(acc_blended_after_sections) <= tg_max_chars:
            chunks.append(acc_blended_after_sections)
            current_chars += len(sep) + len(acc_blended_after_sections)

    if lvl == "ADSET":
        # Snapshot collector stores adset name under row["name"].
        adsets = _group_rows("adset_id", "name")

        # Единственный обязательный фильтр: spend > 0
        adsets_spend = [a for a in (adsets or []) if float((a or {}).get("spend", 0.0) or 0.0) > 0]
        adsets_text, _ = _truncate_entity_blocks(
            header="🧩 Адсеты",
            entities=adsets_spend,
            flags=flags,
            max_chars=tg_max_chars,
            current_chars=current_chars + len(sep),
            kind="адсетов",
        )
        chunks.append(adsets_text)
        current_chars += len(sep) + len(adsets_text)
        if show_blended_after_sections and show_blended:
            if current_chars + len(sep) + len(acc_blended_after_sections) <= tg_max_chars:
                chunks.append(acc_blended_after_sections)
                current_chars += len(sep) + len(acc_blended_after_sections)

    if lvl == "AD":
        # We don't collect ad-level rows in heatmap snapshots.
        # Keep the signature stable and show an empty/"нет данных" block.
        ads: list[dict] = []

        # Единственный обязательный фильтр: spend > 0
        ads_spend = [a for a in (ads or []) if float((a or {}).get("spend", 0.0) or 0.0) > 0]
        ads_text, _ = _truncate_entity_blocks(
            header="🎯 Объявления",
            entities=ads_spend,
            flags=flags,
            max_chars=tg_max_chars,
            current_chars=current_chars + len(sep),
            kind="объявлений",
        )
        chunks.append(ads_text)
        current_chars += len(sep) + len(ads_text)
        if show_blended_after_sections and show_blended:
            if current_chars + len(sep) + len(acc_blended_after_sections) <= tg_max_chars:
                chunks.append(acc_blended_after_sections)
                current_chars += len(sep) + len(acc_blended_after_sections)

    # Разделитель обязателен между блоками.
    out = str(base_no_footer) + sep + sep.join(chunks)
    out = _collapse_double_separators(out)
    if isinstance(period, str) and str(period) in {"today", "yesterday"}:
        try:
            logging.getLogger(__name__).info(
                "caller=%s mode=%s cache=%s scope=account scope_id=%s date=%s level=%s",
                str(caller),
                str(entity_mode),
                str(entity_cache_state),
                str(aid),
                str(entity_date_str or ""),
                str(lvl),
            )
        except Exception:
            pass
        foot = _report_source_footer_lines(mode=str(entity_mode), cache_state=str(entity_cache_state))
        out = (out.rstrip() + "\n\n" + "\n".join([str(x) for x in foot if str(x).strip()])).rstrip()
    return out


async def send_period_report(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: str,
    period,
    label: str = "",
):
    """
    Всегда шлём отчёты ТОЛЬКО по enabled=True аккаунтам.
    За 'today' — всегда живые данные (build_report),
    за остальные периоды — через кеш.
    """
    from .storage import load_accounts, get_enabled_accounts_in_order

    store = load_accounts()

    caller = "report"
    if str(period) == "today":
        caller = "rep_today"
    elif str(period) == "yesterday":
        caller = "rep_yday"

    for aid in get_enabled_accounts_in_order():
        if not store.get(aid, {}).get("enabled", True):
            continue

        if period == "today":
            txt = build_report_with_caller(aid, period, label, caller=caller)
        else:
            txt = get_cached_report(aid, period, label)

        if txt:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=txt,
                parse_mode="HTML",
            )


# ======== Сравнение периодов =========
def build_comparison_report(
    aid: str, period1, label1: str, period2, label2: str
) -> str:
    from .storage import get_account_name

    def _extract_since(p):
        if isinstance(p, dict):
            s = p.get("since")
            try:
                return datetime.strptime(s, "%Y-%m-%d")
            except Exception:
                return None
        return None

    d1 = _extract_since(period1)
    d2 = _extract_since(period2)
    if d1 and d2 and d1 > d2:
        period1, period2 = period2, period1
        label1, label2 = label2, label1

    try:
        _, ins1 = fetch_insight(aid, period1)
        _, ins2 = fetch_insight(aid, period2)
    except Exception as e:
        return f"⚠ Ошибка при получении данных: {e.__class__.__name__}: {str(e)}"

    if not ins1 and not ins2:
        return f"Нет данных по {get_account_name(aid)} за оба периода."

    flags = metrics_flags(aid)

    def _stat(ins):
        if not ins:
            return {
                "impr": 0,
                "cpm": 0.0,
                "clicks": 0,
                "cpc": 0.0,
                "spend": 0.0,
                "msgs": 0,
                "leads": 0,
                "total": 0,
                "cpa": None,
            }
        impr = int(ins.get("impressions", 0) or 0)
        cpm = float(ins.get("cpm", 0) or 0)
        clicks = int(ins.get("clicks", 0) or 0)
        cpc = float(ins.get("cpc", 0) or 0)
        spend, msgs, leads, total, blended = _blend_totals(ins)
        return {
            "impr": impr,
            "cpm": cpm,
            "clicks": clicks,
            "cpc": cpc,
            "spend": spend,
            "msgs": msgs,
            "leads": leads,
            "total": total,
            "cpa": blended,
        }

    s1 = _stat(ins1)
    s2 = _stat(ins2)

    def _fmt_money(v: float) -> str:
        return f"{v:.2f} $"

    def _fmt_cpa(cpa):
        return f"{cpa:.2f} $" if cpa is not None else "—"

    def _pct_change(old: float, new: float):
        if old == 0:
            return None
        return (new - old) / old * 100.0

    txt_lines = []
    txt_lines.append(f"📊 <b>{get_account_name(aid)}</b>")
    txt_lines.append(f"Старый период: {label1}")
    txt_lines.append(f"Новый период: {label2}")
    txt_lines.append("")

    # 1️⃣ Старый
    txt_lines.append(f"1️⃣ <b>{label1}</b> (старый период)")
    txt_lines.append(f"   👁 Охваты: {fmt_int(s1['impr'])}")
    txt_lines.append(f"   🖱 Клики: {fmt_int(s1['clicks'])}")
    txt_lines.append(f"   💵 Затраты: {_fmt_money(s1['spend'])}")
    txt_lines.append(f"   🎯 CPM: {s1['cpm']:.2f} $")
    txt_lines.append(f"   💸 CPC: {s1['cpc']:.2f} $")
    if flags["messaging"]:
        txt_lines.append(f"   💬 Переписки: {s1['msgs']}")
    if flags["leads"]:
        txt_lines.append(f"   📩 Лиды: {s1['leads']}")
    if flags["messaging"] or flags["leads"]:
        txt_lines.append(f"   🧮 Заявки всего: {s1['total']}")
        txt_lines.append(f"   🎯 CPA: {_fmt_cpa(s1['cpa'])}")
    txt_lines.append("")

    # 2️⃣ Новый
    txt_lines.append(f"2️⃣ <b>{label2}</b> (новый период)")
    txt_lines.append(f"   👁 Охваты: {fmt_int(s2['impr'])}")
    txt_lines.append(f"   🖱 Клики: {fmt_int(s2['clicks'])}")
    txt_lines.append(f"   💵 Затраты: {_fmt_money(s2['spend'])}")
    txt_lines.append(f"   🎯 CPM: {s2['cpm']:.2f} $")
    txt_lines.append(f"   💸 CPC: {s2['cpc']:.2f} $")
    if flags["messaging"]:
        txt_lines.append(f"   💬 Переписки: {s2['msgs']}")
    if flags["leads"]:
        txt_lines.append(f"   📩 Лиды: {s2['leads']}")
    if flags["messaging"] or flags["leads"]:
        txt_lines.append(f"   🧮 Заявки всего: {s2['total']}")
        txt_lines.append(f"   🎯 CPA: {_fmt_cpa(s2['cpa'])}")
    txt_lines.append("")

    # 3️⃣ Сравнение
    txt_lines.append("3️⃣ <b>Сравнение (новый vs старый)</b>")

    def _add_diff(
        label: str,
        old_v: float,
        new_v: float,
        is_better_lower: bool = False,
        fmt_func=None,
        icon: str = "",
    ):
        if fmt_func is None:
            fmt_func = lambda x: str(int(x))
        base = f"{icon} {label}: {fmt_func(old_v)} → {fmt_func(new_v)}"
        pct = _pct_change(old_v, new_v)
        if pct is None:
            txt_lines.append(base + " (Δ %: н/д)")
            return
        if pct == 0:
            sign = "➡️"
        else:
            sign = (
                "📈"
                if ((not is_better_lower and pct > 0) or (is_better_lower and pct < 0))
                else "📉"
            )
        txt_lines.append(f"{base}   {sign} {pct:+.1f}%")

    _add_diff("Охваты", s1["impr"], s2["impr"], False, fmt_int, "👁")
    _add_diff("Клики", s1["clicks"], s2["clicks"], False, fmt_int, "🖱")
    _add_diff("Затраты", s1["spend"], s2["spend"], False, _fmt_money, "💵")
    _add_diff("CPM", s1["cpm"], s2["cpm"], True, lambda v: f"{v:.2f} $", "🎯")
    _add_diff("CPC", s1["cpc"], s2["cpc"], True, lambda v: f"{v:.2f} $", "💸")

    if flags["messaging"]:
        _add_diff(
            "Переписки",
            s1["msgs"],
            s2["msgs"],
            False,
            lambda v: str(int(v)),
            "💬",
        )
    if flags["leads"]:
        _add_diff(
            "Лиды",
            s1["leads"],
            s2["leads"],
            False,
            lambda v: str(int(v)),
            "📩",
        )

    if flags["messaging"] or flags["leads"]:
        _add_diff(
            "Заявки всего",
            s1["total"],
            s2["total"],
            False,
            lambda v: str(int(v)),
            "🧮",
        )
        if s1["cpa"] is not None and s2["cpa"] is not None:
            _add_diff("CPA", s1["cpa"], s2["cpa"], True, _fmt_cpa, "🎯")

    return "\n".join(txt_lines)


# ======== парсинг дат для кастомных диапазонов =========
_RANGE_RE = re.compile(
    r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{2})\.(\d{4})\s*$"
)


def parse_range(s: str):
    m = _RANGE_RE.match(s)
    if not m:
        return None
    d1 = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    d2 = datetime(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    if d1 > d2:
        d1, d2 = d2, d1
    return (
        {"since": d1.strftime("%Y-%m-%d"), "until": d2.strftime("%Y-%m-%d")},
        f"{d1.strftime('%d.%m')}-{d2.strftime('%d.%m')}",
    )


def parse_two_ranges(s: str):
    parts = [p.strip() for p in re.split(r"[;\n]+", s) if p.strip()]
    if len(parts) != 2:
        return None
    r1 = parse_range(parts[0])
    r2 = parse_range(parts[1])
    if not r1 or not r2:
        return None
    return r1, r2
