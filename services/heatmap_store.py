import os
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fb_report.constants import ALMATY_TZ, DATA_DIR


_BASE_DIR = os.path.join(DATA_DIR, "heatmap_snapshots")


def _atomic_write_json(path: str, obj: Any) -> None:
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


def _snapshot_path(aid: str, *, date_str: str, hour: int) -> str:
    hh = f"{int(hour):02d}"
    return os.path.join(_BASE_DIR, str(aid), str(date_str), hh, "snapshot.json")


def load_snapshot(aid: str, *, date_str: str, hour: int) -> Optional[Dict[str, Any]]:
    path = _snapshot_path(aid, date_str=date_str, hour=hour)
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def save_snapshot(snapshot: Dict[str, Any]) -> None:
    aid = str(snapshot.get("account_id") or "")
    date_str = str(snapshot.get("date") or "")
    hour = int(snapshot.get("hour") or 0)
    if not aid or not date_str:
        raise ValueError("snapshot missing account_id/date")
    path = _snapshot_path(aid, date_str=date_str, hour=hour)
    _atomic_write_json(path, snapshot)


def list_snapshot_hours(aid: str, *, date_str: str) -> List[int]:
    base = os.path.join(_BASE_DIR, str(aid), str(date_str))
    try:
        entries = os.listdir(base)
    except Exception:
        return []
    out: List[int] = []
    for e in entries:
        try:
            h = int(str(e))
        except Exception:
            continue
        path = os.path.join(base, str(e), "snapshot.json")
        if os.path.exists(path):
            out.append(h)
    out.sort()
    return out


def find_latest_ready_snapshots(
    aid: str,
    *,
    max_hours: int,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Returns latest ready snapshots (descending time), up to max_hours.

    This is filesystem-only and never touches Facebook API.
    """
    if max_hours <= 0:
        return []

    now = now or datetime.now(ALMATY_TZ)
    cur = now.replace(minute=0, second=0, microsecond=0)

    out: List[Dict[str, Any]] = []
    # Safety cap to avoid endless loops in case of timezone issues.
    for _ in range(min(24 * 14, max_hours + 48)):
        cur = cur - timedelta(hours=1)
        date_str = cur.strftime("%Y-%m-%d")
        hour = int(cur.strftime("%H"))

        snap = load_snapshot(aid, date_str=date_str, hour=hour)
        if not snap:
            continue
        if str(snap.get("status") or "") not in {"ready", "ready_low_confidence"}:
            continue
        out.append(snap)
        if len(out) >= max_hours:
            break

    return out


def sum_ready_spend_for_date(
    aid: str,
    *,
    date_str: str,
    hours: List[int],
) -> Tuple[Optional[float], str, str]:
    """Sums spend across ready snapshots for a given date/hours.

    Returns:
      - spend_total (float) or None
      - status: ready|missing|collecting|failed
      - reason
    """
    if not hours:
        return None, "missing", "hours_empty"

    total = 0.0
    for h in hours:
        snap = load_snapshot(aid, date_str=date_str, hour=int(h))
        if not snap:
            return None, "missing", "no_snapshot"
        st = str(snap.get("status") or "")
        if st not in {"ready", "ready_low_confidence"}:
            if st == "failed":
                err = (snap.get("error") or {}) if isinstance(snap.get("error"), dict) else {}
                et = str(err.get("type") or "failed")
                return None, "failed", "rate_limit" if et == "rate_limit" else "snapshot_failed"
            err = (snap.get("error") or {}) if isinstance(snap.get("error"), dict) else {}
            et = str(err.get("type") or "")
            if et == "rate_limit":
                return None, "collecting", "rate_limit"
            return None, "collecting", "snapshot_collecting"

        for r in (snap.get("rows") or []):
            if not isinstance(r, dict):
                continue
            try:
                total += float(r.get("spend") or 0.0)
            except Exception:
                continue

    if float(total) <= 0.0:
        return float(total), "ready", "low_volume"
    return float(total), "ready", ""


def _prev_full_hour(now: Optional[datetime] = None) -> Tuple[str, int, datetime, datetime, datetime]:
    now = now or datetime.now(ALMATY_TZ)
    end_dt = now.replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(hours=1)
    deadline_dt = end_dt + timedelta(minutes=30)
    date_str = start_dt.strftime("%Y-%m-%d")
    hour = int(start_dt.strftime("%H"))
    return date_str, hour, start_dt, end_dt, deadline_dt


def prev_full_hour_window(now: Optional[datetime] = None) -> Dict[str, Any]:
    date_str, hour, start_dt, end_dt, deadline_dt = _prev_full_hour(now=now)
    return {
        "date": date_str,
        "hour": int(hour),
        "start": start_dt,
        "end": end_dt,
        "deadline": deadline_dt,
        "window": {
            "start": start_dt.strftime("%H:00"),
            "end": end_dt.strftime("%H:00"),
            "tz": "Asia/Almaty",
        },
    }


def build_snapshot_shell(
    aid: str,
    *,
    date_str: str,
    hour: int,
    start_dt: datetime,
    end_dt: datetime,
    deadline_dt: datetime,
    min_rows_required: int = 30,
) -> Dict[str, Any]:
    try:
        start_s = start_dt.strftime("%H:00")
        end_s = end_dt.strftime("%H:00")
    except Exception:
        start_s = ""
        end_s = ""

    try:
        start_ts = start_dt.isoformat()
        end_ts = end_dt.isoformat()
    except Exception:
        start_ts = ""
        end_ts = ""

    return {
        "account_id": str(aid),
        "date": str(date_str),
        "hour": int(hour),
        "window": {"start": start_s, "end": end_s, "start_ts": start_ts, "end_ts": end_ts, "tz": "Asia/Almaty"},
        "status": "collecting",
        "reason": "snapshot_collecting",
        "attempts": 0,
        "collected_rows": 0,
        "rows_count": 0,
        "spend": 0.0,
        "min_rows_required": int(min_rows_required),
        "last_try_at": None,
        "next_try_at": None,
        "deadline_at": deadline_dt.isoformat(),
        "source": "heatmap_cache",
        "data_level": "adset",
        "rows": [],
        "error": None,
    }


def _normalize_row_hour(row: Dict[str, Any]) -> Optional[int]:
    raw = row.get("hourly_stats_aggregated_by_advertiser_time_zone")
    if not raw:
        return None
    s = str(raw)
    m = None
    try:
        import re

        m = re.search(r"(\d{1,2})\s*:\s*\d{2}", s)
    except Exception:
        m = None
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def get_heatmap_dataset(
    aid: str,
    *,
    date_str: str,
    hours: List[int],
) -> Tuple[Optional[Dict[str, Any]], str, str, Dict[str, Any]]:
    uniq_hours = []
    seen = set()
    for h in (hours or []):
        try:
            hi = int(h)
        except Exception:
            continue
        if hi < 0 or hi > 23:
            continue
        if hi in seen:
            continue
        seen.add(hi)
        uniq_hours.append(hi)

    def _hour_window_iso(date_s: str, hour_i: int) -> Tuple[str, str]:
        try:
            base = datetime.strptime(str(date_s), "%Y-%m-%d").replace(tzinfo=ALMATY_TZ)
            start_dt = base.replace(hour=int(hour_i), minute=0, second=0, microsecond=0)
            end_dt = start_dt + timedelta(hours=1)
            return start_dt.isoformat(), end_dt.isoformat()
        except Exception:
            return "", ""

    win_start_ts = ""
    win_end_ts = ""
    if uniq_hours:
        win_start_ts, _ = _hour_window_iso(date_str, min(uniq_hours))
        _, win_end_ts = _hour_window_iso(date_str, max(uniq_hours))

    meta: Dict[str, Any] = {
        "account_id": str(aid),
        "date": str(date_str),
        "hours": uniq_hours,
        "source": "heatmap_cache",
        "window": {"start_ts": win_start_ts, "end_ts": win_end_ts, "tz": "Asia/Almaty"},
    }

    if not uniq_hours:
        return None, "missing", "hours_empty", meta

    snapshots: List[Dict[str, Any]] = []
    low_confidence: List[int] = []
    missing: List[int] = []
    collecting: List[int] = []
    failed: List[int] = []

    for h in uniq_hours:
        snap = load_snapshot(aid, date_str=date_str, hour=h)
        if not snap:
            missing.append(h)
            continue
        st = str(snap.get("status") or "")
        if st == "ready":
            snapshots.append(snap)
        elif st == "ready_low_confidence":
            snapshots.append(snap)
            low_confidence.append(h)
        elif st == "collecting":
            collecting.append(h)
        elif st == "failed":
            failed.append(h)
        else:
            collecting.append(h)

    if missing:
        meta["missing_hours"] = list(missing)
        return None, "missing", "no_snapshot", meta

    if failed:
        snap = load_snapshot(aid, date_str=date_str, hour=failed[0]) or {}
        err = snap.get("error") or {}
        et = str((err or {}).get("type") or "snapshot_failed")
        meta["error"] = err if isinstance(err, dict) else {"type": et}
        meta["last_try_at"] = snap.get("last_try_at")
        meta["attempts"] = int(snap.get("attempts") or 0)
        return None, "failed", "rate_limit" if et == "rate_limit" else "snapshot_failed", meta

    if collecting:
        snap = load_snapshot(aid, date_str=date_str, hour=collecting[0]) or {}
        attempts = int(snap.get("attempts") or 0)
        meta["attempts"] = attempts
        meta["deadline_at"] = str(snap.get("deadline_at") or "")
        meta["last_try_at"] = snap.get("last_try_at")
        # Collector runs every 10 minutes.
        meta["next_try_in_min"] = 10
        try:
            last_iso = str(snap.get("last_try_at") or "")
            dt = datetime.fromisoformat(last_iso) if last_iso else None
            if dt and not dt.tzinfo:
                dt = ALMATY_TZ.localize(dt)
            if dt:
                meta["next_try_at"] = (dt + timedelta(minutes=10)).isoformat()
        except Exception:
            pass
        err = snap.get("error") or {}
        if isinstance(err, dict) and str(err.get("type") or "") == "rate_limit":
            meta["error"] = err
            return None, "collecting", "rate_limit", meta
        return None, "collecting", "snapshot_collecting", meta

    by_adset: Dict[str, Dict[str, Any]] = {}
    total_rows = 0
    spend_total = 0.0
    for snap in snapshots:
        for r in (snap.get("rows") or []):
            if not isinstance(r, dict):
                continue
            adset_id = str(r.get("adset_id") or "")
            if not adset_id:
                continue
            total_rows += 1
            spend = float(r.get("spend") or 0.0)
            spend_total += float(spend)
            msgs = int(float(r.get("msgs") or 0) or 0)
            leads = int(float(r.get("leads") or 0) or 0)
            total = int(float(r.get("total") or 0) or 0)

            name = r.get("name")
            campaign_id = r.get("campaign_id")

            it = by_adset.setdefault(
                adset_id,
                {
                    "adset_id": adset_id,
                    "name": None,
                    "campaign_id": None,
                    "spend": 0.0,
                    "msgs": 0,
                    "leads": 0,
                    "total": 0,
                },
            )

            if name and not it.get("name"):
                it["name"] = name
            if campaign_id and not it.get("campaign_id"):
                it["campaign_id"] = campaign_id

            it["spend"] = float(it.get("spend") or 0.0) + spend
            it["msgs"] = int(it.get("msgs") or 0) + msgs
            it["leads"] = int(it.get("leads") or 0) + leads
            it["total"] = int(it.get("total") or 0) + total

    out_rows: List[Dict[str, Any]] = []
    for _k, v in by_adset.items():
        spend = float(v.get("spend") or 0.0)
        total = int(v.get("total") or 0)
        cpl = (spend / float(total)) if (total > 0 and spend > 0) else None
        v["cpl"] = cpl
        out_rows.append(v)

    out_rows.sort(key=lambda x: float(x.get("spend") or 0.0), reverse=True)

    meta["rows_count"] = int(len(out_rows))
    meta["collected_rows"] = int(total_rows)
    meta["spend"] = float(spend_total)
    if low_confidence:
        meta["low_confidence_hours"] = list(low_confidence)
        meta["low_confidence"] = True

    if int(total_rows) <= 0:
        return None, "failed", "low_volume", meta

    last_snapshot_at = None
    try:
        last_snapshot_at = max(str((s or {}).get("last_try_at") or "") for s in (snapshots or []))
    except Exception:
        last_snapshot_at = None
    meta["last_snapshot_at"] = last_snapshot_at

    ds = {
        "account_id": str(aid),
        "date": str(date_str),
        "hours": uniq_hours,
        "source": "heatmap_cache",
        "rows": out_rows,
        "collected_rows": int(total_rows),
    }
    if low_confidence:
        return ds, "ready", "low_volume", meta
    return ds, "ready", "", meta
