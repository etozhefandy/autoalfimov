"""Простой watcher биллингов.

Логика аналогична старому скрипту:
- каждые 10 минут опрашиваем аккаунты;
- если account_status меняется с 1 (ACTIVE) на любое другое значение,
  отправляем тревожное сообщение в групповой чат;
- через ~20 минут после первого алёрта по аккаунту опрашиваем баланс ещё раз
  и шлём уточнённую сумму.
"""

from typing import Callable, Iterable, Optional, Dict, Any
from datetime import datetime, timedelta
import json
import os
import logging
import time

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError
from telegram.ext import Application, ContextTypes

from fb_report.constants import ALMATY_TZ, DATA_DIR, kzt_round_up_1000

from fb_report.storage import load_accounts

from services.facebook_api import allow_fb_api_calls


_last_status: Dict[str, Any] = {}
_pending_recheck: Dict[str, Dict[str, Any]] = {}


_FOLLOWUPS_FILE = os.path.join(DATA_DIR, "billing_followups.json")
_BILLING_CACHE_FILE = os.path.join(DATA_DIR, "billing_cache.json")

BILLING_BALANCE_EPSILON_USD = 0.01


def _load_billing_cache() -> dict:
    try:
        with open(_BILLING_CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_billing_cache(obj: dict) -> None:
    try:
        _atomic_write_json(_BILLING_CACHE_FILE, obj if isinstance(obj, dict) else {})
    except Exception:
        pass


def _billing_cache_get_usd(aid: str) -> float | None:
    st = _load_billing_cache() or {}
    item = st.get(str(aid))
    if not isinstance(item, dict):
        return None
    try:
        return float(item.get("last_usd"))
    except Exception:
        return None


def _billing_cache_write(aid: str, usd: float) -> None:
    st = _load_billing_cache() or {}
    st[str(aid)] = {"last_usd": float(usd), "last_ts": int(time.time())}
    _save_billing_cache(st)
    try:
        logging.getLogger(__name__).info("billing_cache_write aid=%s usd=%.2f", str(aid), float(usd))
    except Exception:
        pass


def _atomic_write_json(path: str, obj: dict) -> None:
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _load_state() -> dict:
    try:
        with open(_FOLLOWUPS_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        _atomic_write_json(_FOLLOWUPS_FILE, state if isinstance(state, dict) else {})
    except Exception:
        pass


def _parse_dt(v: Any) -> datetime | None:
    if not v:
        return None
    try:
        dt = datetime.fromisoformat(str(v))
        if not dt.tzinfo:
            dt = ALMATY_TZ.localize(dt)
        return dt.astimezone(ALMATY_TZ)
    except Exception:
        return None


def _dt_iso(dt: datetime) -> str:
    try:
        if not dt.tzinfo:
            dt = ALMATY_TZ.localize(dt)
        return dt.astimezone(ALMATY_TZ).isoformat()
    except Exception:
        try:
            return dt.isoformat()
        except Exception:
            return ""


def _is_no_access_error(http_status: int | None, message: str | None) -> bool:
    if int(http_status or 0) != 403:
        return False
    msg_l = str(message or "").lower()
    if "has not granted" not in msg_l:
        return False
    if ("ads_read" not in msg_l) and ("ads_management" not in msg_l):
        return False
    return True


def _log_api_error(caller: str, aid: str, http_status: int | None, fb_code: int | None, message: str | None) -> None:
    try:
        logging.getLogger(__name__).warning(
            "billing_api_error caller=%s aid=%s http_status=%s fb_code=%s message=%s",
            str(caller),
            str(aid),
            str(http_status) if http_status is not None else "",
            str(fb_code) if fb_code is not None else "",
            str(message or ""),
        )
    except Exception:
        pass


async def _billing_followup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = getattr(getattr(context, "job", None), "data", None) or {}
    aid = str((data or {}).get("aid") or "")
    group_chat_id = str((data or {}).get("group_chat_id") or "")
    group_chat_ids = (data or {}).get("group_chat_ids")
    if not isinstance(group_chat_ids, list):
        group_chat_ids = []
    group_chat_ids = [str(x) for x in group_chat_ids if str(x).strip()]

    if not aid:
        return

    try:
        store = load_accounts() or {}
    except Exception:
        store = {}
    if store and not (store.get(str(aid), {}) or {}).get("enabled", True):
        try:
            state = _load_state() or {}
            followups = state.get("followups") if isinstance(state.get("followups"), dict) else {}
            if isinstance(followups, dict):
                followups.pop(str(aid), None)
            state["followups"] = followups
            _save_state(state)
        except Exception:
            pass
        return

    state = _load_state() or {}
    followups = state.get("followups") if isinstance(state.get("followups"), dict) else {}
    item = followups.get(str(aid)) if isinstance(followups, dict) else None
    if not isinstance(item, dict):
        return

    due_at = _parse_dt(item.get("due_at"))
    now = datetime.now(ALMATY_TZ)
    if due_at and now < due_at:
        return

    name = str(item.get("name") or "")
    first_usd = float(item.get("first_usd") or 0.0)
    rate = float(item.get("rate") or 0.0)
    stored_ids = item.get("group_chat_ids")
    if isinstance(stored_ids, list):
        group_chat_ids = [str(x) for x in stored_ids if str(x).strip()]
    if group_chat_id and group_chat_id not in set(group_chat_ids):
        group_chat_ids.append(str(group_chat_id))

    if not group_chat_ids:
        # Backward/edge compatibility: restore targets even if state/job data misses them.
        try:
            from fb_report.client_groups import active_groups_for_account

            extra = active_groups_for_account(str(aid)) or []
            for cid in extra:
                if str(cid) not in set(group_chat_ids):
                    group_chat_ids.append(str(cid))
        except Exception:
            pass

    if not group_chat_ids:
        return

    cur_usd = first_usd
    status = None
    try:
        with allow_fb_api_calls(reason="billing_followup"):
            info = AdAccount(str(aid)).api_get(fields=["name", "account_status", "balance"])
            if hasattr(info, "export_all_data"):
                info = info.export_all_data()
        if isinstance(info, dict):
            try:
                status = int(info.get("account_status"))
            except Exception:
                status = None
            try:
                cur_usd = float(info.get("balance", 0) or 0) / 100.0
            except Exception:
                cur_usd = first_usd
            try:
                if not name:
                    name = str(info.get("name") or "")
            except Exception:
                pass
    except FacebookRequestError as e:
        http_status = None
        fb_code = None
        try:
            http_status = int(
                getattr(e, "http_status", None)()
                if callable(getattr(e, "http_status", None))
                else getattr(e, "http_status", None)
            )
        except Exception:
            http_status = None
        try:
            fb_code = int(e.api_error_code())
        except Exception:
            fb_code = None
        msg = None
        try:
            msg = str(e)
        except Exception:
            msg = None
        _log_api_error("billing_followup", str(aid), http_status, fb_code, msg)
        try:
            text = f"🔄 {name or str(aid)} — не удалось уточнить баланс (ошибка API)"
            for cid in group_chat_ids:
                try:
                    await context.bot.send_message(chat_id=str(cid), text=str(text))
                except Exception:
                    continue
        except Exception:
            pass
        return
    except Exception as e:
        _log_api_error("billing_followup", str(aid), None, None, str(e))
        try:
            text = f"🔄 {name or str(aid)} — не удалось уточнить баланс (ошибка)"
            for cid in group_chat_ids:
                try:
                    await context.bot.send_message(chat_id=str(cid), text=str(text))
                except Exception:
                    continue
        except Exception:
            pass
        return

    _billing_cache_write(str(aid), float(cur_usd))

    changed = False
    try:
        changed = abs(float(cur_usd) - float(first_usd)) >= float(BILLING_BALANCE_EPSILON_USD)
    except Exception:
        changed = False
    try:
        logging.getLogger(__name__).info(
            "billing_followup aid=%s changed=%s",
            str(aid),
            "true" if changed else "false",
        )
    except Exception:
        pass

    try:
        if isinstance(followups, dict):
            followups.pop(str(aid), None)
        state["followups"] = followups
        _save_state(state)
    except Exception:
        pass

    try:
        if rate > 0:
            kzt = int(kzt_round_up_1000(float(cur_usd) * float(rate)))
        else:
            kzt = 0
    except Exception:
        kzt = 0

    if not group_chat_ids:
        return

    if changed:
        head = f"🔄 {name} — баланс обновлён"
    else:
        head = f"🔄 {name} — баланс подтверждён"
    text = f"{head}\n💵 {float(cur_usd):.2f} $ | 🇰🇿 {int(kzt)} ₸"
    for cid in group_chat_ids:
        try:
            await context.bot.send_message(chat_id=str(cid), text=text)
        except Exception:
            continue


async def _billing_watch_job(
    context: ContextTypes.DEFAULT_TYPE,
    get_enabled_accounts: Callable[[], Iterable[str]],
    get_account_name: Callable[[str], str],
    usd_to_kzt,
    kzt_round_up_1000,
    group_chat_id: Optional[str],
) -> None:
    if not group_chat_id:
        return

    global _last_status, _pending_recheck

    now = datetime.now(ALMATY_TZ)

    state = _load_state() or {}
    st_detect = state.get("last_detected") if isinstance(state.get("last_detected"), dict) else {}
    st_last_status = state.get("last_status") if isinstance(state.get("last_status"), dict) else {}
    st_prelim = state.get("prelim_sent_at") if isinstance(state.get("prelim_sent_at"), dict) else {}
    st_followups = state.get("followups") if isinstance(state.get("followups"), dict) else {}

    try:
        cooldown_h = float(os.getenv("BILLING_COOLDOWN_HOURS", "8") or 8)
    except Exception:
        cooldown_h = 8.0
    if cooldown_h <= 0:
        cooldown_h = 8.0

    try:
        rate = float(usd_to_kzt()) if usd_to_kzt else 0.0
    except Exception:
        rate = 0.0

    try:
        store = load_accounts() or {}
    except Exception:
        store = {}

    all_ids = list(get_enabled_accounts() or [])
    enabled_count = 0
    processed = 0
    skipped_disabled = 0
    if store:
        for _aid in all_ids:
            if (store.get(str(_aid), {}) or {}).get("enabled", True):
                enabled_count += 1
            else:
                skipped_disabled += 1
    else:
        enabled_count = int(len(all_ids))

    for aid in all_ids:
        if store and not (store.get(str(aid), {}) or {}).get("enabled", True):
            continue
        processed += 1
        try:
            with allow_fb_api_calls(reason="billing_watch_poll"):
                info = AdAccount(str(aid)).api_get(fields=["name", "account_status", "balance"])
                if hasattr(info, "export_all_data"):
                    info = info.export_all_data()
        except FacebookRequestError as e:
            http_status = None
            fb_code = None
            try:
                http_status = int(
                    getattr(e, "http_status", None)()
                    if callable(getattr(e, "http_status", None))
                    else getattr(e, "http_status", None)
                )
            except Exception:
                http_status = None
            try:
                fb_code = int(e.api_error_code())
            except Exception:
                fb_code = None
            msg = None
            try:
                msg = str(e)
            except Exception:
                msg = None
            _log_api_error("billing_watch_poll", str(aid), http_status, fb_code, msg)
            continue
        except Exception as e:
            _log_api_error("billing_watch_poll", str(aid), None, None, str(e))
            continue

        if not isinstance(info, dict):
            continue

        name = str(info.get("name") or get_account_name(aid))
        try:
            status = int(info.get("account_status"))
        except Exception:
            status = None
        try:
            balance_usd = float(info.get("balance", 0) or 0) / 100.0
        except Exception:
            balance_usd = 0.0

        _billing_cache_write(str(aid), float(balance_usd))

        billing_detected = (status != 1) or (float(balance_usd) < 0)

        prev_detected = _last_status.get(aid)
        if isinstance(st_detect, dict):
            prev_detected = st_detect.get(str(aid), prev_detected)
            st_detect[str(aid)] = bool(billing_detected)
        if isinstance(st_last_status, dict):
            st_last_status[str(aid)] = status
        _last_status[aid] = bool(billing_detected)

        if (not prev_detected) and billing_detected:
            last_prelim_at = _parse_dt((st_prelim or {}).get(str(aid)) if isinstance(st_prelim, dict) else None)
            if last_prelim_at and (now - last_prelim_at) < timedelta(hours=float(cooldown_h)):
                continue

            if isinstance(st_followups, dict) and str(aid) in st_followups:
                continue

            # Первый алёрт: подчёркиваем, что это предварительная сумма
            lines = [
                "⚠️ ⚠️ ⚠️ Ахтунг! Биллинг в {name}".format(name=name),
                f"Предварительная сумма: {balance_usd:.2f} $",
            ]

            if usd_to_kzt and kzt_round_up_1000:
                try:
                    rate = float(usd_to_kzt())
                    kzt = kzt_round_up_1000(balance_usd * rate)
                    lines.append(f"Примерно: ≈ {kzt} ₸")
                except Exception:
                    pass

            lines.append("Через 60 минут выдам сумму с корректировками.")

            text = "\n".join(lines)

            targets: list[str] = []
            try:
                if group_chat_id:
                    targets.append(str(group_chat_id))
            except Exception:
                targets = []
            try:
                from fb_report.client_groups import active_groups_for_account

                extra = active_groups_for_account(str(aid)) or []
                for cid in extra:
                    if str(cid) not in set(targets):
                        targets.append(str(cid))
            except Exception:
                pass

            for cid in targets:
                try:
                    await context.bot.send_message(chat_id=str(cid), text=text)
                except Exception:
                    continue

            try:
                if isinstance(st_prelim, dict):
                    st_prelim[str(aid)] = _dt_iso(now)
            except Exception:
                pass

            due = now + timedelta(hours=1)
            if isinstance(st_followups, dict):
                st_followups[str(aid)] = {
                    "due_at": _dt_iso(due),
                    "first_usd": float(balance_usd),
                    "name": str(name),
                    "rate": float(rate),
                    "group_chat_id": str(group_chat_id),
                    "group_chat_ids": list(targets),
                }
            try:
                logging.getLogger(__name__).info(
                    "billing_followup scheduled aid=%s run_at=%s",
                    str(aid),
                    str(_dt_iso(due)),
                )
            except Exception:
                pass
            try:
                context.job_queue.run_once(
                    _billing_followup_job,
                    when=timedelta(hours=1),
                    data={"aid": str(aid), "group_chat_id": str(group_chat_id), "group_chat_ids": list(targets)},
                    name=f"billing_followup|{str(aid)}",
                )
            except Exception:
                pass

    state["last_detected"] = st_detect if isinstance(st_detect, dict) else {}
    state["last_status"] = st_last_status if isinstance(st_last_status, dict) else {}
    state["prelim_sent_at"] = st_prelim if isinstance(st_prelim, dict) else {}
    state["followups"] = st_followups if isinstance(st_followups, dict) else {}
    _save_state(state)

    try:
        logging.getLogger(__name__).info(
            "caller=billing_watch_poll enabled=%s processed=%s skipped_disabled=%s",
            str(int(enabled_count)),
            str(int(processed)),
            str(int(skipped_disabled)),
        )
    except Exception:
        pass


def init_billing_watch(
    app: Application,
    get_enabled_accounts: Callable[[], Iterable[str]],
    get_account_name: Callable[[str], str],
    usd_to_kzt=None,
    kzt_round_up_1000=None,
    owner_id: Optional[int] = None,
    group_chat_id: Optional[str] = None,
) -> None:
    # Restore pending followups on startup.
    try:
        now = datetime.now(ALMATY_TZ)
        try:
            store = load_accounts() or {}
        except Exception:
            store = {}
        state = _load_state() or {}
        followups = state.get("followups") if isinstance(state.get("followups"), dict) else {}
        if isinstance(followups, dict):
            for aid, item in list(followups.items()):
                if not isinstance(item, dict):
                    continue
                if store and not (store.get(str(aid), {}) or {}).get("enabled", True):
                    followups.pop(str(aid), None)
                    continue
                due_at = _parse_dt(item.get("due_at"))
                gchat = str(item.get("group_chat_id") or group_chat_id or "")
                if not gchat:
                    continue
                stored_ids = item.get("group_chat_ids") if isinstance(item, dict) else None
                group_ids = []
                if isinstance(stored_ids, list):
                    group_ids = [str(x) for x in stored_ids if str(x).strip()]
                delay = timedelta(seconds=1)
                if due_at:
                    delta = due_at - now
                    delay = delta if delta.total_seconds() > 1 else timedelta(seconds=1)
                app.job_queue.run_once(
                    _billing_followup_job,
                    when=delay,
                    data={"aid": str(aid), "group_chat_id": str(gchat), "group_chat_ids": list(group_ids)},
                    name=f"billing_followup|{str(aid)}",
                )
        try:
            state["followups"] = followups
            _save_state(state)
        except Exception:
            pass
    except Exception:
        pass

    # Мониторинг биллингов каждые 10 минут, как в старом скрипте.
    app.job_queue.run_repeating(
        lambda ctx: _billing_watch_job(
            ctx,
            get_enabled_accounts,
            get_account_name,
            usd_to_kzt,
            kzt_round_up_1000,
            group_chat_id,
        ),
        interval=600,
        first=10,
    )

