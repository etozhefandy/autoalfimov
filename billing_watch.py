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

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError
from telegram.ext import Application, ContextTypes

from fb_report.constants import ALMATY_TZ, DATA_DIR, kzt_round_up_1000

from services.facebook_api import allow_fb_api_calls


_last_status: Dict[str, Any] = {}
_pending_recheck: Dict[str, Dict[str, Any]] = {}


_FOLLOWUPS_FILE = os.path.join(DATA_DIR, "billing_followups.json")


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
    if not aid or not group_chat_id:
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
        return
    except Exception as e:
        _log_api_error("billing_followup", str(aid), None, None, str(e))
        return

    billing_detected = (status != 1) or (float(cur_usd) < 0)
    if not billing_detected:
        try:
            if isinstance(followups, dict):
                followups.pop(str(aid), None)
            state["followups"] = followups
            _save_state(state)
        except Exception:
            pass
        return

    try:
        if rate > 0:
            kzt = int(kzt_round_up_1000(float(cur_usd) * float(rate)))
        else:
            kzt = 0
    except Exception:
        kzt = 0

    text = f"🚨 {name}! у нас биллинг — Итоговая сумма: {float(cur_usd):.2f} $ — ≈ {int(kzt)} ₸\n@Zz11mmaa"
    try:
        await context.bot.send_message(chat_id=group_chat_id, text=text)
    except Exception:
        return

    try:
        if isinstance(followups, dict):
            followups.pop(str(aid), None)
        state["followups"] = followups
        _save_state(state)
    except Exception:
        pass


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

    for aid in get_enabled_accounts():
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

            lines.append("Через 30 минут выдам сумму с корректировками.")

            text = "\n".join(lines)
            await context.bot.send_message(chat_id=group_chat_id, text=text)

            try:
                if isinstance(st_prelim, dict):
                    st_prelim[str(aid)] = _dt_iso(now)
            except Exception:
                pass

            due = now + timedelta(minutes=30)
            if isinstance(st_followups, dict):
                st_followups[str(aid)] = {
                    "due_at": _dt_iso(due),
                    "first_usd": float(balance_usd),
                    "name": str(name),
                    "rate": float(rate),
                    "group_chat_id": str(group_chat_id),
                }
            try:
                context.job_queue.run_once(
                    _billing_followup_job,
                    when=timedelta(minutes=30),
                    data={"aid": str(aid), "group_chat_id": str(group_chat_id)},
                    name=f"billing_followup|{str(aid)}",
                )
            except Exception:
                pass

    state["last_detected"] = st_detect if isinstance(st_detect, dict) else {}
    state["last_status"] = st_last_status if isinstance(st_last_status, dict) else {}
    state["prelim_sent_at"] = st_prelim if isinstance(st_prelim, dict) else {}
    state["followups"] = st_followups if isinstance(st_followups, dict) else {}
    _save_state(state)


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
        state = _load_state() or {}
        followups = state.get("followups") if isinstance(state.get("followups"), dict) else {}
        if isinstance(followups, dict):
            for aid, item in list(followups.items()):
                if not isinstance(item, dict):
                    continue
                due_at = _parse_dt(item.get("due_at"))
                gchat = str(item.get("group_chat_id") or group_chat_id or "")
                if not gchat:
                    continue
                delay = timedelta(seconds=1)
                if due_at:
                    delta = due_at - now
                    delay = delta if delta.total_seconds() > 1 else timedelta(seconds=1)
                app.job_queue.run_once(
                    _billing_followup_job,
                    when=delay,
                    data={"aid": str(aid), "group_chat_id": str(gchat)},
                    name=f"billing_followup|{str(aid)}",
                )
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

