from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign

from fb_report.client_groups import is_client_group
from fb_report.constants import ALLOWED_CHAT_IDS, SUPERADMIN_USER_ID
from fb_report.storage import get_account_name, iter_enabled_accounts_only
from services.analytics import parse_insight
from services.facebook_api import allow_fb_api_calls, fetch_adsets, fetch_ads, fetch_campaigns, fetch_insights_bulk, safe_api_call
from services.reports import fmt_int

_LOG = logging.getLogger(__name__)


def _state(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    st = context.user_data.get("ads_manage")
    if not isinstance(st, dict):
        st = {}
        context.user_data["ads_manage"] = st
    return st


def _can_access(update: Update) -> bool:
    try:
        uid = update.effective_user.id if update.effective_user else None
        chat_id = str(update.effective_chat.id) if update.effective_chat else ""
        chat_type = str(update.effective_chat.type) if update.effective_chat else ""
    except Exception:
        return False

    if uid != SUPERADMIN_USER_ID:
        return False

    if chat_id and is_client_group(chat_id):
        return False

    if chat_type == "private":
        return True

    return chat_id in set(str(x) for x in (ALLOWED_CHAT_IDS or []))


def _status_text(item: Dict[str, Any]) -> str:
    eff = str((item or {}).get("effective_status") or "").upper().strip()
    if eff:
        return eff
    st = str((item or {}).get("status") or "").upper().strip()
    return st or "UNKNOWN"


def _status_emoji(status: str) -> str:
    s = str(status or "").upper().strip()
    if s == "ACTIVE":
        return "🟢"
    if s == "PAUSED":
        return "⏸"
    if s in {"ARCHIVED", "COMPLETED"}:
        return "⚫️"
    if s in {"DISAPPROVED", "ERROR"}:
        return "🔴"
    return "🟡"


def _fmt_money(v: Optional[float]) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.2f} $"
    except Exception:
        return "—"


def _fmt_cpa(spend: float, leads: int) -> str:
    if int(leads or 0) <= 0:
        return "—"
    try:
        return f"{float(spend or 0.0) / float(leads):.2f} $"
    except Exception:
        return "—"


def _enabled_account_ids() -> List[str]:
    out: List[str] = []
    for aid in iter_enabled_accounts_only():
        s = str(aid or "").strip()
        if s:
            out.append(s)
    return out


def _human_fb_error(info: Optional[Dict[str, Any]]) -> str:
    if not isinstance(info, dict):
        return "не удалось выполнить действие"
    kind = str(info.get("kind") or "")
    http_status = info.get("http_status")
    code = info.get("code")
    if kind == "blocked_by_policy":
        return "FB API вызов заблокирован политикой"
    if http_status == 403 or code == 200:
        return "⚠️ Нет доступа к аккаунту/объекту"
    if kind == "rate_limit" or code == 17:
        return "⚠️ FB rate limit, попробуй позже"
    msg = str(info.get("message") or "").strip()
    return msg or "не удалось выполнить действие"


def _accounts_kb(ids: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for aid in ids:
        rows.append([InlineKeyboardButton(str(get_account_name(aid) or aid), callback_data=f"am_acc|{aid}")])
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def _list_kb(*, level: str, items: List[Dict[str, Any]], selected_id: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    for it in items[:30]:
        oid = str(it.get("id") or "")
        if not oid:
            continue
        st = _status_text(it)
        emo = _status_emoji(st)
        nm = str(it.get("name") or "")
        lbl = f"{emo} {nm}".strip()
        if selected_id and selected_id == oid:
            lbl = f"👉 {lbl}"
        if len(lbl) > 60:
            lbl = lbl[:57] + "…"
        rows.append([InlineKeyboardButton(lbl, callback_data=f"am_sel|{oid}")])

    action_row: List[InlineKeyboardButton] = [
        InlineKeyboardButton("Открыть", callback_data="am_open"),
        InlineKeyboardButton("Вкл/выкл", callback_data="am_toggle"),
    ]
    if level == "adsets":
        action_row.append(InlineKeyboardButton("Изменить бюджет", callback_data="am_budget"))

    rows.append(action_row)
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data="am_refresh"), InlineKeyboardButton("Редактирование", callback_data="am_edit")])

    if level == "campaigns":
        rows.append([InlineKeyboardButton("⬅️ К аккаунтам", callback_data="am_menu")])
    else:
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="am_back")])

    return InlineKeyboardMarkup(rows)


def _confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Подтвердить", callback_data="am_confirm"), InlineKeyboardButton("❌ Отмена", callback_data="am_cancel")]]
    )


def _bulk_metrics(*, aid: str, level: str, ids: List[str], filter_field: str, id_key: str) -> Dict[str, Dict[str, Any]]:
    if not ids:
        return {}

    params_extra = {
        "filtering": [{"field": str(filter_field), "operator": "IN", "value": [str(x) for x in ids]}],
        "action_report_time": "conversion",
        "use_unified_attribution_setting": True,
    }

    fields = ["spend", "reach", "actions", str(id_key)]
    with allow_fb_api_calls(reason="ads_manage:insights"):
        rows = fetch_insights_bulk(str(aid), period="last_7d", level=str(level), fields=fields, params_extra=params_extra)

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        oid = str((r or {}).get(str(id_key)) or "").strip()
        if oid:
            out[oid] = dict(r or {})
    return out


def _render_lines(*, title: str, items: List[Dict[str, Any]], metrics: Dict[str, Dict[str, Any]], aid: str, selected_id: str) -> str:
    lines: List[str] = [f"<b>{title}</b>"]

    if not items:
        lines.extend(["", "Нет данных."])
        return "\n".join(lines)

    lines.append("")

    for it in items[:30]:
        oid = str(it.get("id") or "")
        if not oid:
            continue

        st = _status_text(it)
        emo = _status_emoji(st)
        nm = str(it.get("name") or "<без названия>")

        m = metrics.get(oid) or {}
        parsed = parse_insight(dict(m or {}), aid=str(aid), lead_action_type=None)
        leads = int(parsed.get("leads") or 0)
        spend = float(parsed.get("spend") or 0.0)
        reach = None
        try:
            reach = int(float((m or {}).get("reach") or 0) or 0)
        except Exception:
            reach = None

        prefix = "👉 " if selected_id and selected_id == oid else ""
        lines.append(
            f"{prefix}{emo} {nm} — {fmt_int(leads)} / {_fmt_cpa(spend, leads)} — {fmt_int(reach) if reach is not None else '—'} — {_fmt_money(spend)}"
        )

    return "\n".join(lines)


async def open_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _can_access(update):
        return

    ids = _enabled_account_ids()
    if not ids:
        await update.effective_message.reply_text("Нет включённых аккаунтов.")
        return

    st = _state(context)
    st.clear()
    st["level"] = "accounts"

    await update.effective_message.reply_text(
        "🛠 <b>Управление рекламой</b>\n\nВыбери рекламный аккаунт:",
        reply_markup=_accounts_kb(ids),
        parse_mode=ParseMode.HTML,
    )


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not _can_access(update):
        return False

    payload = context.user_data.get("ads_manage_await_budget")
    if not isinstance(payload, dict):
        return False

    raw = str(update.message.text or "").strip() if update.message else ""
    try:
        new_budget = float(raw.replace(",", "."))
    except Exception:
        await update.message.reply_text("Введите число, например 20.5")
        context.user_data["ads_manage_await_budget"] = payload
        return True

    if new_budget <= 0:
        await update.message.reply_text("Бюджет должен быть больше 0")
        context.user_data["ads_manage_await_budget"] = payload
        return True

    st = _state(context)
    st["pending"] = {
        "kind": "budget",
        "aid": str(payload.get("aid") or ""),
        "adset_id": str(payload.get("adset_id") or ""),
        "old_budget": payload.get("old_budget"),
        "new_budget": new_budget,
    }
    context.user_data.pop("ads_manage_await_budget", None)

    await update.message.reply_text(
        f"Поставить budget={new_budget:.2f}?",
        reply_markup=_confirm_kb(),
        parse_mode=ParseMode.HTML,
    )
    return True


def _refresh_allowed(context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, int]:
    st = _state(context)
    now = float(time.time())
    last = float(st.get("last_refresh_ts") or 0.0)
    if (now - last) < 5.0:
        return False, max(0, int(5 - (now - last)))
    st["last_refresh_ts"] = now
    return True, 0


async def _render_accounts(q, context: ContextTypes.DEFAULT_TYPE) -> None:
    ids = _enabled_account_ids()
    st = _state(context)
    st.clear()
    st["level"] = "accounts"

    if not ids:
        await q.edit_message_text("Нет включённых аккаунтов.")
        return

    await q.edit_message_text(
        "🛠 <b>Управление рекламой</b>\n\nВыбери рекламный аккаунт:",
        reply_markup=_accounts_kb(ids),
        parse_mode=ParseMode.HTML,
    )


async def _render_campaigns(q, context: ContextTypes.DEFAULT_TYPE, *, force: bool) -> None:
    st = _state(context)
    aid = str(st.get("aid") or "")
    if not aid:
        await _render_accounts(q, context)
        return

    if force or not isinstance(st.get("items"), list) or st.get("level") != "campaigns":
        with allow_fb_api_calls(reason="ads_manage:list_campaigns"):
            items = fetch_campaigns(aid)
        ids = [str(x.get("id") or "") for x in (items or []) if str(x.get("id") or "").strip()]
        metrics = _bulk_metrics(aid=aid, level="campaign", ids=ids, filter_field="campaign.id", id_key="campaign_id")
        st["items"] = items
        st["metrics"] = metrics
        st["level"] = "campaigns"

    items = list(st.get("items") or [])
    metrics = dict(st.get("metrics") or {})
    selected_id = str(st.get("selected_id") or "")

    text = _render_lines(title=f"Кампании — {get_account_name(aid)}", items=items, metrics=metrics, aid=aid, selected_id=selected_id)
    await q.edit_message_text(text, reply_markup=_list_kb(level="campaigns", items=items, selected_id=selected_id), parse_mode=ParseMode.HTML)


async def _render_adsets(q, context: ContextTypes.DEFAULT_TYPE, *, force: bool) -> None:
    st = _state(context)
    aid = str(st.get("aid") or "")
    campaign_id = str(st.get("campaign_id") or "")
    if not aid or not campaign_id:
        await _render_campaigns(q, context, force=False)
        return

    if force or not isinstance(st.get("items"), list) or st.get("level") != "adsets":
        with allow_fb_api_calls(reason="ads_manage:list_adsets"):
            all_items = fetch_adsets(aid)
        items = [x for x in (all_items or []) if str((x or {}).get("campaign_id") or "") == str(campaign_id)]
        ids = [str(x.get("id") or "") for x in (items or []) if str(x.get("id") or "").strip()]
        metrics = _bulk_metrics(aid=aid, level="adset", ids=ids, filter_field="adset.id", id_key="adset_id")
        st["items"] = items
        st["metrics"] = metrics
        st["level"] = "adsets"

    items = list(st.get("items") or [])
    metrics = dict(st.get("metrics") or {})
    selected_id = str(st.get("selected_id") or "")

    text = _render_lines(title=f"Адсеты — {get_account_name(aid)}", items=items, metrics=metrics, aid=aid, selected_id=selected_id)
    await q.edit_message_text(text, reply_markup=_list_kb(level="adsets", items=items, selected_id=selected_id), parse_mode=ParseMode.HTML)


async def _render_ads(q, context: ContextTypes.DEFAULT_TYPE, *, force: bool) -> None:
    st = _state(context)
    aid = str(st.get("aid") or "")
    adset_id = str(st.get("adset_id") or "")
    if not aid or not adset_id:
        await _render_adsets(q, context, force=False)
        return

    if force or not isinstance(st.get("items"), list) or st.get("level") != "ads":
        with allow_fb_api_calls(reason="ads_manage:list_ads"):
            all_items = fetch_ads(aid)
        items = [x for x in (all_items or []) if str((x or {}).get("adset_id") or "") == str(adset_id)]
        ids = [str(x.get("id") or "") for x in (items or []) if str(x.get("id") or "").strip()]
        metrics = _bulk_metrics(aid=aid, level="ad", ids=ids, filter_field="ad.id", id_key="ad_id")
        st["items"] = items
        st["metrics"] = metrics
        st["level"] = "ads"

    items = list(st.get("items") or [])
    metrics = dict(st.get("metrics") or {})
    selected_id = str(st.get("selected_id") or "")

    text = _render_lines(title=f"Объявления — {get_account_name(aid)}", items=items, metrics=metrics, aid=aid, selected_id=selected_id)
    await q.edit_message_text(text, reply_markup=_list_kb(level="ads", items=items, selected_id=selected_id), parse_mode=ParseMode.HTML)


async def _start_toggle(q, context: ContextTypes.DEFAULT_TYPE) -> None:
    st = _state(context)
    level = str(st.get("level") or "")
    oid = str(st.get("selected_id") or "").strip()
    if not oid:
        await q.answer("Выбери объект", show_alert=False)
        return

    items = list(st.get("items") or [])
    meta = next((x for x in items if str((x or {}).get("id") or "") == oid), None) or {}

    cur = _status_text(meta)
    if cur not in {"ACTIVE", "PAUSED"}:
        await q.answer("Нельзя переключить этот статус", show_alert=True)
        return

    new_status = "PAUSED" if cur == "ACTIVE" else "ACTIVE"

    st["pending"] = {
        "kind": "toggle",
        "aid": str(st.get("aid") or ""),
        "level": level,
        "object_id": oid,
        "name": str(meta.get("name") or ""),
        "old_status": cur,
        "new_status": new_status,
    }

    human = {"campaigns": "Кампания", "adsets": "Адсет", "ads": "Объявление"}.get(level, "Объект")
    await q.edit_message_text(
        f"{human}: <b>{str(meta.get('name') or '')}</b>\n\nПереключить статус {cur} → {new_status}?",
        reply_markup=_confirm_kb(),
        parse_mode=ParseMode.HTML,
    )


async def _start_budget(q, context: ContextTypes.DEFAULT_TYPE) -> None:
    st = _state(context)
    if str(st.get("level") or "") != "adsets":
        await q.answer("Бюджет можно менять только у адсета", show_alert=True)
        return

    adset_id = str(st.get("selected_id") or "").strip()
    if not adset_id:
        await q.answer("Выбери адсет", show_alert=False)
        return

    items = list(st.get("items") or [])
    meta = next((x for x in items if str((x or {}).get("id") or "") == adset_id), None) or {}

    cur = None
    try:
        if meta.get("daily_budget") is not None:
            cur = float(meta.get("daily_budget"))
    except Exception:
        cur = None

    context.user_data["ads_manage_await_budget"] = {"aid": str(st.get("aid") or ""), "adset_id": adset_id, "old_budget": cur}

    suffix = f" Текущий: {cur:.2f}." if cur is not None else ""
    await q.message.reply_text(f"Введи бюджет в день (в валюте аккаунта).{suffix}")


async def _apply_confirm(q, context: ContextTypes.DEFAULT_TYPE) -> None:
    st = _state(context)
    pending = st.get("pending")
    if not isinstance(pending, dict):
        await q.answer("Нет действия", show_alert=False)
        return

    if pending.get("kind") == "toggle":
        aid = str(pending.get("aid") or "")
        level = str(pending.get("level") or "")
        oid = str(pending.get("object_id") or "")
        old_status = str(pending.get("old_status") or "")
        new_status = str(pending.get("new_status") or "")

        res = None
        info = None
        try:
            with allow_fb_api_calls(reason="ads_manage:toggle"):
                if level == "campaigns":
                    obj = Campaign(oid)
                elif level == "adsets":
                    obj = AdSet(oid)
                else:
                    obj = Ad(oid)
                res, info = safe_api_call(obj.api_update, params={"status": new_status}, _caller="ads_manage", _aid=aid, _return_error_info=True)
        except Exception as e:
            info = {"kind": "exception", "message": str(e)}

        ok = res is not None
        err_msg = "" if ok else _human_fb_error(info)

        _LOG.info(
            "caller=ads_manage action=toggle aid=%s object_id=%s old_value=%s new_value=%s result=%s error=%s",
            str(aid),
            str(oid),
            str(old_status),
            str(new_status),
            "ok" if ok else "fail",
            str(err_msg),
        )

        st.pop("pending", None)
        if ok:
            await q.message.reply_text("✅ Статус изменён")
            cur = str(st.get("level") or "")
            if cur == "campaigns":
                await _render_campaigns(q, context, force=True)
            elif cur == "adsets":
                await _render_adsets(q, context, force=True)
            else:
                await _render_ads(q, context, force=True)
            return

        await q.edit_message_text(f"⚠️ Ошибка: {err_msg}", reply_markup=_confirm_kb(), parse_mode=ParseMode.HTML)
        return

    if pending.get("kind") == "budget":
        aid = str(pending.get("aid") or "")
        adset_id = str(pending.get("adset_id") or "")
        old_budget = pending.get("old_budget")
        new_budget = pending.get("new_budget")

        res = None
        info = None
        try:
            cents = int(float(new_budget) * 100)
            with allow_fb_api_calls(reason="ads_manage:budget"):
                obj = AdSet(adset_id)
                res, info = safe_api_call(obj.api_update, params={"daily_budget": cents}, _caller="ads_manage", _aid=aid, _return_error_info=True)
        except Exception as e:
            info = {"kind": "exception", "message": str(e)}

        ok = res is not None
        err_msg = "" if ok else _human_fb_error(info)

        _LOG.info(
            "caller=ads_manage action=budget aid=%s adset_id=%s old_value=%s new_value=%s result=%s error=%s",
            str(aid),
            str(adset_id),
            str(old_budget),
            str(new_budget),
            "ok" if ok else "fail",
            str(err_msg),
        )

        st.pop("pending", None)
        if ok:
            await q.message.reply_text("✅ Бюджет изменён")
            await _render_adsets(q, context, force=True)
            return

        await q.edit_message_text(f"⚠️ Ошибка: {err_msg}", reply_markup=_confirm_kb(), parse_mode=ParseMode.HTML)
        return

    await q.answer("Нет действия", show_alert=False)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not _can_access(update):
        return False

    q = update.callback_query
    if not q:
        return False

    data = str(q.data or "")
    if not data.startswith("am_"):
        return False

    await q.answer()

    st = _state(context)

    if data == "am_menu":
        await _render_accounts(q, context)
        return True

    if data.startswith("am_acc|"):
        aid = data.split("|", 1)[1]
        st.clear()
        st.update({"level": "campaigns", "aid": str(aid), "selected_id": ""})
        await _render_campaigns(q, context, force=True)
        return True

    if data.startswith("am_sel|"):
        oid = data.split("|", 1)[1]
        st["selected_id"] = str(oid)
        cur = str(st.get("level") or "")
        if cur == "campaigns":
            await _render_campaigns(q, context, force=False)
        elif cur == "adsets":
            await _render_adsets(q, context, force=False)
        else:
            await _render_ads(q, context, force=False)
        return True

    if data == "am_back":
        cur = str(st.get("level") or "")
        if cur == "adsets":
            st["level"] = "campaigns"
            st.pop("campaign_id", None)
            st.pop("adset_id", None)
            st["selected_id"] = ""
            await _render_campaigns(q, context, force=True)
            return True
        if cur == "ads":
            st["level"] = "adsets"
            st.pop("adset_id", None)
            st["selected_id"] = ""
            await _render_adsets(q, context, force=True)
            return True
        await _render_accounts(q, context)
        return True

    if data == "am_open":
        cur = str(st.get("level") or "")
        sel = str(st.get("selected_id") or "").strip()
        if not sel:
            await q.answer("Выбери объект", show_alert=False)
            return True
        if cur == "campaigns":
            st["level"] = "adsets"
            st["campaign_id"] = sel
            st["selected_id"] = ""
            await _render_adsets(q, context, force=True)
            return True
        if cur == "adsets":
            st["level"] = "ads"
            st["adset_id"] = sel
            st["selected_id"] = ""
            await _render_ads(q, context, force=True)
            return True
        await q.answer("Нижний уровень", show_alert=False)
        return True

    if data == "am_refresh":
        ok, wait_s = _refresh_allowed(context)
        if not ok:
            await q.answer(f"Подожди {wait_s}с", show_alert=False)
            return True
        cur = str(st.get("level") or "")
        if cur == "campaigns":
            await _render_campaigns(q, context, force=True)
        elif cur == "adsets":
            await _render_adsets(q, context, force=True)
        elif cur == "ads":
            await _render_ads(q, context, force=True)
        else:
            await _render_accounts(q, context)
        return True

    if data == "am_edit":
        await q.answer("⏳ В разработке", show_alert=True)
        return True

    if data == "am_toggle":
        await _start_toggle(q, context)
        return True

    if data == "am_budget":
        await _start_budget(q, context)
        return True

    if data == "am_confirm":
        await _apply_confirm(q, context)
        return True

    if data == "am_cancel":
        st.pop("pending", None)
        cur = str(st.get("level") or "")
        if cur == "campaigns":
            await _render_campaigns(q, context, force=False)
        elif cur == "adsets":
            await _render_adsets(q, context, force=False)
        elif cur == "ads":
            await _render_ads(q, context, force=False)
        else:
            await _render_accounts(q, context)
        return True

    return True
