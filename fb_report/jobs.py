# fb_report/jobs.py
from datetime import datetime, timedelta

from telegram.ext import ContextTypes, Application

from .constants import ALMATY_TZ, DEFAULT_REPORT_CHAT
from .storage import iter_enabled_accounts_only, load_accounts, get_account_name
from .insights import _blend_totals, load_local_insights, save_local_insights
from .reporting import fetch_insight, period_key, send_period_report

# --- history_store: мягкий импорт, чтобы бот не падал, если файла нет ---
try:
    from history_store import append_snapshot, prune_old_history

    HISTORY_STORE_AVAILABLE = True
except ImportError:
    HISTORY_STORE_AVAILABLE = False

    def append_snapshot(*args, **kwargs):
        return

    def prune_old_history(*args, **kwargs):
        return


async def cpa_alerts_job(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Джоб для логирования истории и CPA-алертов.
    История пишется 24/7, алерты шлём только 10–22.
    """
    chat_id = "253181449"  # твой юзер-id
    now = datetime.now(ALMATY_TZ)

    store = load_accounts()

    from .reporting import fetch_insight  # чтобы не ловить циклы

    for aid in iter_enabled_accounts_only():
        row = store.get(aid, {})
        alerts = row.get("alerts", {}) or {}
        target = float(alerts.get("target_cpl", 0.0) or 0.0)

        # 1) Всегда логируем историю, если есть данные
        try:
            _, ins = fetch_insight(aid, "today")
        except Exception:
            ins = None

        if ins and HISTORY_STORE_AVAILABLE:
            spend, msgs, leads, total, blended = _blend_totals(ins)
            append_snapshot(aid, spend=spend, msgs=msgs, leads=leads, ts=now)

        # 2) Раз в сутки чистим историю старше 12 месяцев
        if now.hour == 3 and HISTORY_STORE_AVAILABLE:
            prune_old_history(max_age_days=365)

        # 3) Всё, что ниже — только для алертов (10–22)
        if not (10 <= now.hour <= 22):
            continue

        if not alerts.get("enabled") or target <= 0:
            continue

        mflags = row.get("metrics", {}) or {}
        use_msg = bool(mflags.get("messaging", False))
        use_lead = bool(mflags.get("leads", False))
        if not (use_msg or use_lead):
            continue

        if not ins:
            continue

        spend, msgs, leads, total, blended = _blend_totals(ins)

        if use_msg and not use_lead:
            conv = msgs
            cpa = (spend / conv) if conv > 0 else None
            label = "Переписки"
        elif use_lead and not use_msg:
            conv = leads
            cpa = (spend / conv) if conv > 0 else None
            label = "Лиды"
        else:
            conv = total
            cpa = blended
            label = "Итого (💬+📩)"

        should_alert = False
        reason = ""
        if spend > 0 and conv == 0:
            should_alert = True
            reason = f"есть траты {spend:.2f}$, но 0 конверсий"
        elif cpa is not None and cpa > target:
            should_alert = True
            reason = f"CPA {cpa:.2f}$ > таргета {target:.2f}$"

        if should_alert:
            txt = (
                f"⚠️ <b>{get_account_name(aid)}</b> — {label}\n"
                f"💵 Затраты: {spend:.2f} $\n"
                f"📊 Конверсии: {conv}\n"
                f"🎯 Таргет CPA: {target:.2f} $\n"
                f"🧾 Причина: {reason}"
            )
            await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")


async def full_daily_scan_job(ctx: ContextTypes.DEFAULT_TYPE):
    """
    1 раз в день — собирает инсайты по всем включённым аккаунтам и
    сохраняет их в локальное хранилище.
    """
    now = datetime.now(ALMATY_TZ)

    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    periods = {
        "today": "today",
        "yesterday": "yesterday",
        "week": {
            "since": (now - timedelta(days=7)).strftime("%Y-%m-%d"),
            "until": yesterday,
        },
    }

    for aid in iter_enabled_accounts_only():
        store = load_local_insights(aid)
        for _, period in periods.items():
            key = period_key(period)
            if key in store:
                continue
            try:
                _, ins = fetch_insight(aid, period)
                store[key] = ins
            except Exception as e:
                print(f"[daily_scan] error for {aid}: {e}")
        save_local_insights(aid, store)

    print("[daily_scan] full daily scan completed")


async def daily_report_job(ctx: ContextTypes.DEFAULT_TYPE):
    if not DEFAULT_REPORT_CHAT:
        return
    from .reporting import send_period_report

    label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
    await send_period_report(ctx, str(DEFAULT_REPORT_CHAT), "yesterday", label)


def schedule_cpa_alerts(app: Application):
    """
    Запускаем cpa_alerts_job каждый час (24/7).
    """
    app.job_queue.run_repeating(
        cpa_alerts_job,
        interval=3600,
        first=0,
        name="cpa_alerts_job",
    )
