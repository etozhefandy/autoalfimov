# fb_report/jobs.py

from datetime import datetime, timedelta
import asyncio
import re

from telegram.ext import ContextTypes, Application

from .constants import ALMATY_TZ, DEFAULT_REPORT_CHAT
from .storage import load_accounts, get_account_name
from .reporting import send_period_report, get_cached_report


def _yesterday_period():
    now = datetime.now(ALMATY_TZ)
    until = now - timedelta(days=1)
    since = until
    period = {
        "since": since.strftime("%Y-%m-%d"),
        "until": until.strftime("%Y-%m-%d"),
    }
    label = until.strftime("%d.%m.%Y")
    return period, label


async def full_daily_scan_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(DEFAULT_REPORT_CHAT)

    period, label = _yesterday_period()

    try:
        await send_period_report(context, chat_id, period, label)
    except Exception as e:
        await context.bot.send_message(
            chat_id,
            f"⚠️ full_daily_scan_job: ошибка скана: {e}",
        )


async def daily_report_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(DEFAULT_REPORT_CHAT)

    period, label = _yesterday_period()

    try:
        await send_period_report(context, chat_id, period, label)
    except Exception as e:
        await context.bot.send_message(
            chat_id,
            f"⚠️ daily_report_job: ошибка дневного отчёта: {e}",
        )


def _parse_totals_from_report_text(txt: str):
    messages = 0
    leads = 0
    spend = 0.0

    msg_pattern = re.compile(r"💬[^0-9]*?(\d+)")
    lead_pattern = re.compile(r"[📩♿️][^0-9]*?(\d+)")
    spend_pattern = re.compile(r"💵[^0-9]*?([0-9]+[.,]?[0-9]*)")

    for line in txt.splitlines():
        if "Итого" in line:
            m_msg = msg_pattern.search(line)
            if m_msg:
                try:
                    messages = int(m_msg.group(1))
                except Exception:
                    pass

            m_lead = lead_pattern.search(line)
            if m_lead:
                try:
                    leads = int(m_lead.group(1))
                except Exception:
                    pass

            m_spend = spend_pattern.search(line)
            if m_spend:
                try:
                    spend = float(m_spend.group(1).replace(",", "."))
                except Exception:
                    pass

    if messages == 0 and leads == 0:
        total_msg = 0
        total_leads = 0
        total_spend = 0.0
        for line in txt.splitlines():
            m_msg = msg_pattern.search(line)
            if m_msg:
                try:
                    total_msg += int(m_msg.group(1))
                except Exception:
                    pass

            m_lead = lead_pattern.search(line)
            if m_lead:
                try:
                    total_leads += int(m_lead.group(1))
                except Exception:
                    pass

            m_spend = spend_pattern.search(line)
            if m_spend:
                try:
                    total_spend += float(m_spend.group(1).replace(",", "."))
                except Exception:
                    pass

        messages = messages or total_msg
        leads = leads or total_leads
        spend = spend or total_spend

    total_convs = messages + leads
    cpa = None
    if total_convs > 0 and spend > 0:
        cpa = spend / total_convs

    return {
        "messages": messages,
        "leads": leads,
        "total_conversions": total_convs,
        "spend": spend,
        "cpa": cpa,
    }


async def _cpa_alerts_job(context: ContextTypes.DEFAULT_TYPE):
    accounts = load_accounts() or {}
    chat_id = str(DEFAULT_REPORT_CHAT)

    period, label = _yesterday_period()

    for aid, row in accounts.items():
        alerts = (row or {}).get("alerts") or {}
        target_cpl = float(alerts.get("target_cpl", 0.0) or 0.0)
        enabled = bool(alerts.get("enabled", False)) and target_cpl > 0

        if not enabled:
            continue

        try:
            txt = get_cached_report(aid, period, label)
        except Exception:
            txt = None

        if not txt:
            continue

        totals = _parse_totals_from_report_text(txt)

        total_convs = totals["total_conversions"]
        spend = totals["spend"]
        cpa = totals["cpa"]

        if not cpa or total_convs == 0 or spend == 0:
            continue

        if cpa <= target_cpl:
            continue

        acc_name = get_account_name(aid)

        header = f"⚠️ {acc_name} — Итого (💬+📩)"
        body_lines = [
            f"💵 Затраты: {spend:.2f} $",
            f"📊 Заявки (💬+📩): {total_convs}",
            f"🎯 Таргет CPA: {target_cpl:.2f} $",
            f"🧾 Причина: CPA {cpa:.2f}$ > таргета {target_cpl:.2f}$",
        ]
        body = "\n".join(body_lines)

        text = f"{header}\n{body}"

        try:
            await context.bot.send_message(chat_id, text)
            await asyncio.sleep(2.0)
        except Exception:
            continue


def schedule_cpa_alerts(app: Application):
    app.job_queue.run_repeating(
        _cpa_alerts_job,
        interval=timedelta(hours=1),
        first=timedelta(minutes=15),
    )
