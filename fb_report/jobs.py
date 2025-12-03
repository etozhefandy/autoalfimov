# fb_report/jobs.py

from datetime import datetime, timedelta
import asyncio
import re

from telegram.ext import ContextTypes, Application

from .constants import ALMATY_TZ, DEFAULT_REPORT_CHAT, ALLOWED_USER_IDS
from .storage import load_accounts, get_account_name
from .reporting import send_period_report, get_cached_report

# Для Railway могут быть разные пути импорта services.*.
# Пытаемся взять реальные функции, а при ошибке делаем мягкие заглушки,
# чтобы бот не падал при старте.
try:  # pragma: no cover - защитный импорт для продакшена
    from services.storage import load_hourly_stats, save_hourly_stats
except Exception:  # noqa: BLE001 - нам важен ЛЮБОЙ ImportError/RuntimeError
    def load_hourly_stats() -> dict:  # type: ignore[override]
        return {}

    def save_hourly_stats(_stats: dict) -> None:  # type: ignore[override]
        return None

try:  # pragma: no cover
    from services.facebook_api import fetch_insights
    from services.analytics import parse_insight
except Exception:  # noqa: BLE001
    fetch_insights = None  # type: ignore[assignment]

    def parse_insight(_ins: dict) -> dict:  # type: ignore[override]
        return {"msgs": 0, "leads": 0, "total": 0, "spend": 0.0}


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
    """
    Парсим ОДИН текстовый отчёт по аккаунту и вытаскиваем:
    - messages (✉️ / 💬)
    - leads (📩 / ♿️)
    - total_conversions (из строки 'Итого: N заявок', если есть)
    - spend (💵)
    """
    total_messages = 0
    total_leads = 0
    spend = 0.0
    total_from_line = None

    msg_pattern = re.compile(r"(?:💬|✉️)[^0-9]*?(\d+)")
    lead_pattern = re.compile(r"(?:📩|♿️)[^0-9]*?(\d+)")
    spend_pattern = re.compile(r"💵[^0-9]*?([0-9]+[.,]?[0-9]*)")
    total_pattern = re.compile(r"Итого:\s*([0-9]+)\s+заяв", re.IGNORECASE)

    for line in txt.splitlines():
        m_msg = msg_pattern.search(line)
        if m_msg:
            try:
                total_messages += int(m_msg.group(1))
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
                spend = float(m_spend.group(1).replace(",", "."))
            except Exception:
                pass

        m_total = total_pattern.search(line)
        if m_total:
            try:
                total_from_line = int(m_total.group(1))
            except Exception:
                pass

    total_convs = total_messages + total_leads

    if total_from_line is not None and total_from_line > 0:
        total_convs = total_from_line

    cpa = None
    if total_convs > 0 and spend > 0:
        cpa = spend / total_convs

    return {
        "messages": total_messages,
        "leads": total_leads,
        "total_conversions": total_convs,
        "spend": spend,
        "cpa": cpa,
    }


async def _cpa_alerts_job(context: ContextTypes.DEFAULT_TYPE):
    # Ограничиваем отправку алёртов часовым окном, как в старом боте:
    # каждый час с 10:00 до 22:00 по Алмате.
    now = datetime.now(ALMATY_TZ)
    if not (10 <= now.hour <= 22):
        return

    accounts = load_accounts() or {}
    # Алёрты шлём напрямую владельцу в личку (первый ID из ALLOWED_USER_IDS).
    # Если по какой-то причине список пуст, используем дефолтный чат как фолбэк.
    owner_id = None
    try:
        owner_id = next(iter(ALLOWED_USER_IDS))
    except StopIteration:
        owner_id = None

    chat_id = owner_id if owner_id is not None else str(DEFAULT_REPORT_CHAT)

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
            await asyncio.sleep(1.0)
        except Exception:
            continue


async def _hourly_snapshot_job(context: ContextTypes.DEFAULT_TYPE):
    """Раз в час снимаем инсайты за today и сохраняем дельту в hour buckets.

    - один запрос fetch_insights(aid, "today") на аккаунт;
    - дельта по messages/leads/total/spend пишется в hourly_stats.json;
    - храним историю ~2 года по дням и часам.
    """
    now = datetime.now(ALMATY_TZ)
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")

    accounts = load_accounts() or {}
    stats = load_hourly_stats() or {}
    acc_section = stats.setdefault("_acc", {})

    # Порог хранения ~2 года
    cutoff_date = (now - timedelta(days=730)).strftime("%Y-%m-%d")

    for aid, row in accounts.items():
        if not (row or {}).get("enabled", True):
            continue

        # Инсайты за today — всегда живые, без кэша (см. fetch_insights).
        try:
            ins = fetch_insights(aid, "today") or {}
        except Exception:
            continue

        metrics = parse_insight(ins)

        cur_msgs = int(metrics.get("msgs", 0) or 0)
        cur_leads = int(metrics.get("leads", 0) or 0)
        cur_total = int(metrics.get("total", 0) or 0)
        cur_spend = float(metrics.get("spend", 0.0) or 0.0)

        prev = acc_section.get(aid, {"msgs": 0, "leads": 0, "total": 0, "spend": 0.0})

        d_msgs = max(0, cur_msgs - int(prev.get("msgs", 0) or 0))
        d_leads = max(0, cur_leads - int(prev.get("leads", 0) or 0))
        d_total = max(0, cur_total - int(prev.get("total", 0) or 0))
        d_spend = max(0.0, cur_spend - float(prev.get("spend", 0.0) or 0.0))

        if any([d_msgs, d_leads, d_total, d_spend]):
            acc_stats = stats.setdefault(aid, {})
            day_stats = acc_stats.setdefault(date_str, {})
            hour_bucket = day_stats.setdefault(
                hour_str,
                {"messages": 0, "leads": 0, "total": 0, "spend": 0.0},
            )

            hour_bucket["messages"] += d_msgs
            hour_bucket["leads"] += d_leads
            hour_bucket["total"] += d_total
            hour_bucket["spend"] += d_spend

        # Обновляем аккумулятор для следующего часа
        acc_section[aid] = {
            "msgs": cur_msgs,
            "leads": cur_leads,
            "total": cur_total,
            "spend": cur_spend,
        }

    # Обрезаем историю старше cutoff_date
    for aid, acc_stats in list(stats.items()):
        if aid == "_acc":
            continue
        if not isinstance(acc_stats, dict):
            continue
        for d in list(acc_stats.keys()):
            if d < cutoff_date:
                del acc_stats[d]

    save_hourly_stats(stats)


def schedule_cpa_alerts(app: Application):
    # Часовые CPA-алёрты (по вчерашнему периоду через текстовые отчёты)
    app.job_queue.run_repeating(
        _cpa_alerts_job,
        interval=timedelta(hours=1),
        first=timedelta(minutes=15),
    )

    # Часовой снимок инсайтов за today для часового кэша
    app.job_queue.run_repeating(
        _hourly_snapshot_job,
        interval=timedelta(hours=1),
        first=timedelta(minutes=5),
    )
