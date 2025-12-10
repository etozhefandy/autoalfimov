# fb_report/jobs.py

from datetime import datetime, timedelta, time
import asyncio
import re
import json

from telegram.ext import ContextTypes, Application

from .constants import ALMATY_TZ, DEFAULT_REPORT_CHAT, ALLOWED_USER_IDS
from .storage import load_accounts, get_account_name
from .reporting import send_period_report, get_cached_report
from .adsets import fetch_adset_insights_7d

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
    from services.ai_focus import ask_deepseek
except Exception:  # noqa: BLE001
    fetch_insights = None  # type: ignore[assignment]

    def parse_insight(_ins: dict) -> dict:  # type: ignore[override]
        return {"msgs": 0, "leads": 0, "total": 0, "spend": 0.0}

    async def ask_deepseek(_messages, json_mode: bool = False):  # type: ignore[override]
        raise RuntimeError("DeepSeek is not available in this environment")


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


<<<<<<< HEAD
async def _cpa_alerts_job(context: ContextTypes.DEFAULT_TYPE):
=======
CPA_ALERT_TIMES = (
    # Временные слоты для режима "3 раза в день" (по Алмате)
    time(hour=11, minute=0, tzinfo=ALMATY_TZ),
    time(hour=15, minute=0, tzinfo=ALMATY_TZ),
    time(hour=19, minute=0, tzinfo=ALMATY_TZ),
)

CPA_HOURLY_START = 10
CPA_HOURLY_END = 22

WEEKDAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def _is_day_enabled(alerts: dict, now: datetime) -> bool:
    days = alerts.get("days") or []
    if not days:
        return False
    key = WEEKDAY_KEYS[now.weekday()]
    return key in days


def _resolve_account_cpa(alerts: dict) -> float:
    """Возвращает таргет CPA для аккаунта с приоритетом новой схемы.

    1) alerts["account_cpa"]
    2) alerts["target_cpl"] (старое поле)
    3) глобальный дефолт 3.0
    """

    acc_cpa = float(alerts.get("account_cpa", 0.0) or 0.0)
    if acc_cpa > 0:
        return acc_cpa
    old = float(alerts.get("target_cpl", 0.0) or 0.0)
    if old > 0:
        return old
    return 3.0


async def _cpa_alerts_job(context: ContextTypes.DEFAULT_TYPE):
    """CPA-алёрты по новой схеме частоты и дней недели.

    Уровни:
    - аккаунт: глобальный переключатель alerts["enabled"], дни и частота;
    - адсет: adset_alerts[adset_id] с приоритетным target_cpa.

    Поведение по умолчанию (обратная совместимость):
    - если adset_alerts пустой, используется только account_cpa как раньше.
    """

>>>>>>> fff35b0 (update)
    now = datetime.now(ALMATY_TZ)
    accounts = load_accounts() or {}

    # Алёрты шлём напрямую владельцу в личку (первый ID из ALLOWED_USER_IDS).
    # Если по какой-то причине список пуст, используем дефолтный чат как фолбэк.
    owner_id = None
    try:
        owner_id = next(iter(ALLOWED_USER_IDS))
    except StopIteration:
        owner_id = None

    chat_id = owner_id if owner_id is not None else str(DEFAULT_REPORT_CHAT)

    # Для алёрта берём текущее состояние за today,
    # чтобы видеть актуальный CPA на момент часа.
    period = "today"
    label = now.strftime("%d.%m.%Y")

    for aid, row in accounts.items():
        alerts = (row or {}).get("alerts") or {}
        if not isinstance(alerts, dict):
            alerts = {}

        # Глобальный флаг включения алёртов по аккаунту
        if not bool(alerts.get("enabled", False)):
            continue

        # Проверяем, включён ли текущий день недели
        if not _is_day_enabled(alerts, now):
            continue

        freq = alerts.get("freq", "3x")

        if freq == "3x":
            # Срабатываем только в определённые времена
            if now.replace(second=0, microsecond=0).timetz() not in [
                t.timetz() for t in CPA_ALERT_TIMES
            ]:
                continue
        elif freq == "hourly":
            # Каждый час в окне 10–22
            if not (CPA_HOURLY_START <= now.hour <= CPA_HOURLY_END):
                continue
        else:
            # Неизвестный режим частоты — пропускаем аккаунт
            continue

        # Таргет на уровне аккаунта остаётся как базовый.
        # ВАЖНО: даже если он <= 0, мы всё равно продолжаем обработку,
        # чтобы адсеты с собственным target_cpa могли работать независимо.
        account_target = _resolve_account_cpa(alerts)

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

        # ====== 1) Старый аккаунтный алёрт (оставляем как есть) ======

        if cpa and total_convs > 0 and spend > 0 and account_target > 0:
            acc_name = get_account_name(aid)

            effective_target_acc = account_target

            if cpa > effective_target_acc:
                header = f"⚠️ {acc_name} — Итого (💬+📩)"
                body_lines = [
                    f"💵 Затраты: {spend:.2f} $",
                    f"📊 Заявки (💬+📩): {total_convs}",
                    f"🎯 Таргет CPA: {effective_target_acc:.2f} $",
                    f"🧾 Причина: CPA {cpa:.2f}$ > таргета {effective_target_acc:.2f}$",
                ]
                body = "\n".join(body_lines)

                text = f"{header}\n{body}"

                try:
                    await context.bot.send_message(chat_id, text)
                    await asyncio.sleep(1.0)
                except Exception:
                    # Не прерываем обработку адсетов, даже если аккаунтный алёрт не ушёл
                    pass

        # ====== 2) Новый алёрт по адсетам ======

        # adset_alerts может быть пустым — тогда поведение полностью как раньше
        adset_alerts = alerts.get("adset_alerts", {}) or {}

        try:
            campaigns, _since, _until = fetch_adset_insights_7d(aid)
        except Exception:
            campaigns = []

        if not campaigns:
            continue

        acc_name = get_account_name(aid)

        problematic_lines: list[str] = []

        for camp in campaigns:
            for ad in camp.get("adsets", []) or []:
                adset_id = ad.get("id")
                if not adset_id:
                    continue

                cfg = (adset_alerts.get(adset_id) or {}) if adset_id in adset_alerts else {}
                adset_enabled = cfg.get("enabled", True)

                # Если адсет явно выключен — пропускаем его
                if not adset_enabled:
                    continue

                adset_target = float(cfg.get("target_cpa") or 0.0)
                # account_target уже посчитан выше через _resolve_account_cpa
                effective_target = adset_target if adset_target > 0 else account_target

                # Если эффективный таргет невалиден — для этого адсета CPA не считаем
                if effective_target <= 0:
                    continue

                ad_spend = float(ad.get("spend", 0.0) or 0.0)
                ad_total = int(ad.get("total", 0) or 0)
                ad_cpa = ad.get("cpa")
                if ad_cpa is None and ad_total > 0 and ad_spend > 0:
                    ad_cpa = ad_spend / ad_total

                if not ad_cpa or ad_total <= 0 or ad_spend <= 0:
                    continue

                if ad_cpa <= effective_target:
                    continue

                # Проблемный адсет — считаем перерасход
                try:
                    overspend_pct = (ad_cpa / effective_target - 1.0) * 100.0
                except ZeroDivisionError:
                    overspend_pct = 0.0

                ad_name = ad.get("name") or adset_id

                problematic_lines.append(
                    "\n".join(
                        [
                            f"{ad_name}",
                            f"• CPA: {ad_cpa:.2f} $",
                            f"• Target: {effective_target:.2f} $",
                            f"• Перерасход: +{overspend_pct:.0f}%",
                        ]
                    )
                )

        if not problematic_lines:
            continue

        header_adsets = f"⚠️ CPA-алёрты по адсетам для {acc_name}"
        text_adsets = header_adsets + "\n\n" + "\n\n".join(problematic_lines)

        # Пытаемся добавить короткий комментарий Фокус-ИИ (DeepSeek),
        # если включён ai_enabled для аккаунта.
        if alerts.get("ai_enabled", True):
            focus_comment = None
            try:
                data_for_analysis = {
                    "account_id": aid,
                    "account_name": acc_name,
                    "date": label,
                    "spend": spend,
                    "total_conversions": total_convs,
                    "cpa": cpa,
                    "target_cpa": target_cpl,
                }

                system_msg = (
                    "Ты — продвинутый аналитик (Focus-ИИ) для CPA-алёртов. "
                    "Отвечай ТОЛЬКО на русском языке. "
                    "Тебе даны затраты, количество заявок и фактический CPA относительно таргет CPA. "
                    "Кратко оцени ситуацию и предложи одно-два действия: оставить бюджет, мягко повысить/понизить бюджет (10–30%), либо проверить креативы/аудитории. "
                    "Отвечай очень кратко (1–2 предложения) в виде обычного текста, без JSON."
                )

                user_msg = json.dumps(data_for_analysis, ensure_ascii=False)

                ds_resp = await ask_deepseek(
                    [
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    json_mode=False,
                )

                choice = (ds_resp.get("choices") or [{}])[0]
                focus_comment = (choice.get("message") or {}).get("content")
            except Exception:
                focus_comment = None

            if focus_comment:
                text = f"{text}\n\n🤖 Комментарий Фокус-ИИ:\n{focus_comment.strip()}"

        try:
            await context.bot.send_message(chat_id, text_adsets)
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
<<<<<<< HEAD
    # CPA-алёрты с комментариями Фокус-ИИ три раза в день: 10:00, 13:00, 18:00.
    for hh in (10, 13, 18):
        app.job_queue.run_daily(
            _cpa_alerts_job,
            time=time(hour=hh, minute=0, tzinfo=ALMATY_TZ),
        )
=======
    # Планировщик CPA-алёртов: единый повторяющийся джоб раз в час.
    # Внутри _cpa_alerts_job уже учитывает days/freq и решает,
    # нужно ли слать уведомления в этот час.
    app.job_queue.run_repeating(
        _cpa_alerts_job,
        interval=timedelta(hours=1),
        first=timedelta(minutes=15),
    )
>>>>>>> fff35b0 (update)

    # Часовой снимок инсайтов за today для часового кэша
    app.job_queue.run_repeating(
        _hourly_snapshot_job,
        interval=timedelta(hours=1),
        first=timedelta(minutes=5),
    )
