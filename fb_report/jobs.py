# fb_report/jobs.py

from datetime import datetime, timedelta, time
import asyncio
import re
import json

from telegram import InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes, Application

from .constants import ALMATY_TZ, DEFAULT_REPORT_CHAT, ALLOWED_USER_IDS
from .storage import load_accounts, get_account_name
from .reporting import send_period_report, get_cached_report, build_account_report
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
    from services.analytics import (
        parse_insight,
        analyze_account,
        analyze_campaigns,
        analyze_adsets,
        analyze_ads,
    )
    from services.ai_focus import ask_deepseek
except Exception:  # noqa: BLE001
    fetch_insights = None  # type: ignore[assignment]

    def parse_insight(_ins: dict) -> dict:  # type: ignore[override]
        return {"msgs": 0, "leads": 0, "total": 0, "spend": 0.0}

    def analyze_account(_aid: str, days: int = 7, period=None):  # type: ignore[override]
        return {"aid": _aid, "metrics": None}

    def analyze_campaigns(_aid: str, days: int = 7, period=None):  # type: ignore[override]
        return []

    def analyze_adsets(_aid: str, days: int = 7, period=None):  # type: ignore[override]
        return []

    def analyze_ads(_aid: str, days: int = 7, period=None):  # type: ignore[override]
        return []

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
    """Утренний отчёт (🌅): вчера vs позавчера по уровням.

    Настройки берутся из row["morning_report"]["level"], где level один из
    OFF / ACCOUNT / CAMPAIGN / ADSET:

    - OFF      — отчёт не отправляется;
    - ACCOUNT  — только итоговый блок по аккаунту;
    - CAMPAIGN — аккаунт + проблемные кампании;
    - ADSET    — аккаунт + проблемные кампании + проблемные адсеты.

    Пороги ухудшения фиксированы:
    - 🔴 CPA вырос ≥25% или лиды упали ≥25%;
    - 🟡 CPA вырос ≥10% или лиды упали ≥10%;
    - иначе 🟢.
    """

    chat_id = str(DEFAULT_REPORT_CHAT)

    now = datetime.now(ALMATY_TZ).date()
    yday = now - timedelta(days=1)

    period_yday = {
        "since": yday.strftime("%Y-%m-%d"),
        "until": yday.strftime("%Y-%m-%d"),
    }

    store = load_accounts() or {}

    for aid, row in store.items():
        if not (row or {}).get("enabled", True):
            continue

        mr = (row or {}).get("morning_report") or {}
        level = str(mr.get("level", "ACCOUNT")).upper()

        if level == "OFF":
            continue

        label = yday.strftime("%d.%m.%Y")
        body = build_account_report(aid, period_yday, level, label=label)
        if not body:
            continue

        try:
            await context.bot.send_message(chat_id, body, parse_mode="HTML")
            await asyncio.sleep(0.5)
        except Exception:
            # Утренний отчёт не должен ломать остальные джобы.
            continue


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
    period_dict = {
        "since": now.strftime("%Y-%m-%d"),
        "until": now.strftime("%Y-%m-%d"),
    }

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
            # Срабатываем только в определённые времена.
            # Округляем текущее время до минуты и сравниваем с допустимыми слотами.
            current_time = now.replace(second=0, microsecond=0).time()
            if current_time not in CPA_ALERT_TIMES:
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
        # чтобы объекты с собственным target_cpa могли работать независимо.
        account_target = _resolve_account_cpa(alerts)

        campaign_alerts = alerts.get("campaign_alerts", {}) or {}
        adset_alerts = alerts.get("adset_alerts", {}) or {}
        ad_alerts = alerts.get("ad_alerts", {}) or {}

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

        # ====== 2) Новый алёрт по кампаниям ======

        acc_name = get_account_name(aid)

        # Сохраняем метрики кампаний в словарь, чтобы переиспользовать для
        # сообщений по объявлениям (CPA кампании и её таргет).
        campaign_stats: dict[str, dict] = {}
        problematic_campaign_lines: list[str] = []

        try:
            camp_metrics = analyze_campaigns(aid, period=period_dict) or []
        except Exception:
            camp_metrics = []

        for camp in camp_metrics:
            cid = camp.get("campaign_id")
            if not cid:
                continue

            cfg_c = (campaign_alerts.get(cid) or {}) if cid in campaign_alerts else {}
            enabled_c = cfg_c.get("enabled", True)
            if not enabled_c:
                continue

            camp_target = float(cfg_c.get("target_cpa") or 0.0)
            effective_target_c = camp_target if camp_target > 0 else account_target
            if effective_target_c <= 0:
                continue

            c_spend = float(camp.get("spend", 0.0) or 0.0)
            c_total = int(camp.get("total", 0) or 0)
            c_cpa = camp.get("cpa")
            if not c_cpa or c_spend <= 0 or c_total <= 0:
                continue

            # Сохраняем статистику кампании для последующего использования в
            # мультимесседж-формате по объявлениям.
            cname = camp.get("name") or cid
            campaign_stats[str(cid)] = {
                "name": cname,
                "cpa": float(c_cpa),
                "target": float(effective_target_c),
            }

            if c_cpa <= effective_target_c:
                continue

            try:
                overspend_pct_c = (c_cpa / effective_target_c - 1.0) * 100.0
            except ZeroDivisionError:
                overspend_pct_c = 0.0

            problematic_campaign_lines.append(
                "\n".join(
                    [
                        f"{cname}",
                        f"• CPA: {c_cpa:.2f} $",
                        f"• Target: {effective_target_c:.2f} $",
                        f"• Перерасход: +{overspend_pct_c:.0f}%",
                    ]
                )
            )

        if problematic_campaign_lines:
            header_camps = f"⚠️ CPA-алёрты по кампаниям для {acc_name}"
            text_camps = header_camps + "\n\n" + "\n\n".join(problematic_campaign_lines)
            try:
                await context.bot.send_message(chat_id, text_camps)
                await asyncio.sleep(1.0)
            except Exception:
                pass

        # ====== 3) Новый алёрт по адсетам ======

        problematic_adset_lines: list[str] = []
        # Статистика по адсетам (для сообщений по объявлениям)
        adset_stats: dict[str, dict] = {}

        try:
            campaigns, _since, _until = fetch_adset_insights_7d(aid)
        except Exception:
            campaigns = []

        for camp in campaigns:
            for ad in camp.get("adsets", []) or []:
                adset_id = ad.get("id")
                if not adset_id:
                    continue

                cid = ad.get("campaign_id")

                cfg_a = (adset_alerts.get(adset_id) or {}) if adset_id in adset_alerts else {}
                enabled_a = cfg_a.get("enabled", True)
                if not enabled_a:
                    continue

                adset_target = float(cfg_a.get("target_cpa") or 0.0)

                # Приоритет: adset → campaign → account
                camp_target = 0.0
                if cid and cid in campaign_alerts:
                    camp_target = float((campaign_alerts.get(cid) or {}).get("target_cpa") or 0.0)

                effective_target_a = (
                    adset_target
                    if adset_target > 0
                    else camp_target
                    if camp_target > 0
                    else account_target
                )

                if effective_target_a <= 0:
                    continue

                ad_spend = float(ad.get("spend", 0.0) or 0.0)
                ad_total = int(ad.get("total", 0) or 0)
                ad_cpa = ad.get("cpa")
                if ad_cpa is None and ad_total > 0 and ad_spend > 0:
                    ad_cpa = ad_spend / ad_total

                if not ad_cpa or ad_total <= 0 or ad_spend <= 0:
                    continue

                # Сохраняем статистику адсета для последующего использования
                # в мультимесседж-формате по объявлениям.
                adset_name = ad.get("name") or adset_id
                adset_stats[str(adset_id)] = {
                    "name": adset_name,
                    "cpa": float(ad_cpa),
                    "target": float(effective_target_a),
                }

                if ad_cpa <= effective_target_a:
                    continue

                try:
                    overspend_pct_a = (ad_cpa / effective_target_a - 1.0) * 100.0
                except ZeroDivisionError:
                    overspend_pct_a = 0.0

                problematic_adset_lines.append(
                    "\n".join(
                        [
                            f"{adset_name}",
                            f"• CPA: {ad_cpa:.2f} $",
                            f"• Target: {effective_target_a:.2f} $",
                            f"• Перерасход: +{overspend_pct_a:.0f}%",
                        ]
                    )
                )

        if problematic_adset_lines:
            header_adsets = f"⚠️ CPA-алёрты по адсетам для {acc_name}"
            text_adsets = header_adsets + "\n\n" + "\n\n".join(problematic_adset_lines)
            try:
                await context.bot.send_message(chat_id, text_adsets)
                await asyncio.sleep(1.0)
            except Exception:
                pass

        # ====== 4) Новый алёрт по объявлениям ======

        # a) Загружаем метрики по объявлениям за today (для CPA и таргетов)
        try:
            ad_metrics_today = analyze_ads(aid, period=period_dict) or []
        except Exception:
            ad_metrics_today = []

        # b) Отдельно считаем альтернативы за последние 7 дней
        try:
            period_7d = {
                "since": (now - timedelta(days=7)).strftime("%Y-%m-%d"),
                "until": now.strftime("%Y-%m-%d"),
            }
            ad_metrics_7d = analyze_ads(aid, period=period_7d) or []
        except Exception:
            ad_metrics_7d = []

        ads_by_adset_7d: dict[str, list[dict]] = {}
        for ad7 in ad_metrics_7d:
            ad_id7 = ad7.get("ad_id")
            if not ad_id7:
                continue
            adset_id7 = ad7.get("adset_id") or ""
            if not adset_id7:
                continue
            a_spend7 = float(ad7.get("spend", 0.0) or 0.0)
            if a_spend7 <= 0:
                continue
            bucket7 = ads_by_adset_7d.setdefault(str(adset_id7), [])
            bucket7.append(ad7)

        # c) Группируем ПРОБЛЕМНЫЕ объявления по кампании/адсету
        problems_by_campaign: dict[str, dict] = {}

        for ad in ad_metrics_today:
            ad_id = ad.get("ad_id")
            if not ad_id:
                continue

            cfg_ad = (ad_alerts.get(ad_id) or {}) if ad_id in ad_alerts else {}
            enabled_ad = cfg_ad.get("enabled", True)
            silent_ad = cfg_ad.get("silent", False)

            if not enabled_ad:
                continue

            ad_target = float(cfg_ad.get("target_cpa") or 0.0)

            # Иерархия: ad → adset → campaign → account
            adset_id = ad.get("adset_id")
            camp_id = ad.get("campaign_id")

            adset_target2 = 0.0
            if adset_id and adset_id in adset_alerts:
                adset_target2 = float((adset_alerts.get(adset_id) or {}).get("target_cpa") or 0.0)

            camp_target2 = 0.0
            if camp_id and camp_id in campaign_alerts:
                camp_target2 = float((campaign_alerts.get(camp_id) or {}).get("target_cpa") or 0.0)

            effective_target_ad = (
                ad_target
                if ad_target > 0
                else adset_target2
                if adset_target2 > 0
                else camp_target2
                if camp_target2 > 0
                else account_target
            )

            if effective_target_ad <= 0:
                continue

            a_spend = float(ad.get("spend", 0.0) or 0.0)
            a_total = int(ad.get("total", 0) or 0)
            a_cpa = ad.get("cpa")
            if not a_cpa or a_spend <= 0 or a_total <= 0:
                continue

            if a_cpa <= effective_target_ad:
                continue

            # Есть ли альтернативы внутри того же адсета за последние 7 дней
            has_alternative = False
            if adset_id:
                all_in_adset7 = ads_by_adset_7d.get(str(adset_id)) or []
                for other in all_in_adset7:
                    if other.get("ad_id") == ad_id:
                        continue
                    if float(other.get("spend", 0.0) or 0.0) > 0:
                        has_alternative = True
                        break

            # Если объявление в тихом режиме — считаем CPA, но не добавляем в список
            if silent_ad:
                continue

            ad_name = ad.get("name") or ad_id
            adset_name = ad.get("adset_name") or adset_id or "?"
            camp_name = ad.get("campaign_name") or camp_id or "?"

            camp_key = str(camp_id or "?")
            camp_entry = problems_by_campaign.setdefault(
                camp_key,
                {"name": camp_name, "adsets": {}},
            )

            adset_key = str(adset_id or "?")
            adsets_map = camp_entry["adsets"]
            adset_entry = adsets_map.setdefault(
                adset_key,
                {"name": adset_name, "ads": []},
            )

            adset_entry["ads"].append(
                {
                    "ad_id": ad_id,
                    "ad_name": ad_name,
                    "cpa": float(a_cpa),
                    "target": float(effective_target_ad),
                    "has_alternative_in_adset": bool(has_alternative),
                }
            )

        # d) Мультимесседж-формат: Кампания → Адсет → Объявления
        # Не шлём кампанию/адсет, если внутри нет проблемных объявлений
        for camp_key in sorted(problems_by_campaign.keys()):
            camp_entry = problems_by_campaign[camp_key]
            adsets_map = camp_entry.get("adsets") or {}

            # Считаем общее количество проблемных объявлений в кампании
            total_ads_in_camp = sum(
                len(adset_entry.get("ads") or []) for adset_entry in adsets_map.values()
            )
            if total_ads_in_camp <= 0:
                continue

            # Сообщение по кампании
            camp_stat = campaign_stats.get(camp_key) or {}
            camp_cpa_val = camp_stat.get("cpa")
            camp_tgt_val = camp_stat.get("target")
            camp_cpa_str = f"{camp_cpa_val:.2f}$" if camp_cpa_val is not None else "н/д"
            camp_tgt_str = f"{camp_tgt_val:.2f}$" if camp_tgt_val is not None else "н/д"

            cname = camp_entry.get("name") or camp_key
            camp_lines = [
                f"🟩 Кампания: {cname}",
                f"CPA кампании: {camp_cpa_str} (таргет: {camp_tgt_str})",
                "⚠️ Проблемные элементы внутри → см. сообщения ниже",
            ]
            try:
                await context.bot.send_message(chat_id, "\n".join(camp_lines))
                await asyncio.sleep(0.3)
            except Exception:
                pass

            # Сообщения по адсетам и объявлениям
            for adset_key in sorted(adsets_map.keys()):
                adset_entry = adsets_map[adset_key]
                ads_list = adset_entry.get("ads") or []
                if not ads_list:
                    continue

                as_name = adset_entry.get("name") or adset_key
                adset_stat = adset_stats.get(adset_key) or {}
                adset_cpa_val = adset_stat.get("cpa")
                adset_tgt_val = adset_stat.get("target")
                adset_cpa_str = (
                    f"{adset_cpa_val:.2f}$" if adset_cpa_val is not None else "н/д"
                )
                adset_tgt_str = (
                    f"{adset_tgt_val:.2f}$" if adset_tgt_val is not None else "н/д"
                )

                adset_lines = [
                    f"🟦 Адсет: {as_name}",
                    f"CPA адсета: {adset_cpa_str} (таргет: {adset_tgt_str})",
                    "⚠️ Внутри объявления, которые превышают CPA → следующие сообщения",
                ]
                try:
                    await context.bot.send_message(chat_id, "\n".join(adset_lines))
                    await asyncio.sleep(0.3)
                except Exception:
                    pass

                # Для каждого объявления — отдельное сообщение с собственными кнопками
                for ad_info in ads_list:
                    ad_id = ad_info.get("ad_id")
                    ad_name_txt = ad_info.get("ad_name") or ad_id
                    cpa_val = float(ad_info.get("cpa", 0.0) or 0.0)
                    tgt_val = float(ad_info.get("target", 0.0) or 0.0)
                    has_alt_flag = bool(ad_info.get("has_alternative_in_adset"))

                    alt_str = "да" if has_alt_flag else "нет"

                    ad_lines = [
                        f"🟨 Объявление: {ad_name_txt}",
                        "",
                        f"CPA креатива: {cpa_val:.2f} $",
                        f"Таргет: {tgt_val:.2f} $",
                        f"Перерасход: +{max(0.0, (cpa_val / tgt_val - 1.0) * 100.0):.0f}%"
                        if tgt_val > 0
                        else "Перерасход: н/д",
                        f"Есть альтернативы в адсете: {alt_str}",
                    ]

                    kb_row: list[InlineKeyboardButton] = []
                    if has_alt_flag and ad_id:
                        kb_row.append(
                            InlineKeyboardButton(
                                "Выключить",
                                callback_data=f"cpa_ad_off|{aid}|{ad_id}",
                            )
                        )
                    if ad_id:
                        kb_row.append(
                            InlineKeyboardButton(
                                "Тихий режим",
                                callback_data=f"cpa_ad_silent|{aid}|{ad_id}",
                            )
                        )

                    try:
                        await context.bot.send_message(
                            chat_id,
                            "\n".join(ad_lines),
                            reply_markup=InlineKeyboardMarkup([kb_row]) if kb_row else None,
                        )
                        await asyncio.sleep(0.3)
                    except Exception:
                        pass

        # ====== 5) Пытаемся добавить комментарий Фокус-ИИ (DeepSeek) ======

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
            except Exception as e:
                focus_comment = (
                    "Фокус-ИИ сейчас недоступен для этого CPA-алёрта "
                    f"(ошибка {type(e).__name__}). Оцени ситуацию по цифрам выше."
                )

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
    # Планировщик CPA-алёртов: единый повторяющийся джоб раз в час.
    # Внутри _cpa_alerts_job уже учитывает days/freq и решает,
    # нужно ли слать уведомления в этот час.
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
