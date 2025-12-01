# fb_report/insights.py
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta

from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount

ALMATY_TZ = timezone("Asia/Almaty")

# ========= ПУТИ / ХРАНИЛИЩЕ ИНСАЙТОВ =========

DATA_DIR = os.getenv("DATA_DIR", "/data")
INSIGHTS_DIR = os.path.join(DATA_DIR, "insights")
os.makedirs(INSIGHTS_DIR, exist_ok=True)


def _insights_path(aid: str) -> str:
    safe = aid.replace(":", "_").replace("/", "_")
    return os.path.join(INSIGHTS_DIR, f"{safe}.json")


def _atomic_write_json(path: str, obj: dict):
    tmp = f"{path}.tmp"
    bak = f"{path}.bak"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    try:
        if os.path.exists(path):
            os.replace(path, bak)
    except Exception:
        pass
    os.replace(tmp, path)


# ========= ПУБЛИЧНЫЕ ХЕЛПЕРЫ ДЛЯ ЛОКАЛЬНОГО КЭША =========

def load_local_insights(aid: str) -> dict:
    """
    Загружает словарь инсайтов по аккаунту:
    { period_key -> dict | None }
    """
    path = _insights_path(aid)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_local_insights(aid: str, data: dict):
    """
    Сохраняет словарь инсайтов по аккаунту.
    """
    path = _insights_path(aid)
    _atomic_write_json(path, data)


# ========= ОБРАБОТКА ACTIONS И СВОДНЫХ МЕТРИК =========

def extract_actions(row: dict) -> dict:
    """
    Преобразует поле actions из ответа Facebook в dict:
    { action_type -> value } с суммированием по типам.
    """
    res: dict[str, float] = {}
    actions = row.get("actions") or []
    for a in actions:
        at = a.get("action_type")
        if not at:
            continue
        try:
            v = float(a.get("value", 0) or 0)
        except Exception:
            v = 0.0
        res[at] = res.get(at, 0.0) + v
    return res


def _extract_leads(acts: dict) -> int:
    """
    Унифицированное извлечение лидов:
    сначала пытаемся взять Website Submit Applications,
    если нет — падаем на пиксельные/lead-события.
    """
    keys = [
        "Website Submit Applications",
        "offsite_conversion.fb_pixel_submit_application",
        "offsite_conversion.fb_pixel_lead",
        "lead",
    ]
    for k in keys:
        v = acts.get(k)
        if v:
            try:
                return int(v)
            except Exception:
                return 0
    return 0


def _blend_totals(row: dict):
    """
    Единый расчёт итогов для аккаунта/кампании/адсета.

    Возвращает кортеж:
    (spend, msgs, leads, total, blended_cpa_or_None)
    """
    try:
        spend = float(row.get("spend", 0) or 0)
    except Exception:
        spend = 0.0

    acts = extract_actions(row)

    msgs = int(
        acts.get("onsite_conversion.messaging_conversation_started_7d", 0) or 0
    )
    leads = _extract_leads(acts)

    total = msgs + leads
    blended = (spend / total) if total > 0 else None

    return spend, msgs, leads, total, blended


# ========= ТЕПЛОВАЯ КАРТА АДСЕТОВ =========

def _date_range_for_mode(mode: str):
    now = datetime.now(ALMATY_TZ).date()
    if mode == "7":
        until = now - timedelta(days=1)
        since = until - timedelta(days=6)
    elif mode == "14":
        until = now - timedelta(days=1)
        since = until - timedelta(days=13)
    else:  # "month" — текущий месяц до вчера
        until = now - timedelta(days=1)
        since = until.replace(day=1)
    return since, until


def _heat_emoji(cpa: float | None) -> str:
    """
    Грубая 'тепловая' оценка по CPA:
    дешёвые — зелёные, средние — жёлтые, дорогие — красные.
    """
    if cpa is None:
        return "⚪️"
    if cpa <= 2:
        return "🟢"
    if cpa <= 4:
        return "🟡"
    return "🔴"


def build_heatmap_for_account(
    aid: str,
    get_account_name,
    mode: str = "7",
) -> str:
    """
    Тепловая карта по адсетам аккаунта за период:
    mode = "7" | "14" | "month".

    Строит простой отчет:
    - TOP-адсеты по spend
    - для каждого: CPA и 'тепловой' индикатор
    (используется в Telegram-кнопках hm7/hm14/hmmonth).
    """
    since, until = _date_range_for_mode(mode)

    acc = AdAccount(aid)
    params = {
        "level": "adset",
        "time_range": {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        },
    }
    fields = [
        "adset_id",
        "adset_name",
        "impressions",
        "clicks",
        "spend",
        "actions",
    ]

    try:
        data = acc.get_insights(fields=fields, params=params)
    except Exception as e:
        return (
            f"⚠️ Ошибка при получении данных для {get_account_name(aid)}:\n"
            f"{e}"
        )

    if not data:
        return (
            f"По {get_account_name(aid)} нет данных по адсетам "
            f"за {since.strftime('%d.%m')}–{until.strftime('%d.%m')}"
        )

    # Агрегируем по адсетам
    agg: dict[str, dict] = {}

    for row in data:
        ad_id = row.get("adset_id") or "unknown"
        ad_name = row.get("adset_name") or ad_id

        spend, msgs, leads, total, blended = _blend_totals(row)
        impr = int(row.get("impressions", 0) or 0)
        clicks = int(row.get("clicks", 0) or 0)

        slot = agg.setdefault(
            ad_id,
            {
                "id": ad_id,
                "name": ad_name,
                "spend": 0.0,
                "impr": 0,
                "clicks": 0,
                "msgs": 0,
                "leads": 0,
                "total": 0,
            },
        )
        slot["spend"] += spend
        slot["impr"] += impr
        slot["clicks"] += clicks
        slot["msgs"] += msgs
        slot["leads"] += leads
        slot["total"] += total

    # Подсчёт CPA и сортировка по тратам
    items = list(agg.values())
    for it in items:
        if it["total"] > 0:
            it["cpa"] = it["spend"] / it["total"]
        else:
            it["cpa"] = None

    items.sort(key=lambda x: x["spend"], reverse=True)

    # Ограничимся топ-15 по тратам, чтобы сообщение не разъезжалось
    items = items[:15]

    header = [
        f"🔥 <b>Тепловая карта по адсетам</b>",
        f"Аккаунт: <b>{get_account_name(aid)}</b>",
        f"Период: {since.strftime('%d.%m.%Y')}–{until.strftime('%d.%m.%Y')}",
        "",
        "Чем ближе к 🟢 — тем дешевле заявка (по сумме переписок+лидов).",
        "",
    ]

    lines: list[str] = header

    if not items:
        lines.append("Нет активных адсетов за выбранный период.")
        return "\n".join(lines)

    for it in items:
        cpa = it["cpa"]
        emoji = _heat_emoji(cpa)
        if cpa is None:
            cpa_txt = "—"
        else:
            cpa_txt = f"{cpa:.2f} $"

        line = (
            f"{emoji} <b>{it['name']}</b>\n"
            f"   💵 {it['spend']:.2f} $  |  👁 {it['impr']}  |  🖱 {it['clicks']}\n"
            f"   💬 {it['msgs']}  |  📩 {it['leads']}  |  🧮 {it['total']}  |  🎯 CPA: {cpa_txt}"
        )
        lines.append(line)

    return "\n\n".join(lines)
