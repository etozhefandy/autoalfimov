# services/analytics.py

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import logging

from services.facebook_api import (
    fetch_insights,
    fetch_adsets,
    fetch_ads,
    fetch_campaigns,
)
from services.storage import load_accounts
from fb_report.constants import ALMATY_TZ


# ============================================================
# 🔥 БАЗОВЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def safe_div(x: float, y: float) -> float:
    if y == 0:
        return 0.0
    try:
        return float(x) / float(y)
    except Exception:
        return 0.0


def to_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


# ============================================================
# 🔥 ПАРСИНГ INSIGHT → нормальные метрики
# ============================================================

def parse_insight(ins: Dict[str, Any]) -> Dict[str, Any]:
    """
    Превращает insight-словарь в нормальные метрики в одном месте.
    """
    if not ins:
        return {
            "impr": 0,
            "clicks": 0,
            "spend": 0.0,
            "msgs": 0,
            "leads": 0,
            "total": 0,
            "cpa": None,
            "cpm": 0.0,
            "cpc": 0.0,
            "ctr": 0.0,
            "freq": 0.0,
        }

    impr = int(ins.get("impressions", 0) or 0)
    clicks = int(ins.get("clicks", 0) or 0)
    spend = to_float(ins.get("spend", 0) or 0)

    actions = ins.get("actions", []) or []
    msgs = 0
    leads = 0
    for a in actions:
        t = a.get("action_type")
        v = to_float(a.get("value", 0))
        if t == "onsite_conversion.messaging_conversation_started_7d":
            msgs += int(v)
        if t in {
            "Website Submit Applications",
            "offsite_conversion.fb_pixel_submit_application",
            "offsite_conversion.fb_pixel_lead",
            "lead",
        }:
            leads += int(v)

    total = msgs + leads
    cpa = (spend / total) if total > 0 else None
    cpm = safe_div(spend * 1000, impr)
    cpc = safe_div(spend, clicks)
    ctr = safe_div(clicks, impr) * 100
    freq = to_float(ins.get("frequency", 0.0) or 0.0)

    return {
        "impr": impr,
        "clicks": clicks,
        "spend": spend,
        "msgs": msgs,
        "leads": leads,
        "total": total,
        "cpa": cpa,
        "cpm": cpm,
        "cpc": cpc,
        "ctr": ctr,
        "freq": freq,
    }


# ============================================================
# 🔥 АНАЛИТИКА АККАУНТА / ADSETS / ADS
# ============================================================

def _make_period_for_mode(mode: str) -> Dict[str, str]:
    """Утилита для построения периода по режиму.

    Используется в Фокус-ИИ и других отчётах.
    """
    today = datetime.now(ALMATY_TZ).date()

    if mode == "today":
        since = until = today
    elif mode == "yday":
        until = today - timedelta(days=1)
        since = until
    elif mode == "7d":
        until = today - timedelta(days=1)
        since = until - timedelta(days=6)
    elif mode == "30d":
        until = today - timedelta(days=1)
        since = until - timedelta(days=29)
    else:
        # fallback = последние 7 дней до вчера
        until = today - timedelta(days=1)
        since = until - timedelta(days=6)

    return {
        "since": since.strftime("%Y-%m-%d"),
        "until": until.strftime("%Y-%m-%d"),
    }


def analyze_account(
    aid: str,
    days: int = 7,
    period: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Анализ аккаунта за последние X дней.
    """
    if period is None:
        until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
        since = until - timedelta(days=days - 1)

        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }

    ins = fetch_insights(aid, period)
    if not ins:
        return {"aid": aid, "metrics": None}

    parsed = parse_insight(ins)
    return {
        "aid": aid,
        "metrics": parsed,
        "period": period,
    }


def analyze_adsets(
    aid: str,
    days: int = 7,
    period: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Аналитика адсетов:
    - собирает адсеты
    - считает инсайты каждого
    - выстраивает ранжирование от лучшего к худшему по CPA
    """
    adsets = fetch_adsets(aid)
    results = []

    if period is None:
        until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
        since = until - timedelta(days=days - 1)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }

    for adset in adsets:
        adset_id = adset["id"]

        # инсайты по адсету
        # NB: insights по adset делаются через account.get_insights(level='adset')
        ins = fetch_insights_by_level(aid, adset_id, period, level="adset")

        parsed = parse_insight(ins or {})
        # Пропускаем адсеты с нулевым spend, чтобы не засорять отчёты
        if (parsed.get("spend") or 0.0) <= 0:
            continue
        parsed["adset_id"] = adset_id
        parsed["name"] = adset["name"]
        parsed["daily_budget"] = adset["daily_budget"]

        results.append(parsed)

    # сортируем: лучший CPA → хуже
    def score(x):
        cpa = x.get("cpa")
        return cpa if cpa is not None else 999_999

    results.sort(key=score)
    return results


def analyze_campaigns(
    aid: str,
    days: int = 7,
    period: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Аналитика кампаний за последние days дней.

    Для каждой кампании считаем стандартные метрики через parse_insight и
    сортируем кампании по spend (затратам) по убыванию.
    """
    camps = fetch_campaigns(aid)
    results: List[Dict[str, Any]] = []

    if period is None:
        until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
        since = until - timedelta(days=days - 1)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }

    for camp in camps:
        cid = camp.get("id")
        if not cid:
            continue

        ins = fetch_insights_by_level(aid, cid, period, level="campaign")
        parsed = parse_insight(ins or {})
        # Пропускаем кампании с нулевым spend
        if (parsed.get("spend") or 0.0) <= 0:
            continue
        parsed["campaign_id"] = cid
        parsed["name"] = camp.get("name", "<без названия>")
        parsed["status"] = camp.get("status")
        parsed["effective_status"] = camp.get("effective_status")

        results.append(parsed)

    # сортируем по затратам по убыванию
    results.sort(key=lambda x: x.get("spend", 0.0), reverse=True)
    return results


def analyze_ads(
    aid: str,
    days: int = 7,
    period: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Аналитика объявлений:
    - CTR
    - CPC
    - CPA
    """
    ads = fetch_ads(aid)
    results = []

    if period is None:
        until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
        since = until - timedelta(days=days - 1)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }

    for ad in ads:
        ad_id = ad["id"]

        ins = fetch_insights_by_level(aid, ad_id, period, level="ad")

        parsed = parse_insight(ins or {})
        parsed["ad_id"] = ad_id
        parsed["name"] = ad["name"]
        # Дополнительно пробуем сохранить связи с адсетом и кампанией, если есть
        parsed["adset_id"] = ad.get("adset_id")
        parsed["campaign_id"] = ad.get("campaign_id")
        parsed["adset_name"] = ad.get("adset", {}).get("name") if isinstance(ad.get("adset"), dict) else None
        parsed["campaign_name"] = ad.get("campaign", {}).get("name") if isinstance(ad.get("campaign"), dict) else None

        results.append(parsed)

    # сортировка по CPA
    results.sort(key=lambda x: x.get("cpa") if x.get("cpa") is not None else 999_999)
    return results


# ============================================================
# 🔥 ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: INSIGHTS ПО УРОВНЯМ
# ============================================================

def fetch_insights_by_level(aid: str, entity_id: str, period: Dict[str, str], level: str):
    """
    Универсальный вызов инсайтов по уровню:
    - level="adset"
    - level="ad"
    """
    from facebook_business.adobjects.adaccount import AdAccount

    from services.facebook_api import safe_api_call, _period_to_params
    from services.storage import load_local_insights, save_local_insights, period_key

    log = logging.getLogger(__name__)

    pkey = period_key(period)
    cache_key = f"{pkey}|lvl:{str(level)}|id:{str(entity_id)}"
    store = load_local_insights(aid)

    # Для периода "today" всегда берём свежие данные.
    use_cache = not (isinstance(period, str) and period == "today")
    if use_cache and cache_key in store:
        return store.get(cache_key)

    params = _period_to_params(period)
    params["level"] = level
    params["filtering"] = [
        {
            "field": f"{level}.id",
            "operator": "EQUAL",
            "value": entity_id,
        }
    ]

    fields = ["impressions", "clicks", "spend", "actions", "cpm", "cpc", "frequency"]
    acc = AdAccount(aid)
    data = safe_api_call(acc.get_insights, fields=fields, params=params)

    if not data:
        store[cache_key] = None
        save_local_insights(aid, store)
        return None

    row = data[0] if len(data) > 0 else None
    if hasattr(row, "export_all_data"):
        out = row.export_all_data()
    else:
        try:
            out = dict(row)
        except Exception:
            out = None

    # Быстрая диагностика: spend>0 есть, но ответ не парсится
    try:
        if out and float((out or {}).get("spend", 0) or 0) > 0 and not isinstance(out, dict):
            log.warning(
                "fetch_insights_by_level returned unexpected type for spend>0: aid=%s level=%s id=%s type=%s",
                aid,
                level,
                entity_id,
                type(out).__name__,
            )
    except Exception:
        pass

    store[cache_key] = out
    save_local_insights(aid, store)
    return out


# ============================================================
# 🔥 ПЛАН ФАКТ ЗАЯВОК (для автопилата)
# ============================================================

def compute_lead_plan(
    monthly_plan: int,
    days_in_month: int,
    today_day: int,
    achieved: int,
) -> Dict[str, Any]:
    """
    Простой план-факт:
    - сколько д.б. заявок на сегодня
    - отставание/опережение
    """
    daily_rate = monthly_plan / days_in_month
    expected_today = round(daily_rate * today_day)
    delta = achieved - expected_today

    return {
        "monthly_plan": monthly_plan,
        "daily_rate": daily_rate,
        "expected_today": expected_today,
        "achieved": achieved,
        "delta": delta,
    }


# ============================================================
# 🔥 БЮДЖЕТ (нормы / лимиты)
# ============================================================

def compute_daily_budget(monthly_budget_kzt: float, usd_rate: float, days: int) -> Dict[str, Any]:
    """
    Вычисляет дневной бюджет в USD.
    Месячный бюджет задаётся в тенге.
    """
    monthly_budget_usd = monthly_budget_kzt / usd_rate
    daily_budget_usd = monthly_budget_usd / days

    return {
        "monthly_budget_usd": monthly_budget_usd,
        "daily_budget_usd": daily_budget_usd,
    }


def check_daily_budget(spend_today_usd: float, daily_limit_usd: float) -> Dict[str, Any]:
    """
    Проверяет превышение дневного бюджета.
    """
    if spend_today_usd > daily_limit_usd:
        return {
            "exceeded": True,
            "delta": spend_today_usd - daily_limit_usd,
        }
    return {
        "exceeded": False,
        "delta": 0,
    }


# ============================================================
# 🔥 ГЕНЕРАЦИЯ РЕКОМЕНДАЦИЙ ДЛЯ АВТОПИЛОТА
# ============================================================

def generate_recommendations(aid: str) -> List[Dict[str, Any]]:
    """
    Генерирует базовый список рекомендаций:
    - что выключить
    - что поднять по бюджету
    - что понизить
    (первая ступень для автопилата)
    """
    adsets = analyze_adsets(aid, days=7)
    recommendations = []

    for adset in adsets:
        cpa = adset.get("cpa")
        if cpa is None:
            continue

        if cpa > 10:  # TODO: сделать динамически/по таргету
            recommendations.append({
                "action": "decrease_budget",
                "entity_type": "adset",
                "entity_id": adset["adset_id"],
                "percent": -20,
                "reason": f"Высокий CPA: {cpa:.2f}$",
            })

        if cpa < 3:
            recommendations.append({
                "action": "increase_budget",
                "entity_type": "adset",
                "entity_id": adset["adset_id"],
                "percent": +20,
                "reason": f"CPA отличный ({cpa:.2f}$)",
            })

    return recommendations
