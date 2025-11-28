# services/analytics.py

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

from services.facebook_api import (
    fetch_insights,
    fetch_adsets,
    fetch_ads,
)
from services.storage import load_accounts
from config import ALMATY_TZ


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
    }


# ============================================================
# 🔥 АНАЛИТИКА АККАУНТА / ADSETS / ADS
# ============================================================

def analyze_account(aid: str, days: int = 7) -> Dict[str, Any]:
    """
    Анализ аккаунта за последние X дней.
    """
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


def analyze_adsets(aid: str, days: int = 7) -> List[Dict[str, Any]]:
    """
    Аналитика адсетов:
    - собирает адсеты
    - считает инсайты каждого
    - выстраивает ранжирование от лучшего к худшему по CPA
    """
    adsets = fetch_adsets(aid)
    results = []

    for adset in adsets:
        adset_id = adset["id"]

        # строим период
        until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
        since = until - timedelta(days=days - 1)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }

        # инсайты по адсету
        # NB: insights по adset делаются через account.get_insights(level='adset')
        ins = fetch_insights_by_level(aid, adset_id, period, level="adset")

        parsed = parse_insight(ins or {})
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


def analyze_ads(aid: str, days: int = 7) -> List[Dict[str, Any]]:
    """
    Аналитика объявлений:
    - CTR
    - CPC
    - CPA
    """
    ads = fetch_ads(aid)
    results = []

    for ad in ads:
        ad_id = ad["id"]

        until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
        since = until - timedelta(days=days - 1)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }

        ins = fetch_insights_by_level(aid, ad_id, period, level="ad")

        parsed = parse_insight(ins or {})
        parsed["ad_id"] = ad_id
        parsed["name"] = ad["name"]

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

    params = {
        "level": level,
        "time_range": {
            "since": period["since"],
            "until": period["until"],
        },
        "filtering": [
            {
                "field": f"{level}.id",
                "operator": "EQUAL",
                "value": entity_id,
            }
        ],
    }

    fields = ["impressions", "clicks", "spend", "actions", "cpm", "cpc"]

    acc = AdAccount(aid)

    try:
        data = acc.get_insights(fields=fields, params=params)
    except Exception as e:
        print(f"[fetch_insights_by_level] {e}")
        return None

    if not data:
        return None

    row = data[0]
    if hasattr(row, "export_all_data"):
        return row.export_all_data()

    try:
        return dict(row)
    except Exception:
        return None


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
