# services/facebook_api.py

from typing import Any, Dict, List, Optional
from datetime import datetime
import json

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

from config import FB_ACCESS_TOKEN, FB_APP_ID, FB_APP_SECRET
from config import ALMATY_TZ
from services.storage import load_local_insights, save_local_insights, period_key


# ИНИЦИАЛИЗАЦИЯ FACEBOOK API (один раз для всего проекта)
if FB_ACCESS_TOKEN:
    FacebookAdsApi.init(FB_APP_ID, FB_APP_SECRET, FB_ACCESS_TOKEN)


# ========= НИЗКОУРОВНЕВЫЕ БЕЗОПАСНЫЕ ВЫЗОВЫ =========

def safe_api_call(fn, *args, **kwargs):
    """
    Универсальная безопасная упаковка любых вызовов FB SDK.
    Ловит ошибки, возвращает None в случае неудачи.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        print(f"[facebook_api] Error: {e}")
        return None


# ========= ВСПОМОГАТЕЛЬНЫЕ =========

def _normalize_insight(row: Any) -> Dict[str, Any]:
    """
    AdsInsights → обычный dict.
    Если объект имеет метод export_all_data() — используем.
    Иначе возвращаем простой dict(row).
    """
    if row is None:
        return {}

    if hasattr(row, "export_all_data"):
        try:
            return row.export_all_data()
        except Exception:
            pass

    try:
        return dict(row)
    except Exception:
        return {}


def _period_to_params(period: Any) -> Dict[str, Any]:
    """
    Принимает:
    - "today", "yesterday", "last_7d"
    - {"since": "YYYY-MM-DD", "until": "YYYY-MM-DD"}

    Возвращает params для get_insights().
    """
    if isinstance(period, dict):
        return {
            "time_range": {
                "since": period.get("since"),
                "until": period.get("until")
            }
        }
    else:
        return {"date_preset": str(period)}


# ========= ОСНОВНАЯ ФУНКЦИЯ: ПОЛУЧЕНИЕ ИНСАЙТОВ С КЭШЕМ =========

def fetch_insights(aid: str, period: Any) -> Optional[Dict[str, Any]]:
    """
    Получает инсайты аккаунта:
    1) читает локальный кэш (insights_cache/<aid>.json)
    2) если нет — делает запрос в FB API
    3) сохраняет в локальный кэш
    4) возвращает dict или None
    """
    # --- 1. LOCAL CACHE ---
    store = load_local_insights(aid)
    pkey = period_key(period)

    # Для периода "today" всегда берём свежие данные из API,
    # игнорируя уже существующий кэш, но после запроса обновляем его.
    use_cache = not (isinstance(period, str) and period == "today")

    if use_cache and pkey in store:
        return store[pkey]  # может быть dict или None

    # --- 2. FACEBOOK API REQUEST ---
    params = _period_to_params(period)
    params["level"] = "account"
    fields = ["impressions", "cpm", "clicks", "cpc", "spend", "actions"]

    acc = AdAccount(aid)
    data = safe_api_call(acc.get_insights, fields=fields, params=params)

    if not data:
        store[pkey] = None
        save_local_insights(aid, store)
        return None

    raw = data[0] if len(data) > 0 else None
    insight_dict = _normalize_insight(raw)

    # --- 3. SAVE TO LOCAL CACHE ---
    store[pkey] = insight_dict
    save_local_insights(aid, store)

    return insight_dict


# ========= КАМПАНИИ =========

def fetch_campaigns(aid: str) -> List[Dict[str, Any]]:
    """
    Возвращает список кампаний аккаунта:
    [
      {"id": "...", "name": "...", "status": "..."},
      ...
    ]
    """
    acc = AdAccount(aid)
    data = safe_api_call(
        acc.get_campaigns,
        fields=["id", "name", "status", "effective_status"]
    )

    if not data:
        return []

    out = []
    for row in data:
        try:
            out.append({
                "id": row.get("id"),
                "name": row.get("name"),
                "status": row.get("status"),
                "effective_status": row.get("effective_status"),
            })
        except Exception:
            continue

    return out


# ========= ADSETS =========

def fetch_adsets(aid: str) -> List[Dict[str, Any]]:
    """
    Возвращает список адсетов в аккаунте:
    [
      {
        "id": "...",
        "name": "...",
        "daily_budget": 2000,
        "status": "ACTIVE",
        "campaign_id": "123",
      }
    ]
    """
    acc = AdAccount(aid)
    data = safe_api_call(
        acc.get_ad_sets,
        fields=["id", "name", "daily_budget", "status", "campaign_id"]
    )

    if not data:
        return []

    out = []
    for row in data:
        try:
            out.append({
                "id": row.get("id"),
                "name": row.get("name"),
                "campaign_id": row.get("campaign_id"),
                "daily_budget": float(row.get("daily_budget", 0)) / 100.0,
                "status": row.get("status"),
            })
        except Exception:
            continue

    return out


# ========= AD CREATIVES =========

def fetch_ads(aid: str) -> List[Dict[str, Any]]:
    """
    Возвращает объявления:
    [
      {
        "id": "...",
        "adset_id": "...",
        "name": "...",
        "creative_id": "...",
        "status": "ACTIVE",
      }
    ]
    """
    acc = AdAccount(aid)
    data = safe_api_call(
        acc.get_ads,
        fields=["id", "name", "adset_id", "creative", "status"]
    )

    if not data:
        return []

    out = []
    for row in data:
        creative_id = None
        try:
            c = row.get("creative")
            if c and hasattr(c, "get"):
                creative_id = c.get("id")
        except Exception:
            pass

        try:
            out.append({
                "id": row.get("id"),
                "name": row.get("name"),
                "adset_id": row.get("adset_id"),
                "creative_id": creative_id,
                "status": row.get("status"),
            })
        except Exception:
            continue

    return out
