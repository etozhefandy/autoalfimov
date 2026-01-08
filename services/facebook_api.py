# services/facebook_api.py

from typing import Any, Dict, List, Optional
from datetime import datetime
import json
import time
import random

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.exceptions import FacebookRequestError

from config import FB_ACCESS_TOKEN
from services.storage import load_local_insights, save_local_insights, period_key


_LAST_API_ERROR: Optional[str] = None
_LAST_API_ERROR_AT: Optional[str] = None
_LAST_API_ERROR_INFO: Dict[str, Any] = {}
_RATE_LIMIT_UNTIL_TS: float = 0.0


def get_last_api_error() -> Dict[str, Optional[str]]:
    return {"error": _LAST_API_ERROR, "at": _LAST_API_ERROR_AT}


def get_last_api_error_info() -> Dict[str, Any]:
    return dict(_LAST_API_ERROR_INFO or {})


def is_rate_limited_now() -> bool:
    return time.time() < float(_RATE_LIMIT_UNTIL_TS or 0.0)


def rate_limit_retry_after_seconds() -> int:
    try:
        left = float(_RATE_LIMIT_UNTIL_TS or 0.0) - time.time()
    except Exception:
        left = 0.0
    if left < 0:
        left = 0.0
    return int(left)


def _mark_rate_limited_for(seconds: float) -> None:
    global _RATE_LIMIT_UNTIL_TS
    until = time.time() + float(seconds or 0.0)
    if until > float(_RATE_LIMIT_UNTIL_TS or 0.0):
        _RATE_LIMIT_UNTIL_TS = until


def _set_last_error_info(info: Dict[str, Any]) -> None:
    global _LAST_API_ERROR_INFO
    if not isinstance(info, dict):
        info = {"message": str(info)}
    _LAST_API_ERROR_INFO = info


# ИНИЦИАЛИЗАЦИЯ FACEBOOK API (один раз для всего проекта)
if FB_ACCESS_TOKEN:
    # Используем токен без app_id/app_secret, как в config.py.
    FacebookAdsApi.init(access_token=FB_ACCESS_TOKEN)


# ========= НИЗКОУРОВНЕВЫЕ БЕЗОПАСНЫЕ ВЫЗОВЫ =========

def safe_api_call(fn, *args, **kwargs):
    """
    Универсальная безопасная упаковка любых вызовов FB SDK.
    Ловит ошибки, возвращает None в случае неудачи.
    """
    global _LAST_API_ERROR, _LAST_API_ERROR_AT
    if is_rate_limited_now():
        _set_last_error_info(
            {
                "code": 17,
                "message": "User request limit reached (rate limited)",
                "kind": "rate_limit",
            }
        )
        return None
    try:
        return fn(*args, **kwargs)
    except FacebookRequestError as e:
        code = None
        subcode = None
        try:
            code = int(e.api_error_code())
        except Exception:
            code = None
        try:
            subcode = int(e.api_error_subcode())
        except Exception:
            subcode = None

        try:
            _LAST_API_ERROR = str(e)
        except Exception:
            _LAST_API_ERROR = "<unprintable error>"
        try:
            _LAST_API_ERROR_AT = datetime.utcnow().isoformat()
        except Exception:
            _LAST_API_ERROR_AT = None

        info = {
            "kind": "fb_request_error",
            "code": code,
            "subcode": subcode,
            "message": _LAST_API_ERROR,
        }
        _set_last_error_info(info)

        if code == 17:
            base_min = 20
            base_max = 30
            jitter = random.randint(0, 5)
            minutes = random.randint(base_min, base_max) + jitter
            _mark_rate_limited_for(float(minutes) * 60.0)

        print(f"[facebook_api] Error: {e}")
        return None
    except Exception as e:
        try:
            _LAST_API_ERROR = str(e)
        except Exception:
            _LAST_API_ERROR = "<unprintable error>"
        try:
            _LAST_API_ERROR_AT = datetime.utcnow().isoformat()
        except Exception:
            _LAST_API_ERROR_AT = None
        _set_last_error_info({"kind": "exception", "message": _LAST_API_ERROR})
        print(f"[facebook_api] Error: {e}")
        return None


def fetch_insights_bulk(
    aid: str,
    *,
    period: Any,
    level: str,
    fields: List[str],
    params_extra: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    try:
        pkey = period_key(period)
    except Exception:
        pkey = str(period)
    fields_key = ",".join([str(x) for x in (fields or [])])
    cache_key = f"insights_bulk:{aid}:{str(level)}:{pkey}:{fields_key}"

    ttl_s = 600.0 if (isinstance(period, str) and period == "today") else 3600.0
    cached = _cache_get(cache_key, ttl_s=ttl_s)
    if cached is not None:
        return list(cached)

    acc = AdAccount(aid)
    params = _period_to_params(period)
    params["level"] = str(level)
    if params_extra:
        params.update(params_extra)
    data = safe_api_call(acc.get_insights, fields=fields, params=params)
    if not data:
        stale = _cache_get(cache_key, ttl_s=24 * 3600.0)
        return list(stale) if stale is not None else []
    out: List[Dict[str, Any]] = []
    for row in data:
        out.append(_normalize_insight(row))
    _cache_set(cache_key, out)
    return out


_CATALOG_CACHE: Dict[str, Dict[str, Any]] = {}


def _cache_get(key: str, ttl_s: float) -> Any:
    it = _CATALOG_CACHE.get(key) or {}
    try:
        ts = float(it.get("ts") or 0.0)
    except Exception:
        ts = 0.0
    if not ts:
        return None
    if (time.time() - ts) > float(ttl_s):
        return None
    return it.get("value")


def _cache_set(key: str, value: Any) -> None:
    _CATALOG_CACHE[key] = {"ts": time.time(), "value": value}


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
    fields = [
        "impressions",
        "cpm",
        "clicks",
        "cpc",
        "spend",
        "actions",
        "cost_per_action_type",
    ]

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
    cache_key = f"campaigns:{aid}"
    cached = _cache_get(cache_key, ttl_s=1800.0)
    if cached is not None:
        return list(cached)

    acc = AdAccount(aid)
    data = safe_api_call(
        acc.get_campaigns,
        fields=["id", "name", "status", "effective_status"],
    )

    if not data:
        stale = _cache_get(cache_key, ttl_s=24 * 3600.0)
        return list(stale) if stale is not None else []

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

    _cache_set(cache_key, out)
    return out


# ========= AD MANAGEMENT =========

def pause_ad(ad_id: str) -> Dict[str, Any]:
    """Ставит объявление в статус PAUSED через Facebook Marketing API.

    Возвращает dict с ключами:
      - status: "ok" или "error"
      - message: человекочитаемое описание
      - api_response: сырой ответ SDK (для отладки), если есть
      - exception: текст исключения при ошибке, если было
    """

    if not ad_id:
        return {
            "status": "error",
            "message": "Пустой ad_id",
            "api_response": None,
            "exception": None,
        }

    # Гарантируем инициализацию SDK перед вызовом.
    try:
        api = FacebookAdsApi.get_default_api()
    except Exception:
        api = None

    if api is None and FB_ACCESS_TOKEN:
        try:
            FacebookAdsApi.init(access_token=FB_ACCESS_TOKEN)
        except Exception as e:  # pragma: no cover
            return {
                "status": "error",
                "message": "Не удалось инициализировать Facebook API",
                "api_response": None,
                "exception": str(e),
            }

    try:
        ad = Ad(ad_id)
        # Обновляем статус объявления на PAUSED.
        res = safe_api_call(ad.api_update, params={"status": "PAUSED"})
        if res is None:
            return {
                "status": "error",
                "message": "API вернуло пустой ответ",
                "api_response": None,
                "exception": None,
            }
    except Exception as e:  # pragma: no cover - обёртка ошибок SDK
        return {
            "status": "error",
            "message": "Исключение при вызове API",
            "api_response": None,
            "exception": str(e),
        }

    return {
        "status": "ok",
        "message": "Объявление поставлено на паузу через Facebook API.",
        "api_response": res,
        "exception": None,
    }


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
    cache_key = f"adsets:{aid}"
    cached = _cache_get(cache_key, ttl_s=1800.0)
    if cached is not None:
        return list(cached)

    acc = AdAccount(aid)
    data = safe_api_call(
        acc.get_ad_sets,
        fields=["id", "name", "daily_budget", "status", "effective_status", "campaign_id"],
    )

    if not data:
        stale = _cache_get(cache_key, ttl_s=24 * 3600.0)
        return list(stale) if stale is not None else []

    out = []
    for row in data:
        try:
            out.append({
                "id": row.get("id"),
                "name": row.get("name"),
                "campaign_id": row.get("campaign_id"),
                "daily_budget": float(row.get("daily_budget", 0)) / 100.0,
                "status": row.get("status"),
                "effective_status": row.get("effective_status"),
            })
        except Exception:
            continue

    _cache_set(cache_key, out)
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
    cache_key = f"ads:{aid}"
    cached = _cache_get(cache_key, ttl_s=900.0)
    if cached is not None:
        return list(cached)

    acc = AdAccount(aid)
    data = safe_api_call(
        acc.get_ads,
        fields=["id", "name", "adset_id", "creative", "status", "effective_status"],
    )

    if not data:
        stale = _cache_get(cache_key, ttl_s=24 * 3600.0)
        return list(stale) if stale is not None else []

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
                "effective_status": row.get("effective_status"),
            })
        except Exception:
            continue

    _cache_set(cache_key, out)
    return out
