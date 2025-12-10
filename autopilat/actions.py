# autopilat/actions.py

from typing import Dict, Any, Optional
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi

from services.facebook_api import safe_api_call
from config import FB_ACCESS_TOKEN


# Инициализация API (на случай прямого вызова)
if FB_ACCESS_TOKEN:
    # Используем только access_token, как в services/facebook_api.
    FacebookAdsApi.init(access_token=FB_ACCESS_TOKEN)


# ============================================================
# 🔥 РАЗБОР РУЧНОГО ВВОДА ПРОЦЕНТА
# ============================================================

def parse_manual_input(text: str) -> Optional[float]:
    """
    Принимает строку вида:
    '1.2', '1,2', '20', '-15', '+5'

    Возвращает float процента:
    1.2 → +1.2
    -15 → -15.0

    Если ввод неверный → None
    """
    if not text:
        return None

    cleaned = text.strip().replace(",", ".")
    try:
        return float(cleaned)
    except Exception:
        return None


# ============================================================
# 🔥 ИЗМЕНЕНИЕ БЮДЖЕТА ADSET
# ============================================================

def apply_budget_change(adset_id: str, percent: float) -> Dict[str, Any]:
    """
    Реально применяет изменение бюджета.

    percent:
       +20 → поднять на 20%
       -20 → опустить на 20%

    Возвращает:
    {
        "status": "ok" / "error",
        "old_budget": ...,
        "new_budget": ...,
        "message": "...",
    }
    """
    # Мягкое ограничение изменения за один шаг, чтобы не было резких скачков.
    max_step = 30.0
    if percent > max_step:
        percent = max_step
    elif percent < -max_step:
        percent = -max_step

    adset = AdSet(adset_id)

    # Получаем текущий бюджет
    info = safe_api_call(adset.api_get, fields=["daily_budget"])
    if not info:
        return {
            "status": "error",
            "message": f"Не удалось получить бюджет адсета {adset_id}"
        }

    old_budget = float(info.get("daily_budget", 0)) / 100.0
    if old_budget <= 0:
        old_budget = 1.0  # защита

    # Новый бюджет
    new_budget = old_budget * (1 + percent / 100)

    # Минимальный бюджет — 1$
    if new_budget < 1.0:
        new_budget = 1.0

    new_budget_fb = int(round(new_budget * 100))  # FB требует в центах, integer

    # Применяем
    res = safe_api_call(
        adset.api_update,
        params={"daily_budget": new_budget_fb}
    )

    if res is None:
        return {
            "status": "error",
            "message": f"Ошибка при обновлении бюджета {adset_id}"
        }

    return {
        "status": "ok",
        "old_budget": old_budget,
        "new_budget": new_budget,
        "message": f"Бюджет {adset_id} изменён: {old_budget:.2f} → {new_budget:.2f} $"
    }


# ============================================================
# 🔥 ВЫКЛЮЧЕНИЕ ADSET
# ============================================================

def disable_entity(adset_id: str) -> Dict[str, Any]:
    """
    Выключает ADSET через Facebook API.
    """
    adset = AdSet(adset_id)

    res = safe_api_call(
        adset.api_update,
        params={"status": "PAUSED"}
    )

    if res is None:
        return {
            "status": "error",
            "message": f"Не удалось выключить {adset_id}"
        }

    return {
        "status": "ok",
        "message": f"🛑 ADSET {adset_id} выключен."
    }


# ============================================================
# 🔥 ПРАВИЛА БЕЗОПАСНОСТИ
# ============================================================

def can_disable(adaccount_id: str, adset_id_to_disable: str) -> bool:
    """
    Проверяем, что отключение adset НЕ оставит аккаунт без трафика.
    """
    acc = AdAccount(adaccount_id)
    adsets = safe_api_call(
        acc.get_ad_sets,
        fields=["id", "status"]
    )

    if not adsets:
        return False

    active_count = 0
    for a in adsets:
        if a.get("status") == "ACTIVE" and a.get("id") != adset_id_to_disable:
            active_count += 1

    return active_count > 0
