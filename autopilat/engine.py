# autopilat/engine.py

from typing import List, Dict, Any, Optional

from services.analytics import generate_recommendations
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


# ============================================================
# 🔥 БАЗОВЫЕ РЕЖИМЫ АВТОПИЛОТА
# ============================================================

AUTOPILOT_MODES = {
    "recommendations": "Рекомендации",
    "autopilot": "Автопилат",
}

AUTOPILOT_SUBMODES = {
    "manual": "Ручной ввод",
    "auto": "Автоматически (на усмотрение Пилата)",
}


# ============================================================
# 🔥 UI КНОПКИ ДЛЯ КОНКРЕТНОЙ РЕКОМЕНДАЦИИ
# ============================================================

def recommendation_action_buttons(entity_id: str) -> InlineKeyboardMarkup:
    """
    Генерируем набор кнопок под рекомендацией:
    [⬇️ -20%] [⬆️ +20%] [Выключить] [Назад]
    """
    buttons = [
        [
            InlineKeyboardButton("⬇️ -20% бюджета", callback_data=f"ap|down20|{entity_id}"),
            InlineKeyboardButton("⬆️ +20% бюджета", callback_data=f"ap|up20|{entity_id}"),
        ],
        [
            InlineKeyboardButton("🔴 Выключить", callback_data=f"ap|off|{entity_id}")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="ap|back")
        ]
    ]

    return InlineKeyboardMarkup(buttons)


# ============================================================
# 🔥 UI ДЛЯ ВЫБОРА РЕЖИМА АВТОПИЛОТА
# ============================================================

def autopilot_mode_selector() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🧠 Рекомендации", callback_data="apmode|recommendations"),
                InlineKeyboardButton("🤖 Автопилат", callback_data="apmode|autopilot"),
            ]
        ]
    )


def autopilot_submode_selector() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✍️ Ручной ввод", callback_data="apsub|manual"),
                InlineKeyboardButton("⚡ Автоматически", callback_data="apsub|auto"),
            ]
        ]
    )


# ============================================================
# 🔥 ГЕНЕРАЦИЯ UI ДЛЯ РЕКОМЕНДАЦИЙ
# ============================================================

def get_recommendations_ui(aid: str) -> Dict[str, Any]:
    """
    Возвращает:
    {
      "text": "...",
      "items": [
         {"entity_id": "...", "text": "...", "buttons": InlineKeyboardMarkup(...)},
      ]
    }
    """

    recs = generate_recommendations(aid)
    if not recs:
        return {
            "text": f"По аккаунту нет рекомендаций.",
            "items": []
        }

    items = []
    for r in recs:
        entity_id = r["entity_id"]
        percent = r.get("percent")
        reason = r.get("reason")

        txt = (
            f"⏳ <b>Рекомендация</b>\n"
            f"ID: <code>{entity_id}</code>\n"
            f"Причина: {reason}\n"
        )
        if percent:
            txt += f"Предлагаемая корректировка: {percent:+}%"


        items.append({
            "entity_id": entity_id,
            "text": txt,
            "buttons": recommendation_action_buttons(entity_id)
        })

    return {
        "text": f"🔍 Найдено рекомендаций: {len(items)}",
        "items": items
    }


# ============================================================
# 🔥 ОБРАБОТКА ДЕЙСТВИЙ АВТОПИЛОТА
# ============================================================

def handle_autopilot_action(action: str, entity_id: str) -> Dict[str, Any]:
    """
    Возвращает структуру:
    {
      "status": "ok" / "error",
      "message": "Что написать пользователю",
      "effect": {...}
    }

    Пока НЕ изменяем реальные бюджеты — это будет в actions.py
    Сейчас — только заглушки.
    """
    if action == "down20":
        return {
            "status": "ok",
            "message": f"⬇️ Снижение бюджета для <code>{entity_id}</code> на 20% (требуется подтверждение).",
            "effect": {
                "type": "budget_change",
                "entity_id": entity_id,
                "delta_percent": -20,
            }
        }

    if action == "up20":
        return {
            "status": "ok",
            "message": f"⬆️ Увеличение бюджета для <code>{entity_id}</code> на 20% (требуется подтверждение).",
            "effect": {
                "type": "budget_change",
                "entity_id": entity_id,
                "delta_percent": 20,
            }
        }

    if action == "off":
        return {
            "status": "ok",
            "message": f"🔴 Выключение <code>{entity_id}</code> (требуется подтверждение).",
            "effect": {
                "type": "disable",
                "entity_id": entity_id,
            }
        }

    return {
        "status": "error",
        "message": f"Неизвестное действие: {action}",
        "effect": None
    }
