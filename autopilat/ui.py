# autopilat/ui.py

from telegram import InlineKeyboardMarkup, InlineKeyboardButton


# ============================================================
# 🔥 РЕЖИМЫ АВТОПИЛОТА (главная панель)
# ============================================================

def autopilot_main_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🧠 Рекомендации", callback_data="apmode|recommendations"),
            InlineKeyboardButton("🤖 Автопилат", callback_data="apmode|autopilot"),
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="menu")
        ]
    ])


# ============================================================
# 🔥 ПОД-РЕЖИМЫ (ручной / автомат)
# ============================================================

def autopilot_submode_menu():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✍️ Ручной ввод", callback_data="apsub|manual"),
            InlineKeyboardButton("⚡ Автоматически", callback_data="apsub|auto"),
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="ap_back_main")
        ]
    ])


# ============================================================
# 🔥 КНОПКИ ПОД РЕКОМЕНДАЦИЕЙ
# ============================================================

def recommendation_buttons(entity_id: str):
    """
    Набор кнопок:
    [⬇️ -20%] [⬆️ +20%]
    [Ввести вручную]
    [Выключить]
    [Назад]
    """
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⬇️ -20%", callback_data=f"ap|down20|{entity_id}"),
            InlineKeyboardButton("⬆️ +20%", callback_data=f"ap|up20|{entity_id}"),
        ],
        [
            InlineKeyboardButton("✍️ Ввести вручную", callback_data=f"ap|manual|{entity_id}")
        ],
        [
            InlineKeyboardButton("🔴 Выключить", callback_data=f"ap|off|{entity_id}")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="ap|back")
        ]
    ])


# ============================================================
# 🔥 КНОПКИ ПОДТВЕРЖДЕНИЯ ДЕЙСТВИЯ
# ============================================================

def confirm_action_buttons(action: str, entity_id: str):
    """
    Кнопки:
    [Да] [Нет]
    """
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да", callback_data=f"apconfirm|yes|{action}|{entity_id}"),
            InlineKeyboardButton("❌ Нет", callback_data=f"apconfirm|no|{action}|{entity_id}"),
        ]
    ])


# ============================================================
# 🔥 УНИВЕРСАЛЬНЫЙ UI-СТРОИТЕЛЬ ДЛЯ СПИСКА РЕКОМЕНДАЦИЙ
# ============================================================

def build_recommendations_ui(items):
    """
    items — список:
    [
        {
            "entity_id": "...",
            "text": "...",
        },
        ...
    ]

    Возвращает список готовых блоков:
    [
        {"text": "...", "reply_markup": InlineKeyboardMarkup(...)},
    ]
    """
    blocks = []
    for it in items:
        entity_id = it["entity_id"]
        text = it["text"]

        blocks.append({
            "text": text,
            "reply_markup": recommendation_buttons(entity_id)
        })

    return blocks
