# autopilat/ui.py

ALMATY_TZ = timezone("Asia/Almaty")
from datetime import datetime, timedelta
from pytz import timezone
from facebook_business.adobjects.adset import AdSet

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

# autopilat/ui.py

def build_recommendations_ui(items: list[dict]) -> list[dict]:
    """
    На входе items — список рекомендаций от движка.
    На выходе — список блоков вида:
    {
      "text": "...",
      "reply_markup": InlineKeyboardMarkup(...)
    }
    """

    # считаем период как "последние 7 дней до вчера"
    now = datetime.now(ALMATY_TZ).date()
    until = now - timedelta(days=1)
    since = until - timedelta(days=6)
    period_label = f"{since.strftime('%d.%m.%Y')}–{until.strftime('%d.%m.%Y')}"

    blocks: list[dict] = []

    for it in items:
        entity_id = it.get("entity_id") or ""
        reason = it.get("reason") or ""
        suggestion = it.get("suggestion") or ""
        cpa = it.get("cpa")
        metric_label = it.get("metric_label") or "CPA"

        # Пытаемся подтянуть название адсета и имена объявлений
        adset_name = None
        ad_names: list[str] = []

        if entity_id:
            try:
                adset = AdSet(entity_id).api_get(fields=["name"])
                adset_name = adset.get("name")

                # объявления внутри адсета
                ads = AdSet(entity_id).get_ads(fields=["name"])
                ad_names = [a.get("name") for a in ads if a.get("name")]
            except Exception:
                # если что-то не получилось — просто оставим ID
                pass

        header_lines = ["⏳ Рекомендация"]

        if adset_name:
            header_lines.append(f"Adset: <b>{adset_name}</b>")
            header_lines.append(f"ID: <code>{entity_id}</code>")
        elif entity_id:
            header_lines.append(f"ID adset: <code>{entity_id}</code>")

        header_lines.append(f"Данные за: {period_label}")

        if cpa is not None:
            header_lines.append(f"{metric_label}: {cpa:.2f} $")

        header_lines.append(f"Причина: {reason}")
        header_lines.append(f"Предлагаемая корректировка: {suggestion}")

        # Добавляем названия объявлений, если есть
        if ad_names:
            header_lines.append("")
            header_lines.append("Объявления в этом adset:")
            for name in ad_names[:10]:  # чтобы не улететь в простыню
                header_lines.append(f"• {name}")

        text = "\n".join(header_lines)

        # кнопки: up/down/manual/off/back
        kb = recommendation_buttons(entity_id)

        blocks.append(
            {
                "text": text,
                "reply_markup": kb,
            }
        )

    return blocks
