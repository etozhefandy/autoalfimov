from datetime import datetime
from typing import Any, Dict, List

from facebook_business.adobjects.adaccount import AdAccount

from services.facebook_api import safe_api_call


def _parse_fb_datetime(value: str) -> datetime:
    """Парсит дату/время из формата Facebook API.

    Ожидаемый формат: 'YYYY-MM-DDTHH:MM:SS+0000'.
    При ошибке возвращает datetime.min, чтобы такие записи уходили в конец.
    """
    if not value:
        return datetime.min
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        try:
            # Фолбэк без таймзоны
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return datetime.min


def fetch_instagram_active_ads_links(account_id: str) -> List[Dict[str, Any]]:
    """Возвращает плоский список активных инста-объявлений вида:

    [
      {
        "created_time": datetime,
        "ad_id": "...",
        "ad_name": "...",
        "instagram_url": "...",
      },
      ...
    ]

    Берём только объявления с effective_status = ACTIVE и непустым
    creative.instagram_permalink_url.
    """

    acc = AdAccount(account_id)

    ads = safe_api_call(
        acc.get_ads,
        fields=[
            "id",
            "name",
            "effective_status",
            "created_time",
            "creative{instagram_permalink_url}",
        ],
        params={"effective_status": ["ACTIVE"]},
    )

    if not ads:
        return []

    result: List[Dict[str, Any]] = []

    for row in ads:
        try:
            if row.get("effective_status") != "ACTIVE":
                continue

            creative_info = row.get("creative") or {}
            if not isinstance(creative_info, dict) and hasattr(creative_info, "export_all_data"):
                try:
                    creative_info = creative_info.export_all_data()
                except Exception:
                    creative_info = dict(creative_info) if hasattr(creative_info, "__iter__") else {}

            url = None
            if hasattr(creative_info, "get"):
                url = creative_info.get("instagram_permalink_url")

            if not url:
                continue

            created_time_raw = row.get("created_time") or ""
            created_dt = _parse_fb_datetime(created_time_raw)

            ad_name = row.get("name") or row.get("id") or "Без названия объявления"

            result.append(
                {
                    "created_time": created_dt,
                    "ad_id": row.get("id"),
                    "ad_name": ad_name,
                    "instagram_url": url,
                }
            )
        except Exception:
            continue

    # Сортируем от новых к старым по created_time
    result.sort(key=lambda x: x["created_time"], reverse=True)

    return result


def format_instagram_ads_links(items: List[Dict[str, Any]], *, max_chars: int = 3500) -> List[str]:
    """Форматирует плоский список объявлений в один или несколько текстов для Telegram.

    Формат:

    Активная реклама (Instagram, от новых к старым):

    2025-12-03 | Телефон
    https://www.instagram.com/p/...

    2025-11-24 | 1 рус
    https://www.instagram.com/p/...
    """
    if not items:
        return ["Активной рекламы в Instagram с прямыми ссылками сейчас нет."]

    messages: List[str] = []
    current_lines: List[str] = [
        "Активная реклама (Instagram, от новых к старым):",
        "",
    ]

    def flush() -> None:
        if current_lines:
            messages.append("\n".join(current_lines))
            current_lines.clear()

    for ad in items:
        created = ad.get("created_time")
        if isinstance(created, datetime):
            dt_str = created.date().isoformat()
        else:
            dt_str = "?"

        name = ad.get("ad_name") or "Без названия объявления" 
        url = ad.get("instagram_url") or ""

        line1 = f"{dt_str} | {name}"
        line2 = url
        block = [line1, line2, ""]

        # Проверяем, поместится ли блок в текущее сообщение
        if sum(len(l) + 1 for l in current_lines + block) > max_chars:
            flush()
            # после сброса начинаем новый блок с заголовком
            current_lines.extend([
                "Активная реклама (Instagram, от новых к старым):",
                "",
            ])

        current_lines.extend(block)

    flush()

    return messages
