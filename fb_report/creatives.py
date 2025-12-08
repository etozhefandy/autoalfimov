from datetime import datetime
from typing import Any, Dict, List

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adcreative import AdCreative

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


def fetch_instagram_active_ads_links(account_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Возвращает список активных инста-объявлений с ссылками на посты.

    Каждый элемент:
    {
      "launch_time": datetime,
      "name": str,
      "instagram_url": str,
    }
    """
    acc = AdAccount(account_id)

    # Берём только активные объявления (effective_status=ACTIVE)
    ads = safe_api_call(
        acc.get_ads,
        fields=[
            "id",
            "name",
            "effective_status",
            "created_time",
            "start_time",
            "creative",
        ],
        params={"effective_status": ["ACTIVE"]},
    )

    if not ads:
        return []

    results: List[Dict[str, Any]] = []

    for row in ads:
        try:
            if row.get("effective_status") != "ACTIVE":
                continue

            creative_info = row.get("creative") or {}
            creative_id = None
            if hasattr(creative_info, "get"):
                creative_id = creative_info.get("id")
            else:
                creative_id = creative_info.get("id") if isinstance(creative_info, dict) else None

            if not creative_id:
                continue

            creative = safe_api_call(
                AdCreative(creative_id).api_get,
                fields=["instagram_permalink_url"],
            )
            if not creative:
                continue

            url = creative.get("instagram_permalink_url")
            if not url:
                continue

            # Дата запуска: start_time, иначе created_time
            start_time = row.get("start_time") or ""
            created_time = row.get("created_time") or ""
            launch_str = start_time or created_time
            launch_time = _parse_fb_datetime(launch_str)

            results.append(
                {
                    "launch_time": launch_time,
                    "name": row.get("name") or creative_id,
                    "instagram_url": url,
                }
            )
        except Exception:
            continue

    if not results:
        return []

    # Сортируем от новых к старым
    results.sort(key=lambda x: x["launch_time"], reverse=True)

    return results[:limit]


def format_instagram_ads_links(items: List[Dict[str, Any]]) -> str:
    """Форматирует список ссылок в текст для Telegram."""
    if not items:
        return "Активной рекламы в Instagram с прямыми ссылками сейчас нет."

    lines: List[str] = []
    for item in items:
        dt = item["launch_time"].date().isoformat() if isinstance(item.get("launch_time"), datetime) else "?"
        name = item.get("name") or "Без названия"
        url = item.get("instagram_url") or ""
        lines.append(f"{dt} | {name} \n🔗 {url}")

    return "\n\n".join(lines)
