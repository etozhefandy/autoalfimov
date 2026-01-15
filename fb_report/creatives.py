from datetime import datetime
from typing import Any, Dict, List

from services.facebook_api import deny_fb_api_calls


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
    """Возвращает иерархию активной инста-рекламы вида:

    [
      {
        "campaign_id": "...",
        "campaign_name": "...",
        "adsets": [
          {
            "adset_id": "...",
            "adset_name": "...",
            "creatives": [
              {
                "launch_time": datetime,
                "ad_id": "...",
                "ad_name": "...",
                "instagram_url": "...",
              },
              ...
            ],
          },
          ...
        ],
      },
      ...
    ]

    Берём только объявления с effective_status = ACTIVE и непустым
    creative.instagram_permalink_url.
    """

    with deny_fb_api_calls(reason="creatives_fetch_instagram_links"):
        return []


def format_instagram_ads_links(items: List[Dict[str, Any]], *, max_chars: int = 3500) -> List[str]:
    """Форматирует дерево кампаний/адсетов/объявлений в список сообщений Telegram.

    Каждая кампания — отдельное сообщение, в стиле, как на макете:

    🟩 Название кампании
    ────────────
    Адсет: Адсет 1
    ────────────────
      2025-12-03 — Телефон — 🔗 https://www.instagram.com/p/...
      ...
    """
    if not items:
        return ["Активной рекламы в Instagram с прямыми ссылками сейчас нет."]

    messages: List[str] = []

    for camp in items:
        camp_name = camp.get("campaign_name") or camp.get("campaign_id") or "Без названия кампании"

        lines: List[str] = [
            f"🟩 {camp_name}",
            "────────────",
        ]

        for adset in camp.get("adsets", []):
            adset_name = adset.get("adset_name") or "Без названия адсета"

            lines.extend([
                "",
                f"Адсет: {adset_name}",
                "────────────────",
            ])

            for cr in adset.get("creatives", []):
                lt = cr.get("created_time")
                if isinstance(lt, datetime):
                    dt_str = lt.date().isoformat()
                else:
                    dt_str = "?"

                ad_name = cr.get("ad_name") or "Без названия объявления"
                url = cr.get("instagram_url") or ""

                lines.append(f"  {dt_str} — {ad_name} — 🔗 {url}")

        messages.append("\n".join(lines))

    return messages
