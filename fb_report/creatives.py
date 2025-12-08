from datetime import datetime
from typing import Any, Dict, List

from facebook_business.adobjects.adaccount import AdAccount

from services.facebook_api import safe_api_call, fetch_campaigns, fetch_adsets


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

    # Карты для имён кампаний и адсетов
    campaigns = fetch_campaigns(account_id)
    campaigns_map: Dict[str, str] = {
        c.get("id"): c.get("name") or c.get("id") for c in campaigns
    }

    adsets = fetch_adsets(account_id)
    adsets_map: Dict[str, Dict[str, Any]] = {}
    for a in adsets:
        adsets_map[a.get("id")] = {
            "name": a.get("name") or a.get("id"),
            "campaign_id": a.get("campaign_id"),
        }

    acc = AdAccount(account_id)

    ads = safe_api_call(
        acc.get_ads,
        fields=[
            "id",
            "name",
            "effective_status",
            "created_time",
            "start_time",
            "adset_id",
            "campaign_id",
            "creative{instagram_permalink_url}",
        ],
        params={"effective_status": ["ACTIVE"]},
    )

    if not ads:
        return []

    tree: Dict[str, Dict[str, Any]] = {}

    for row in ads:
        try:
            adset_id = row.get("adset_id")
            campaign_id = row.get("campaign_id")

            if not campaign_id and adset_id in adsets_map:
                campaign_id = adsets_map[adset_id].get("campaign_id")

            if not campaign_id:
                continue

            campaign_name = campaigns_map.get(campaign_id, campaign_id)

            adset_info = adsets_map.get(adset_id, {}) if adset_id else {}
            adset_name = adset_info.get("name") or adset_id or "Без названия адсета"

            creative_info = row.get("creative") or {}
            if not isinstance(creative_info, dict) and hasattr(creative_info, "export_all_data"):
                try:
                    creative_info = creative_info.export_all_data()
                except Exception:
                    try:
                        creative_info = dict(creative_info)
                    except Exception:
                        creative_info = {}

            url = None
            if hasattr(creative_info, "get"):
                url = creative_info.get("instagram_permalink_url")

            if not url:
                continue

            # Дата публикации: используем created_time как основу сортировки/отображения.
            created_time_raw = row.get("created_time") or ""
            created_dt = _parse_fb_datetime(created_time_raw)

            camp_entry = tree.setdefault(
                campaign_id,
                {
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "adsets": {},
                },
            )

            adsets_dict: Dict[str, Any] = camp_entry["adsets"]
            adset_entry = adsets_dict.setdefault(
                adset_id or "unknown",
                {
                    "adset_id": adset_id,
                    "adset_name": adset_name,
                    "creatives": [],
                },
            )

            ad_name = row.get("name") or row.get("id") or "Без названия объявления"

            adset_entry["creatives"].append(
                {
                    "created_time": created_dt,
                    "ad_id": row.get("id"),
                    "ad_name": ad_name,
                    "instagram_url": url,
                }
            )
        except Exception:
            continue

    if not tree:
        return []

    campaigns_list: List[Dict[str, Any]] = []
    for camp in tree.values():
        adset_list: List[Dict[str, Any]] = []
        for a in camp["adsets"].values():
            # сортировка объявлений внутри адсета от новых к старым по created_time
            a["creatives"].sort(key=lambda x: x["created_time"], reverse=True)
            adset_list.append(a)

        # сортируем адсеты по дате самого нового объявления
        adset_list.sort(
            key=lambda ad: ad["creatives"][0]["created_time"] if ad["creatives"] else datetime.min,
            reverse=True,
        )

        camp["adsets"] = adset_list
        campaigns_list.append(camp)

    # Сортируем кампании по имени для стабильного вывода
    campaigns_list.sort(key=lambda c: c.get("campaign_name") or "")

    return campaigns_list


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
                lt = cr.get("launch_time")
                if isinstance(lt, datetime):
                    dt_str = lt.date().isoformat()
                else:
                    dt_str = "?"

                ad_name = cr.get("ad_name") or "Без названия объявления"
                url = cr.get("instagram_url") or ""

                lines.append(f"  {dt_str} — {ad_name} — 🔗 {url}")

        messages.append("\n".join(lines))

    return messages
