from datetime import datetime
from typing import Any, Dict, List

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.api import FacebookAdsApi

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

    # Берём только активные объявления (effective_status=ACTIVE)
    # и сразу подтягиваем нужные под-поля креатива:
    # instagram_permalink_url и effective_object_story_id.
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
            "creative{instagram_permalink_url,effective_object_story_id,id}",
        ],
        params={"effective_status": ["ACTIVE"]},
    )

    if not ads:
        return []

    # Временное хранилище: campaign_id -> {campaign_name, adsets: {adset_id: {...}}}
    tree: Dict[str, Dict[str, Any]] = {}

    for row in ads:
        try:
            if row.get("effective_status") != "ACTIVE":
                continue

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
                    creative_info = dict(creative_info) if hasattr(creative_info, "__iter__") else {}

            creative_id = None
            if hasattr(creative_info, "get"):
                creative_id = creative_info.get("id")
            elif isinstance(creative_info, dict):
                creative_id = creative_info.get("id")

            # 1) Пытаемся использовать прямой creative.instagram_permalink_url
            url = None
            if hasattr(creative_info, "get"):
                url = creative_info.get("instagram_permalink_url")

            # 2) Если прямой ссылки нет, но есть effective_object_story_id,
            # пробуем достать permalink через Graph API.
            if not url and hasattr(creative_info, "get"):
                story_id_raw = creative_info.get("effective_object_story_id") or ""
                story_object_id = None
                if isinstance(story_id_raw, str) and "_" in story_id_raw:
                    # формат обычно ACTORID_OBJECTID, берём часть после подчёркивания
                    story_object_id = story_id_raw.split("_", 1)[1]

                if story_object_id:
                    resp = safe_api_call(
                        FacebookAdsApi.get_default_api().call,
                        "GET",
                        (story_object_id,),
                        params={"fields": "permalink,permalink_url"},
                    )

                    if resp:
                        try:
                            data = resp.json() if hasattr(resp, "json") else resp
                        except Exception:
                            data = resp

                        if hasattr(data, "get") or isinstance(data, dict):
                            link = None
                            try:
                                link = data.get("permalink") or data.get("permalink_url")
                            except Exception:
                                pass

                            if isinstance(link, str) and "instagram.com" in link:
                                url = link

            # Если ни прямой, ни фолбэк-ссылки нет — пропускаем объявление.
            if not url:
                continue

            # Дата запуска: start_time, иначе created_time
            start_time = row.get("start_time") or ""
            created_time = row.get("created_time") or ""
            launch_str = start_time or created_time
            launch_time = _parse_fb_datetime(launch_str)

            # Строим дерево
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

            ad_name = row.get("name") or creative_id

            adset_entry["creatives"].append(
                {
                    "launch_time": launch_time,
                    "ad_id": row.get("id"),
                    "ad_name": ad_name,
                    "instagram_url": url,
                }
            )
        except Exception:
            continue

    if not tree:
        return []

    # Преобразуем дерево в список и сортируем
    campaigns_list: List[Dict[str, Any]] = []
    for camp in tree.values():
        adset_list: List[Dict[str, Any]] = []
        for a in camp["adsets"].values():
            # сортировка креативов внутри адсета от новых к старым
            a["creatives"].sort(key=lambda x: x["launch_time"], reverse=True)
            adset_list.append(a)

        # можно дополнительно отсортировать адсеты по самому новому креативу
        adset_list.sort(
            key=lambda ad:
            ad["creatives"][0]["launch_time"] if ad["creatives"] else datetime.min,
            reverse=True,
        )

        camp["adsets"] = adset_list
        campaigns_list.append(camp)

    # Сортируем кампании по имени для стабильного вывода
    campaigns_list.sort(key=lambda c: c.get("campaign_name") or "")

    return campaigns_list


def format_instagram_ads_links(items: List[Dict[str, Any]], *, max_chars: int = 3500) -> List[str]:
    """Форматирует дерево кампаний/адсетов/креативов в список сообщений Telegram.

    Возвращает список текстовых блоков, чтобы не упереться в лимит длины сообщения.
    """
    if not items:
        return ["Активной рекламы в Instagram с прямыми ссылками сейчас нет."]

    messages: List[str] = []
    current_lines: List[str] = []

    def flush() -> None:
        if current_lines:
            messages.append("\n".join(current_lines))
            current_lines.clear()

    for camp in items:
        camp_name = camp.get("campaign_name") or camp.get("campaign_id") or "Без названия кампании"

        # Заголовок кампании
        header_lines = [
            f"🟩 {camp_name}",
            "────────────",
        ]

        # Проверяем, поместится ли заголовок в текущее сообщение
        if sum(len(l) + 1 for l in current_lines + header_lines) > max_chars:
            flush()

        current_lines.extend(header_lines)

        for adset in camp.get("adsets", []):
            adset_name = adset.get("adset_name") or "Без названия адсета"

            adset_header = [
                "",
                f"Адсет: {adset_name}",
                "────────────────",
            ]

            if sum(len(l) + 1 for l in current_lines + adset_header) > max_chars:
                flush()

            current_lines.extend(adset_header)

            for cr in adset.get("creatives", []):
                lt = cr.get("launch_time")
                if isinstance(lt, datetime):
                    dt_str = lt.date().isoformat()
                else:
                    dt_str = "?"

                ad_name = cr.get("ad_name") or "Без названия объявления"
                url = cr.get("instagram_url") or ""

                line = f"  {dt_str} — {ad_name} — 🔗 {url}"

                if sum(len(l) + 1 for l in current_lines) + len(line) + 1 > max_chars:
                    flush()

                current_lines.append(line)

    flush()

    return messages
