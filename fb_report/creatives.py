from datetime import datetime
from typing import Any, Dict, List

import re
import logging

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.ad import Ad

from services.facebook_api import allow_fb_api_calls, safe_api_call
from services.facebook_api import fetch_adsets


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
    """Возвращает список adset'ов с активными объявлениями и IG-ссылками:

    Формат:
    [
      {
        "adset_id": "...",
        "adset_name": "...",
        "ads": [
          {
            "ad_id": "...",
            "ad_name": "...",
            "created_time": datetime|None,
            "updated_time": datetime|None,
            "instagram_url": str|None,
          },
          ...
        ],
      },
      ...
    ]
    """

    def _dt_or_none(value: Any) -> datetime | None:
        try:
            if isinstance(value, datetime):
                return value
            if isinstance(value, str) and value.strip():
                dt = _parse_fb_datetime(value.strip())
                return None if dt == datetime.min else dt
        except Exception:
            return None
        return None

    def _try_int(v: Any) -> int | None:
        try:
            s = str(v or "").strip()
            if not s:
                return None
            return int(s)
        except Exception:
            return None

    def _find_instagram_permalink_url(obj: Any, *, _depth: int = 0) -> str | None:
        if obj is None:
            return None
        if _depth > 4:
            return None
        if isinstance(obj, dict):
            try:
                vv = str(obj.get("instagram_permalink_url") or "").strip()
            except Exception:
                vv = ""
            if vv:
                return vv
            for v in obj.values():
                found = _find_instagram_permalink_url(v, _depth=_depth + 1)
                if found:
                    return found
        if isinstance(obj, list):
            for v in obj:
                found = _find_instagram_permalink_url(v, _depth=_depth + 1)
                if found:
                    return found
        return None

    def _ig_permalink_from_media_id(media_id: str) -> str | None:
        mid = str(media_id or "").strip()
        if not mid:
            return None
        try:
            api = FacebookAdsApi.get_default_api()
        except Exception:
            api = None
        if api is None:
            return None

        try:
            res = safe_api_call(
                api.call,
                "GET",
                f"/{mid}",
                params={"fields": "permalink"},
                _caller="ads_links",
                _meta={"endpoint": "ig_permalink", "aid": str(account_id), "media_id": str(mid)},
            )
        except Exception:
            res = None

        try:
            url = str((res or {}).get("permalink") or "").strip()
        except Exception:
            url = ""
        if url and "instagram.com" in url:
            return url
        return None

    def _ig_url_from_creative(creative: Any) -> str | None:
        if not creative:
            return None

        try:
            if hasattr(creative, "get"):
                v = creative.get("instagram_permalink_url")
            elif isinstance(creative, dict):
                v = creative.get("instagram_permalink_url")
            else:
                v = None
            v = str(v or "").strip()
            if v:
                return v
        except Exception:
            pass

        try:
            if hasattr(creative, "get"):
                spec = creative.get("object_story_spec")
            elif isinstance(creative, dict):
                spec = creative.get("object_story_spec")
            else:
                spec = None
            if isinstance(spec, dict):
                for k in [
                    "instagram_permalink_url",
                    "permalink_url",
                ]:
                    vv = str((spec or {}).get(k) or "").strip()
                    if vv:
                        return vv
        except Exception:
            pass

        try:
            if hasattr(creative, "get"):
                afs = creative.get("asset_feed_spec")
            elif isinstance(creative, dict):
                afs = creative.get("asset_feed_spec")
            else:
                afs = None
            if isinstance(afs, dict):
                vv = str((afs or {}).get("instagram_permalink_url") or "").strip()
                if vv:
                    return vv
        except Exception:
            pass

        try:
            found = _find_instagram_permalink_url(creative)
            if found:
                return found
        except Exception:
            pass

        # 1) effective_instagram_media_id -> permalink
        try:
            mid = None
            if hasattr(creative, "get"):
                mid = creative.get("effective_instagram_media_id")
            elif isinstance(creative, dict):
                mid = creative.get("effective_instagram_media_id")
            mid_s = str(mid or "").strip()
            if mid_s:
                url = _ig_permalink_from_media_id(mid_s)
                if url:
                    return url
        except Exception:
            pass

        # 2) object_story_id or effective_object_story_id -> try to resolve as IG media id
        for key in ["object_story_id", "effective_object_story_id"]:
            try:
                raw = None
                if hasattr(creative, "get"):
                    raw = creative.get(key)
                elif isinstance(creative, dict):
                    raw = creative.get(key)
                s = str(raw or "").strip()
                if not s:
                    continue
                cand = s
                if "_" in cand:
                    cand = cand.split("_", 1)[1]
                url = _ig_permalink_from_media_id(cand)
                if url:
                    return url
            except Exception:
                continue

        return None

    def _ig_url_from_previews(ad_id: str) -> str | None:
        if not ad_id:
            return None
        ad = Ad(str(ad_id))
        formats = [
            "INSTAGRAM_STANDARD",
            "INSTAGRAM_STORY",
            "INSTAGRAM_REELS",
        ]
        for fmt in formats:
            try:
                previews = safe_api_call(
                    ad.get_previews,
                    params={"ad_format": fmt},
                    _caller="ads_links",
                    _meta={"endpoint": "ad_previews", "aid": str(account_id), "ad_id": str(ad_id)},
                )
            except Exception:
                previews = None
            for p in (previews or []):
                try:
                    body = p.get("body") if hasattr(p, "get") else None
                except Exception:
                    body = None
                text = str(body or "")
                m = re.search(r"https?://(?:www\.)?instagram\.com/[^\s\"']+", text)
                if m:
                    url = str(m.group(0) or "").strip()
                    if url:
                        return url
        return None

    with allow_fb_api_calls(reason="ads_links"):
        adsets = fetch_adsets(str(account_id))

        adset_name_map: dict[str, str] = {}
        for a in (adsets or []):
            try:
                _id = str((a or {}).get("id") or "").strip()
                if not _id:
                    continue
                adset_name_map[_id] = str((a or {}).get("name") or _id)
            except Exception:
                continue

        acc = AdAccount(str(account_id))
        fields = [
            "id",
            "name",
            "adset_id",
            "effective_status",
            "created_time",
            "updated_time",
            "creative{instagram_permalink_url,effective_object_story_id,effective_instagram_media_id,object_story_spec,object_story_id,asset_feed_spec}",
        ]
        params = {
            "effective_status": ["ACTIVE"],
            "limit": 250,
        }
        data = safe_api_call(
            acc.get_ads,
            fields=fields,
            params=params,
            _caller="ads_links",
            _meta={"endpoint": "get_ads", "aid": str(account_id)},
        )

        if not data:
            return []

        by_adset: dict[str, list[dict]] = {}
        links_found = 0
        links_missing = 0
        active_ads_fetched = 0
        for row in (data or []):
            try:
                if not row:
                    continue
                st = str(row.get("effective_status") or "").strip().upper()
                if st != "ACTIVE":
                    continue
                ad_id = str(row.get("id") or "").strip()
                if not ad_id:
                    continue
                adset_id = str(row.get("adset_id") or "").strip()
                if not adset_id:
                    continue
                ad_name = str(row.get("name") or ad_id)
                created_dt = _dt_or_none(row.get("created_time"))
                updated_dt = _dt_or_none(row.get("updated_time"))

                creative = row.get("creative")
                ig_url = _ig_url_from_creative(creative)
                if not ig_url:
                    ig_url = _ig_url_from_previews(ad_id)

                active_ads_fetched += 1
                if ig_url:
                    links_found += 1
                else:
                    links_missing += 1

                by_adset.setdefault(adset_id, []).append(
                    {
                        "ad_id": ad_id,
                        "ad_name": ad_name,
                        "created_time": created_dt,
                        "updated_time": updated_dt,
                        "instagram_url": ig_url or None,
                    }
                )
            except Exception:
                continue

        try:
            logging.getLogger(__name__).info(
                "caller=ads_links aid=%s active_ads_fetched=%s adsets_count=%s links_found=%s links_missing=%s",
                str(account_id),
                int(active_ads_fetched),
                int(len(by_adset)),
                int(links_found),
                int(links_missing),
            )
        except Exception:
            pass

    def _ad_sort_key(ad: dict) -> tuple[int, int, int]:
        dt = ad.get("created_time") or ad.get("updated_time")
        try:
            ts = int(dt.timestamp()) if isinstance(dt, datetime) else 0
        except Exception:
            ts = 0
        try:
            upd = int(ad.get("updated_time").timestamp()) if isinstance(ad.get("updated_time"), datetime) else 0
        except Exception:
            upd = 0
        ad_id = str(ad.get("ad_id") or "")
        ad_int = _try_int(ad_id)
        # Use numeric id DESC as a surrogate if timestamps are missing.
        return (ts, upd, int(ad_int or 0))

    out: list[dict] = []
    for adset_id, ads in by_adset.items():
        ads_sorted = sorted(list(ads or []), key=_ad_sort_key, reverse=True)
        out.append(
            {
                "adset_id": str(adset_id),
                "adset_name": str(adset_name_map.get(str(adset_id)) or str(adset_id)),
                "ads": ads_sorted,
            }
        )

    def _adset_sort_key(adset_row: dict) -> tuple[int, int, int]:
        ads = list(adset_row.get("ads") or [])
        if not ads:
            return (0, 0, 0)
        a0 = ads[0]
        dt = a0.get("created_time") or a0.get("updated_time")
        try:
            ts = int(dt.timestamp()) if isinstance(dt, datetime) else 0
        except Exception:
            ts = 0
        try:
            upd = int(a0.get("updated_time").timestamp()) if isinstance(a0.get("updated_time"), datetime) else 0
        except Exception:
            upd = 0
        ad_id0 = str(a0.get("ad_id") or "")
        return (ts, upd, int(_try_int(ad_id0) or 0))

    out = sorted(out, key=_adset_sort_key, reverse=True)
    return out


def format_instagram_ads_links(items: List[Dict[str, Any]], *, max_chars: int = 3500) -> List[str]:
    """Форматирует список adset'ов в список сообщений Telegram (1 сообщение = 1 adset)."""
    if not items:
        return ["Активной рекламы в Instagram с прямыми ссылками сейчас нет."]

    messages: List[str] = []

    for adset in (items or []):
        adset_name = adset.get("adset_name") or adset.get("adset_id") or "Без названия адсета"
        lines: List[str] = [
            f"Adset: {adset_name}",
        ]

        for ad in (adset.get("ads") or []):
            ad_name = ad.get("ad_name") or ad.get("ad_id") or "(без названия)"
            url = str(ad.get("instagram_url") or "").strip()
            if url:
                line = f"{ad_name} — {url}"
            else:
                line = f"{ad_name} — (нет IG-ссылки, preview недоступен)"
            lines.append(line)

        text = "\n".join(lines).strip()
        if len(text) > int(max_chars):
            clipped = []
            size = 0
            for ln in lines:
                add = ("\n" if clipped else "") + ln
                if size + len(add) > int(max_chars) - 20:
                    clipped.append("… (обрезано)")
                    break
                clipped.append(ln)
                size += len(add)
            text = "\n".join(clipped).strip()
        messages.append(text)
    return messages
