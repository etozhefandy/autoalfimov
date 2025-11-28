# services/storage.py

import os
import json
import shutil
from datetime import datetime
from typing import Any, Dict

from config import (
    DATA_DIR,
    INSIGHTS_DIR,
    ACCOUNTS_JSON,
    REPORT_CACHE_FILE,
    SYNC_META_FILE,
    REPORT_CACHE_TTL,
    ALMATY_TZ,
)

# ========= БАЗОВЫЙ JSON I/O =========

def _atomic_write_json(path: str, obj: Any) -> None:
    """
    Безопасная запись JSON:
    - пишет во временный файл
    - делает fsync
    - атомарно заменяет оригинал
    - старый файл сохраняет в .bak (если был)
    """
    tmp = f"{path}.tmp"
    bak = f"{path}.bak"

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    try:
        if os.path.exists(path):
            shutil.copy2(path, bak)
    except Exception:
        # бэкап не обязателен для работы
        pass

    os.replace(tmp, path)


# ========= ACCOUNTS.JSON И МЕТА =========

def ensure_accounts_file(repo_accounts_path: str | None = None) -> None:
    """
    Гарантирует наличие ACCOUNTS_JSON в DATA_DIR.
    Если файла нет:
    - пытается скопировать repo_accounts_path (обычно ./accounts.json рядом с fb_report.py)
    - если и его нет, создаёт пустой словарь.
    """
    if os.path.exists(ACCOUNTS_JSON):
        return

    if repo_accounts_path and os.path.exists(repo_accounts_path):
        try:
            os.makedirs(os.path.dirname(ACCOUNTS_JSON), exist_ok=True)
            shutil.copy2(repo_accounts_path, ACCOUNTS_JSON)
            return
        except Exception:
            # если не удалось скопировать — просто создадим пустой файл ниже
            pass

    _atomic_write_json(ACCOUNTS_JSON, {})


def load_accounts() -> Dict[str, Any]:
    """Читает accounts.json, при ошибке возвращает пустой словарь."""
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_accounts(data: Dict[str, Any]) -> None:
    """Сохраняет словарь в accounts.json атомарно."""
    _atomic_write_json(ACCOUNTS_JSON, data)


def load_sync_meta() -> Dict[str, Any]:
    """Метаданные синка из BM (время последней синхронизации и т.п.)."""
    try:
        with open(SYNC_META_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_sync_meta(meta: Dict[str, Any]) -> None:
    """Сохраняет sync_meta.json."""
    _atomic_write_json(SYNC_META_FILE, meta)


def human_last_sync() -> str:
    """
    Возвращает человекочитаемую строку последнего синка BM.
    Используется в главном меню.
    """
    meta = load_sync_meta()
    iso = meta.get("last_sync")
    if not iso:
        return "нет данных"
    try:
        dt = datetime.fromisoformat(iso)
        if not dt.tzinfo:
            dt = ALMATY_TZ.localize(dt)
        dt = dt.astimezone(ALMATY_TZ)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "нет данных"


# ========= ЛОКАЛЬНЫЙ КЭШ ИНСАЙТОВ =========

def _insight_file(aid: str) -> str:
    """
    Путь к JSON-файлу с инсайтами для конкретного аккаунта.
    INSIGHTS_DIR уже создан в config.py.
    """
    safe = str(aid).replace("act_", "")
    return os.path.join(INSIGHTS_DIR, f"{safe}.json")


def load_local_insights(aid: str) -> Dict[str, Any]:
    """
    Читает локальный файл с инсайтами аккаунта.
    Структура:
    {
      "<period_key>": { ... raw insight dict ... } или None
    }
    """
    path = _insight_file(aid)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_local_insights(aid: str, data: Dict[str, Any]) -> None:
    """
    Атомарно сохраняет локальный файл с инсайтами аккаунта.
    """
    path = _insight_file(aid)
    tmp = f"{path}.tmp"

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp, path)


# ========= ТЕКСТОВЫЙ КЭШ ОТЧЁТОВ =========

def period_key(period: Any) -> str:
    """
    Генерирует строковый ключ периода для кэша:
    - если period dict с since/until → range:YYYY-MM-DD:YYYY-MM-DD
    - иначе → preset:<value> (например, 'today', 'yesterday')
    """
    if isinstance(period, dict):
        since = period.get("since", "")
        until = period.get("until", "")
        return f"range:{since}:{until}"
    return f"preset:{str(period)}"


def load_report_cache() -> Dict[str, Any]:
    """
    Читает report_cache.json.
    Структура:
    {
      "<aid>": {
        "<period_key>": {
          "text": "...готовый текст отчёта...",
          "ts": 1710000000.0   # unix timestamp
        }
      }
    }
    """
    try:
        with open(REPORT_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_report_cache(cache: Dict[str, Any]) -> None:
    """Сохраняет report_cache.json атомарно."""
    _atomic_write_json(REPORT_CACHE_FILE, cache)


def get_cached_report_entry(aid: str, key: str) -> Dict[str, Any] | None:
    """
    Возвращает entry из кэша по аккаунту и ключу периода,
    либо None, если нет записи.
    """
    cache = load_report_cache()
    return cache.get(aid, {}).get(key)


def set_cached_report_entry(aid: str, key: str, text: str) -> None:
    """
    Обновляет/создаёт запись в кэше отчётов и сохраняет её.
    """
    now_ts = datetime.now(ALMATY_TZ).timestamp()
    cache = load_report_cache()
    cache.setdefault(aid, {})
    cache[aid][key] = {"text": text, "ts": now_ts}
    save_report_cache(cache)


def is_cache_fresh(entry: Dict[str, Any] | None) -> bool:
    """
    Проверяет, не устарел ли элемент кэша.
    Использует REPORT_CACHE_TTL из config.py.
    """
    if not entry:
        return False
    try:
        ts = float(entry.get("ts", 0))
    except Exception:
        return False

    now_ts = datetime.now(ALMATY_TZ).timestamp()
    return (now_ts - ts) <= REPORT_CACHE_TTL
