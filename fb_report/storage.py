# fb_report/storage.py
import json
import os
import shutil
from datetime import datetime

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User

from .constants import (
    DATA_DIR,
    ACCOUNTS_JSON,
    REPO_ACCOUNTS_JSON,
    SYNC_META_FILE,
    ACCOUNT_NAMES,
    EXCLUDED_AD_ACCOUNT_IDS,
    EXCLUDED_NAME_KEYWORDS,
    AD_ACCOUNTS_FALLBACK,
    ALMATY_TZ,
)

# ====== низкоуровневые операции с файлами ======


def _atomic_write_json(path: str, obj: dict):
    tmp = f"{path}.tmp"
    bak = f"{path}.bak"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    try:
        if os.path.exists(path):
            shutil.copy2(path, bak)
    except Exception:
        pass
    os.replace(tmp, path)


def _ensure_accounts_file():
    if not os.path.exists(ACCOUNTS_JSON):
        if os.path.exists(REPO_ACCOUNTS_JSON):
            try:
                shutil.copy2(REPO_ACCOUNTS_JSON, ACCOUNTS_JSON)
                return
            except Exception:
                pass
        _atomic_write_json(ACCOUNTS_JSON, {})


_ensure_accounts_file()

# ========= STORES / META ==========


def load_accounts() -> dict:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_accounts(d: dict):
    _atomic_write_json(ACCOUNTS_JSON, d)


def load_sync_meta() -> dict:
    try:
        with open(SYNC_META_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_sync_meta(d: dict):
    _atomic_write_json(SYNC_META_FILE, d)


def human_last_sync() -> str:
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


def _norm_act(aid: str) -> str:
    aid = str(aid).strip()
    return aid if aid.startswith("act_") else "act_" + aid


def get_account_name(aid: str) -> str:
    store = load_accounts()
    if aid in store and store[aid].get("name"):
        return store[aid]["name"]
    return ACCOUNT_NAMES.get(aid, aid)


def get_enabled_accounts_in_order() -> list[str]:
    """
    Для отчётов и фоновых джобов:
    - сначала все включённые аккаунты,
    - потом выключенные (чтобы были внизу списков).
    """
    store = load_accounts()
    if not store:
        return AD_ACCOUNTS_FALLBACK
    enabled = [acc for acc, row in store.items() if row.get("enabled", True)]
    disabled = [acc for acc, row in store.items() if not row.get("enabled", True)]
    ordered = enabled + disabled
    return ordered or AD_ACCOUNTS_FALLBACK


def iter_enabled_accounts_only():
    """Итерируем только включённые аккаунты (enabled=True)."""
    store = load_accounts()
    ids = get_enabled_accounts_in_order()
    if not store:
        # если нет конфига, считаем все аккаунты включёнными (fallback)
        for aid in ids:
            yield aid
        return
    for aid in ids:
        if store.get(aid, {}).get("enabled", True):
            yield aid


def looks_excluded(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_NAME_KEYWORDS)


def upsert_from_bm() -> dict:
    """
    Добавляет новые аккаунты и обновляет ИМЕНА.
    Настройки enabled/metrics/alerts не затирает.
    Также сохраняет время последней синхронизации.
    """
    store = load_accounts()
    me = User(fbid="me")
    fetched = list(me.get_ad_accounts(fields=["account_id", "name", "account_status"]))
    added, updated, skipped = 0, 0, 0
    for it in fetched:
        aid = _norm_act(it.get("account_id"))
        name = it.get("name") or aid
        if aid in EXCLUDED_AD_ACCOUNT_IDS or looks_excluded(name):
            skipped += 1
            continue
        ACCOUNT_NAMES.setdefault(aid, name)
        if aid in store:
            if name and store[aid].get("name") != name:
                store[aid]["name"] = name
                updated += 1
        else:
            store[aid] = {
                "name": name,
                "enabled": True,
                "metrics": {"messaging": True, "leads": False},
                "alerts": {"enabled": False, "target_cpl": 0.0},
            }
            added += 1
    save_accounts(store)

    last_sync_iso = datetime.now(ALMATY_TZ).isoformat()
    meta = load_sync_meta()
    meta["last_sync"] = last_sync_iso
    save_sync_meta(meta)

    return {
        "added": added,
        "updated": updated,
        "skipped": skipped,
        "total": len(store),
        "last_sync": last_sync_iso,
    }


def metrics_flags(aid: str) -> dict:
    st = load_accounts().get(aid, {})
    m = st.get("metrics", {}) or {}
    return {
        "messaging": bool(m.get("messaging", False)),
        "leads": bool(m.get("leads", False)),
    }


def is_active(aid: str) -> bool:
    try:
        st = AdAccount(aid).api_get(fields=["account_status"])["account_status"]
        return st == 1
    except Exception:
        return False


# ========= Настройки Фокус-ИИ =========


def get_focus_for_account(aid: str) -> dict:
    """Возвращает словарь focus для аккаунта (по всем пользователям)."""
    store = load_accounts()
    row = store.get(aid) or {}
    return row.get("focus") or {}


def save_focus_for_account(aid: str, focus: dict) -> None:
    """Сохраняет словарь focus для аккаунта в accounts.json."""
    store = load_accounts()
    row = store.get(aid) or {}
    row["focus"] = focus
    store[aid] = row
    save_accounts(store)


def user_has_focus_settings(user_id: str) -> bool:
    """Проверяет, есть ли у пользователя включённые и активные таргеты Фокус-ИИ.

    Проходит по всем аккаунтам в accounts.json и ищет focus[user_id]
    с enabled=True и хотя бы одним target с active!=False.
    """
    uid = str(user_id)
    store = load_accounts()
    for row in store.values():
        focus = row.get("focus") or {}
        u = focus.get(uid)
        if not u or not u.get("enabled", False):
            continue
        targets = u.get("targets") or []
        for t in targets:
            if t is None:
                continue
            if t.get("active", True):
                return True
    return False


def disable_focus_target(aid: str, user_id: str, level: str, object_id: str) -> None:
    """Помечает конкретный target Фокус-ИИ как неактивный (active=False)."""
    uid = str(user_id)
    focus = get_focus_for_account(aid)
    u = focus.get(uid) or {}
    targets = u.get("targets") or []
    for t in targets:
        if (
            t.get("level") == level
            and t.get("object_id") == object_id
            and t.get("active", True)
        ):
            t["active"] = False
    u["targets"] = targets
    focus[uid] = u
    save_focus_for_account(aid, focus)
