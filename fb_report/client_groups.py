import json
import os
import shutil
import time
from datetime import datetime

from fb_report.constants import (
    DATA_DIR,
    CLIENT_GROUPS_FILE,
    CLIENT_RATE_LIMITS_FILE,
    SUPERADMIN_USER_ID,
    ALMATY_TZ,
)


def is_superadmin(user_id: int | None) -> bool:
    try:
        return int(user_id or 0) == int(SUPERADMIN_USER_ID)
    except Exception:
        return False


def _atomic_write_json(path: str, obj: dict) -> None:
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
        pass
    os.replace(tmp, path)


def _load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_json(path: str, obj: dict) -> None:
    try:
        _atomic_write_json(path, obj if isinstance(obj, dict) else {})
    except Exception:
        pass


def load_client_groups() -> dict:
    return _load_json(CLIENT_GROUPS_FILE)


def save_client_groups(st: dict) -> None:
    _save_json(CLIENT_GROUPS_FILE, st)


def _ensure_schema(st: dict) -> dict:
    if not isinstance(st, dict):
        st = {}
    groups = st.get("groups")
    if not isinstance(groups, dict):
        groups = {}
    st["groups"] = groups
    return st


def get_group(chat_id: str) -> dict | None:
    st = _ensure_schema(load_client_groups())
    groups = st.get("groups") if isinstance(st.get("groups"), dict) else {}
    g = (groups or {}).get(str(chat_id))
    return g if isinstance(g, dict) else None


def is_client_group(chat_id: str) -> bool:
    return bool(get_group(str(chat_id)) is not None)


def is_active_client_group(chat_id: str) -> bool:
    g = get_group(str(chat_id)) or {}
    return bool(g.get("active") is True)


def list_groups() -> list[tuple[str, dict]]:
    st = _ensure_schema(load_client_groups())
    groups = st.get("groups") if isinstance(st.get("groups"), dict) else {}
    out: list[tuple[str, dict]] = []
    for cid, g in (groups or {}).items():
        if isinstance(g, dict):
            out.append((str(cid), dict(g)))
    out.sort(key=lambda x: x[0])
    return out


def activate_group(*, chat_id: str, title: str, actor_user_id: int | None) -> None:
    st = _ensure_schema(load_client_groups())
    groups = st.get("groups") if isinstance(st.get("groups"), dict) else {}
    cid = str(chat_id)
    cur = (groups or {}).get(cid)
    if not isinstance(cur, dict):
        cur = {
            "active": False,
            "title": str(title or ""),
            "created_by": int(actor_user_id or 0),
            "created_at": int(time.time()),
            "accounts": {},
        }
    cur["active"] = True
    if title:
        cur["title"] = str(title)
    if "accounts" not in cur or not isinstance(cur.get("accounts"), dict):
        cur["accounts"] = {}
    groups[cid] = cur
    st["groups"] = groups
    save_client_groups(st)


def deactivate_group(*, chat_id: str) -> None:
    st = _ensure_schema(load_client_groups())
    groups = st.get("groups") if isinstance(st.get("groups"), dict) else {}
    cid = str(chat_id)
    cur = (groups or {}).get(cid)
    if not isinstance(cur, dict):
        return
    cur["active"] = False
    groups[cid] = cur
    st["groups"] = groups
    save_client_groups(st)


def set_group_account(*, chat_id: str, aid: str, enabled: bool) -> None:
    st = _ensure_schema(load_client_groups())
    groups = st.get("groups") if isinstance(st.get("groups"), dict) else {}
    cid = str(chat_id)
    g = (groups or {}).get(cid)
    if not isinstance(g, dict):
        g = {
            "active": False,
            "title": "",
            "created_by": int(SUPERADMIN_USER_ID),
            "created_at": int(time.time()),
            "accounts": {},
        }
    acc = g.get("accounts") if isinstance(g.get("accounts"), dict) else {}
    acc[str(aid)] = bool(enabled)
    g["accounts"] = acc
    groups[cid] = g
    st["groups"] = groups
    save_client_groups(st)


def toggle_group_account(*, chat_id: str, aid: str) -> bool:
    g = get_group(str(chat_id)) or {}
    acc = g.get("accounts") if isinstance(g.get("accounts"), dict) else {}
    cur = bool(acc.get(str(aid)) is True)
    set_group_account(chat_id=str(chat_id), aid=str(aid), enabled=(not cur))
    return not cur


def enabled_accounts_for_group(chat_id: str) -> list[str]:
    g = get_group(str(chat_id)) or {}
    acc = g.get("accounts") if isinstance(g.get("accounts"), dict) else {}
    out = [str(aid) for aid, v in (acc or {}).items() if v is True]
    out.sort()
    return out


def active_groups_for_account(aid: str) -> list[str]:
    out: list[str] = []
    for cid, g in list_groups():
        if not isinstance(g, dict):
            continue
        if not bool(g.get("active") is True):
            continue
        acc = g.get("accounts") if isinstance(g.get("accounts"), dict) else {}
        if bool((acc or {}).get(str(aid)) is True):
            out.append(str(cid))
    out.sort()
    return out


def _today_key() -> str:
    try:
        return datetime.now(ALMATY_TZ).date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()


def check_rate_limit_and_touch(*, chat_id: str, user_id: int) -> tuple[bool, str]:
    if is_superadmin(user_id):
        return True, ""

    st = _load_json(CLIENT_RATE_LIMITS_FILE)

    today = _today_key()
    k_count = f"{str(chat_id)}:{int(user_id)}:{today}"
    k_last = f"{str(chat_id)}:{int(user_id)}"

    now_ts = int(time.time())

    count_today = 0
    try:
        count_today = int((st.get(k_count) or {}).get("count") or 0)
    except Exception:
        count_today = 0

    last_ts = 0
    try:
        last_ts = int((st.get(k_last) or {}).get("ts") or 0)
    except Exception:
        last_ts = 0

    if last_ts and (now_ts - last_ts) < 300:
        return False, "Лимит запросов. Попробуй позже."

    if count_today >= 10:
        return False, "Лимит запросов. Попробуй позже."

    st[k_last] = {"ts": int(now_ts)}
    st[k_count] = {"count": int(count_today) + 1, "ts": int(now_ts)}
    _save_json(CLIENT_RATE_LIMITS_FILE, st)
    return True, ""
