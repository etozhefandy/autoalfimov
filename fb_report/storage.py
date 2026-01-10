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
    AUTOPILOT_CHAT_ID,
    DEFAULT_REPORT_CHAT,
)


AUTOPILOT_CONFIG_FILE = os.path.join(DATA_DIR, "autopilot_config.json")

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


def get_autopilot_chat_id() -> str | None:
    try:
        with open(AUTOPILOT_CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return None
    cid = (cfg or {}).get("chat_id")
    cid = str(cid) if cid not in (None, "") else ""
    return cid or None


def set_autopilot_chat_id(chat_id: str) -> None:
    cid = str(chat_id or "").strip()
    _atomic_write_json(AUTOPILOT_CONFIG_FILE, {"chat_id": cid})


def resolve_autopilot_chat_id() -> tuple[str, str]:
    """Returns (chat_id, source), where source is one of: storage|env|fallback."""
    cid = get_autopilot_chat_id()
    if cid:
        return str(cid), "storage"
    env = str(AUTOPILOT_CHAT_ID or "").strip()
    if env:
        return env, "env"
    return str(DEFAULT_REPORT_CHAT), "fallback"


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


def _migrate_alerts_schema(store: dict) -> dict:
    """Мягкая миграция структуры alerts к новой схеме.

    - Переносит старый target_cpl в account_cpa, если account_cpa ещё не задан.
    - Устанавливает дефолты для days/freq/ai_enabled, если их нет.
    """

    for aid, row in (store or {}).items():
        if not isinstance(row, dict):
            continue
        alerts = row.get("alerts") or {}
        if not isinstance(alerts, dict):
            alerts = {}

        # Если новая схема ещё не применялась
        if "account_cpa" not in alerts:
            old = float(alerts.get("target_cpl", 0.0) or 0.0)
            if old > 0:
                alerts["account_cpa"] = old
        # Дни недели: по умолчанию все включены, чтобы не ломать старое поведение
        alerts.setdefault(
            "days",
            ["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
        )
        # Частота по умолчанию — 3 раза в день
        alerts.setdefault("freq", "3x")
        # По умолчанию ИИ-анализ включён (как в старой системе с комментариями)
        alerts.setdefault("ai_enabled", True)

        alerts.setdefault("ai_cpa_ads_enabled", False)

        # Adset-уровень CPA-алёртов: по умолчанию пустой словарь.
        alerts.setdefault("adset_alerts", {})
        # Campaign-уровень CPA-алёртов: по умолчанию пустой словарь.
        alerts.setdefault("campaign_alerts", {})
        # Ad-уровень CPA-алёртов: по умолчанию пустой словарь.
        alerts.setdefault("ad_alerts", {})

        # Новое поле silent для ad_alerts: по умолчанию False, если не задано.
        try:
            ad_alerts = alerts.get("ad_alerts") or {}
            if isinstance(ad_alerts, dict):
                for _ad_id, cfg in ad_alerts.items():
                    if isinstance(cfg, dict) and "silent" not in cfg:
                        cfg["silent"] = False
        except Exception:
            pass

        row["alerts"] = alerts
        store[aid] = row

    return store


def _migrate_monitoring_schema(store: dict) -> dict:
    for aid, row in (store or {}).items():
        if not isinstance(row, dict):
            continue

        mon = row.get("monitoring") or {}
        if not isinstance(mon, dict):
            mon = {}

        if "compare_enabled" not in mon:
            mon["compare_enabled"] = bool(row.get("enabled", True))

        row["monitoring"] = mon
        store[aid] = row

    return store


def _migrate_autopilot_schema(store: dict) -> dict:
    for aid, row in (store or {}).items():
        if not isinstance(row, dict):
            continue

        ap = row.get("autopilot") or {}
        if not isinstance(ap, dict):
            ap = {}

        ap.setdefault("mode", "OFF")

        goals = ap.get("goals") or {}
        if not isinstance(goals, dict):
            goals = {}
        goals.setdefault("leads", None)
        goals.setdefault("period", "day")
        goals.setdefault("until", None)
        goals.setdefault("target_cpl", None)
        goals.setdefault("planned_budget", None)
        ap["goals"] = goals

        limits = ap.get("limits") or {}
        if not isinstance(limits, dict):
            limits = {}
        limits.setdefault("max_budget_step_pct", 20)
        limits.setdefault("max_daily_risk_pct", 30)
        limits.setdefault("heatmap_min_interval_minutes", 60)
        limits.setdefault("allow_pause_ads", True)
        limits.setdefault("allow_pause_adsets", False)
        limits.setdefault("allow_redistribute", True)
        limits.setdefault("allow_reenable_ads", False)
        ap["limits"] = limits

        ap.setdefault("campaign_groups", {})
        ap.setdefault("active_group_id", None)
        ap.setdefault("active_group_ids", [])

        active_ids = ap.get("active_group_ids")
        if not isinstance(active_ids, list):
            active_ids = []
        active_ids = [str(x) for x in active_ids if str(x).strip()]
        gid = ap.get("active_group_id")
        if gid and str(gid).strip() and str(gid) not in set(active_ids):
            active_ids.append(str(gid))
        ap["active_group_ids"] = active_ids

        row["autopilot"] = ap
        store[aid] = row

    return store


def _migrate_morning_report_schema(store: dict) -> dict:
    """Мягкая миграция блока morning_report к новой схеме с полем level.

    Новая истина:
    - row["morning_report"]["level"] в одном из значений
      {"OFF", "ACCOUNT", "CAMPAIGN", "ADSET"}.

    Обратная совместимость:
    - если level отсутствует, но есть enabled:
        * enabled == False  -> level = "OFF"
        * enabled == True   -> level = "ACCOUNT" (дефолт)
    - если level отсутствует и есть levels.account/campaigns/adsets, то
      выбираем максимально детализированный включённый уровень:
        * adsets -> "ADSET"
        * campaigns -> "CAMPAIGN"
        * иначе -> "ACCOUNT".
    """

    VALID_LEVELS = {"OFF", "ACCOUNT", "CAMPAIGN", "ADSET"}

    for aid, row in (store or {}).items():
        if not isinstance(row, dict):
            continue

        mr = row.get("morning_report") or {}
        if not isinstance(mr, dict):
            mr = {}

        level_raw = mr.get("level")

        if level_raw is None:
            enabled = mr.get("enabled")
            levels = mr.get("levels") or {}

            if enabled is False:
                level = "OFF"
            else:
                # Если явно включены уровни, берём максимально детализированный.
                if bool(levels.get("adsets")):
                    level = "ADSET"
                elif bool(levels.get("campaigns")):
                    level = "CAMPAIGN"
                else:
                    # enabled == True или не задан → считаем, что нужен аккаунт.
                    level = "ACCOUNT"
        else:
            level = str(level_raw).upper()

        if level not in VALID_LEVELS:
            level = "ACCOUNT"

        mr["level"] = level
        row["morning_report"] = mr
        store[aid] = row

    return store


def load_accounts() -> dict:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    # Мягко мигрируем alerts к новой схеме при каждом чтении
    store = _migrate_alerts_schema(data)

    # Мягкая миграция схемы morning_report к полю level
    store = _migrate_morning_report_schema(store)
    store = _migrate_autopilot_schema(store)
    store = _migrate_monitoring_schema(store)
    return store


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
                # Новая расширенная схема alerts. Старые поля (target_cpl, enabled)
                # остаются для обратной совместимости.
                "alerts": {
                    "enabled": False,
                    "target_cpl": 0.0,
                    "account_cpa": 0.0,
                    "days": [
                        "mon",
                        "tue",
                        "wed",
                        "thu",
                        "fri",
                        "sat",
                        "sun",
                    ],
                    "freq": "3x",
                    "ai_enabled": True,
                    # Adset-уровень CPA-алёртов: по умолчанию пусто.
                    "adset_alerts": {},
                    # Campaign-уровень CPA-алёртов: по умолчанию пусто.
                    "campaign_alerts": {},
                    # Ad-уровень CPA-алёртов: по умолчанию пусто.
                    "ad_alerts": {},
                },
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


def get_lead_metric_for_account(aid: str) -> dict | None:
    store = load_accounts() or {}
    row = store.get(str(aid)) or {}
    sel = row.get("lead_metric")
    if not sel:
        return None
    if isinstance(sel, dict):
        action_type = (sel.get("action_type") or "").strip()
        label = (sel.get("label") or "").strip()
        if action_type:
            return {
                "action_type": action_type,
                "label": label or action_type,
            }
        return None
    action_type = str(sel).strip()
    return {"action_type": action_type, "label": action_type} if action_type else None


def set_lead_metric_for_account(aid: str, *, action_type: str, label: str | None = None) -> None:
    at = (action_type or "").strip()
    if not at:
        return
    store = load_accounts() or {}
    row = store.get(str(aid)) or {}
    row["lead_metric"] = {
        "action_type": at,
        "label": (label or "").strip() or at,
    }
    row["metrics"] = row.get("metrics") or {}
    row["metrics"]["leads"] = True
    store[str(aid)] = row
    save_accounts(store)


def clear_lead_metric_for_account(aid: str) -> None:
    store = load_accounts() or {}
    row = store.get(str(aid)) or {}
    if "lead_metric" in row:
        row.pop("lead_metric", None)
    store[str(aid)] = row
    save_accounts(store)


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
