# history_store.py - лог истории для "часов пик"

import os
import json
from datetime import datetime, timedelta

DATA_DIR = os.getenv("DATA_DIR", "/data")
HISTORY_FILE = os.path.join(DATA_DIR, "history.jsonl")
HISTORY_MAX_AGE_DAYS = int(os.getenv("HISTORY_MAX_AGE_DAYS", "365"))


def _ensure_dir():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception:
        pass


def append_snapshot(account_id: str, spend: float, msgs: int, leads: int, ts=None):
    """
    Добавляет строку-замер в history.jsonl.
    ts — datetime, если None, берём utcnow.
    """
    _ensure_dir()
    if ts is None:
        ts = datetime.utcnow()
    rec = {
        "ts": ts.isoformat(),
        "account_id": account_id,
        "spend": float(spend),
        "msgs": int(msgs),
        "leads": int(leads),
    }
    line = json.dumps(rec, ensure_ascii=False)
    try:
        with open(HISTORY_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # молча игнорируем, чтобы не ломать основной поток
        return


def prune_old_history(max_age_days: int = HISTORY_MAX_AGE_DAYS):
    """
    Оставляет только записи за последние max_age_days (по ts).
    """
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return
    except Exception:
        return

    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    new_lines = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        try:
            rec = json.loads(ln)
            ts_str = rec.get("ts")
            if not ts_str:
                continue
            ts = datetime.fromisoformat(ts_str.split("Z")[0])
        except Exception:
            continue
        if ts >= cutoff:
            new_lines.append(ln + "\n")

    try:
        _ensure_dir()
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
    except Exception:
        return
