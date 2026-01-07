# history_store.py
import os
import json
from datetime import datetime, timedelta
from pytz import timezone

ALMATY_TZ = timezone("Asia/Almaty")

# Папка для истории
DATA_DIR = os.getenv("DATA_DIR", "/data")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
os.makedirs(HISTORY_DIR, exist_ok=True)


def _history_file_for(aid: str) -> str:
    """Формирует путь к файлу истории для аккаунта."""
    safe = aid.replace("act_", "")
    return os.path.join(HISTORY_DIR, f"history_{safe}.jsonl")


def append_snapshot(aid: str, spend: float, msgs: int, leads: int, ts: datetime):
    """
    Добавляет строку в историю:
    {
      "ts": "...",
      "spend": ...,
      "msgs": ...,
      "leads": ...
    }
    """
    path = _history_file_for(aid)
    row = {
        "ts": ts.isoformat(),
        "spend": float(spend),
        "msgs": int(msgs),
        "leads": int(leads),
    }

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def prune_old_history(max_age_days: int = 365):
    """
    Удаляет записи старше max_age_days.
    Делается раз в сутки внутри cpa_alerts_job.
    """
    cutoff = datetime.now(ALMATY_TZ) - timedelta(days=max_age_days)

    for fname in os.listdir(HISTORY_DIR):
        if not fname.startswith("history_"):
            continue

        full = os.path.join(HISTORY_DIR, fname)
        if not os.path.isfile(full):
            continue

        tmp = full + ".tmp"
        with open(full, "r", encoding="utf-8") as fin, open(tmp, "w", encoding="utf-8") as fout:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                try:
                    dt = datetime.fromisoformat(obj.get("ts", ""))
                except Exception:
                    continue

                if dt >= cutoff:
                    fout.write(line + "\n")

        os.replace(tmp, full)


def _autopilot_file_for(aid: str) -> str:
    safe = aid.replace("act_", "")
    return os.path.join(HISTORY_DIR, f"autopilot_{safe}.jsonl")


def append_autopilot_event(aid: str, event: dict, ts: datetime | None = None):
    if not aid:
        return

    if ts is None:
        ts = datetime.now(ALMATY_TZ)

    try:
        payload = dict(event or {})
    except Exception:
        payload = {}
    payload["ts"] = ts.isoformat()

    path = _autopilot_file_for(str(aid))
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_autopilot_events(aid: str, limit: int = 20) -> list[dict]:
    if not aid:
        return []

    path = _autopilot_file_for(str(aid))
    if not os.path.exists(path):
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.readlines() if ln.strip()]
    except Exception:
        return []

    out: list[dict] = []
    for ln in reversed(lines[-max(limit, 0) :]):
        try:
            out.append(json.loads(ln))
        except Exception:
            continue
    return out
