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
