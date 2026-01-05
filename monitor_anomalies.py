from __future__ import annotations

from typing import List

from fb_report.cpa_monitoring import build_anomaly_messages_for_account as _run_monitoring


# DEPRECATED: wrapper for backward compatibility.
# Вся логика мониторинга CPA/аномалий должна быть только в fb_report/cpa_monitoring.py


def detect_adset_anomalies(aid: str):  # pragma: no cover
    raise RuntimeError(
        "monitor_anomalies.detect_adset_anomalies is deprecated; use fb_report.cpa_monitoring instead"
    )


def format_anomaly_message(_anom):  # pragma: no cover
    raise RuntimeError(
        "monitor_anomalies.format_anomaly_message is deprecated; use fb_report.cpa_monitoring instead"
    )


def build_anomaly_messages_for_account(aid: str) -> List[str]:
    return _run_monitoring(aid)
