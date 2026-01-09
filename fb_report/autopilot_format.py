def ap_action_text(action: dict) -> str:
    kind = str(action.get("kind") or "")
    name = action.get("name") or action.get("adset_id")
    reason = action.get("reason") or ""
    sp_t = action.get("spend_today")
    ld_t = action.get("leads_today")
    cpl_t = action.get("cpl_today")
    cpl_3 = action.get("cpl_3d")

    def _fmt_money(v):
        if v is None:
            return "—"
        try:
            return f"{float(v):.2f} $"
        except Exception:
            return "—"

    def _fmt_int(v):
        try:
            return str(int(float(v)))
        except Exception:
            return "0"

    lines = [f"Объект: {name}"]
    lines.append(f"Сегодня: spend {_fmt_money(sp_t)} | leads {_fmt_int(ld_t)} | CPL {_fmt_money(cpl_t)}")
    lines.append(f"Последние 3 дня: CPL {_fmt_money(cpl_3)}")
    lines.append("")

    if kind == "budget_pct":
        pct = action.get("percent")
        try:
            pct_f = float(pct)
        except Exception:
            pct_f = 0.0
        sign = "+" if pct_f >= 0 else ""
        lines.append(f"👉 Предложение: изменить бюджет на {sign}{pct_f:.0f}%")
    elif kind == "pause_adset":
        lines.append("👉 Предложение: остановить adset")
    elif kind == "pause_ad":
        ad_name = action.get("ad_name") or action.get("ad_id")
        lines.append(f"👉 Предложение: отключить объявление ({ad_name})")
    elif kind == "note":
        lines.append("ℹ️ Рекомендация без кнопки применения")
    else:
        lines.append("👉 Предложение: (неизвестно)")

    if reason:
        lines.append(f"Причина: {reason}")

    return "\n".join(lines)
