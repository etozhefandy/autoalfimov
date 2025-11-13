# billing_watch.py
# Модуль фонового мониторинга биллингов Facebook Ads

import os, json, math
from datetime import datetime, timedelta
from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount
from telegram.ext import ContextTypes

ALMATY_TZ = timezone("Asia/Almaty")
STATE_FILE = "/data/billing_state.json"

# === Вспомогательные функции работы с JSON ===
def _load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def _save_state(d: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

# === Хелперы для баланса и прогноза ===
def _fetch_balance(aid: str):
    info = AdAccount(aid).api_get(fields=["name", "account_status", "balance"])
    name = info.get("name", aid)
    status = int(info.get("account_status", 0))
    balance = float(info.get("balance", 0) or 0) / 100.0
    return name, status, balance

def _avg_daily_spend(aid: str, lookback_days: int = 7):
    until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
    since = until - timedelta(days=lookback_days - 1)
    acc = AdAccount(aid)
    data = acc.get_insights(fields=["spend"], params={
        "time_range": {"since": since.strftime("%Y-%m-%d"), "until": until.strftime("%Y-%m-%d")},
    })
    total = sum(float(r.get("spend", 0) or 0) for r in data)
    return (total / lookback_days) if total > 0 else 0.0

def _forecast_days_left(balance_usd, avg_daily):
    if avg_daily <= 0:
        return None
    # твоя логика: порог - 1.5 дня и округление вниз
    return math.floor(balance_usd / avg_daily - 1.5)

def _fmt_kzt(n: float) -> str:
    try:
        return f"{int(round(n)):,}".replace(",", " ")
    except:
        return "0"

# === Основной процесс мониторинга (каждые 15 минут) ===
async def billing_check_job(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Ждёт всё из context.job.data:
      get_enabled_accounts, get_account_name, usd_to_kzt, kzt_round_up_1000,
      owner_id, group_chat_id
    """
    jd = ctx.job.data or {}
    get_enabled_accounts = jd["get_enabled_accounts"]
    get_account_name = jd["get_account_name"]
    usd_to_kzt = jd["usd_to_kzt"]
    kzt_round_up_1000 = jd["kzt_round_up_1000"]
    group_chat_id = jd["group_chat_id"]

    rate = usd_to_kzt()
    state = _load_state()
    now_ts = datetime.now(ALMATY_TZ).timestamp()

    for aid in get_enabled_accounts():
        try:
            name, status, balance = _fetch_balance(aid)
        except Exception:
            continue

        # следим за активными кабинетами
        if status != 1:
            # если раньше фиксировали “минус” — почистим
            if aid in state:
                del state[aid]
                _save_state(state)
            continue

        kzt = kzt_round_up_1000(balance * rate)

        # событие биллинга (уход в минус)
        if balance < 0 and aid not in state:
            state[aid] = {"first_ts": now_ts}
            _save_state(state)
            txt = (
                f"🚨 У аккаунта <b>{name}</b> биллинг!\n"
                f"Сумма неудавшегося списания: {abs(balance):.2f} $ / {_fmt_kzt(abs(kzt))} ₸\n\n"
                "Подожди, не отправляй заказчику — баланс уточнится через ~20 минут."
            )
            await ctx.bot.send_message(chat_id=group_chat_id, text=txt, parse_mode="HTML")
            # планируем уточнение через 20 минут
            ctx.job_queue.run_once(
                billing_recheck_job,
                when=20 * 60,
                data={
                    "aid": aid,
                    "rate": rate,
                    "get_account_name": get_account_name,
                    "kzt_round_up_1000": kzt_round_up_1000,
                    "group_chat_id": group_chat_id,
                },
            )
            continue

        # баланс восстановился — очищаем состояние (если было)
        if balance >= 0 and aid in state:
            del state[aid]
            _save_state(state)

# === Повторная проверка через 20 минут ===
async def billing_recheck_job(ctx: ContextTypes.DEFAULT_TYPE):
    jd = ctx.job.data or {}
    aid = jd.get("aid")
    rate = jd.get("rate")
    get_account_name = jd.get("get_account_name")
    kzt_round_up_1000 = jd.get("kzt_round_up_1000")
    group_chat_id = jd.get("group_chat_id")

    if not aid or rate is None or not group_chat_id:
        return

    try:
        name, status, balance = _fetch_balance(aid)
    except Exception:
        return

    # если долг уже погашен — просто чистим и уходим
    if balance >= 0:
        st = _load_state()
        if aid in st:
            del st[aid]
            _save_state(st)
        return

    kzt = kzt_round_up_1000(balance * rate)
    txt = (
        f"🔁 Уточнённый долг по аккаунту <b>{name}</b>:\n"
        f"Текущий баланс: {balance:.2f} $ / {_fmt_kzt(kzt)} ₸\n\n"
        f"💬 Отправь заказчику:\n"
        f"«Нужно пополнить рекламный кабинет на {abs(balance):.0f}–{abs(balance)*1.15:.0f} $ "
        f"(~{_fmt_kzt(abs(kzt))}–{_fmt_kzt(abs(kzt)*1.15)} ₸) для продолжения работы рекламы.»"
    )
    await ctx.bot.send_message(chat_id=group_chat_id, text=txt, parse_mode="HTML")

    st = _load_state()
    if aid in st:
        del st[aid]
        _save_state(st)

# === Инициализация модуля ===
def init_billing_watch(app, *,
                       get_enabled_accounts,
                       get_account_name,
                       usd_to_kzt,
                       kzt_round_up_1000,
                       owner_id: int,
                       group_chat_id: str):
    """Подключаем фоновые проверки биллингов"""
    app.job_queue.run_repeating(
        billing_check_job,
        interval=900,  # каждые 15 мин
        first=15,
        data={
            "get_enabled_accounts": get_enabled_accounts,
            "get_account_name": get_account_name,
            "usd_to_kzt": usd_to_kzt,
            "kzt_round_up_1000": kzt_round_up_1000,
            "owner_id": owner_id,
            "group_chat_id": group_chat_id,
        },
    )
