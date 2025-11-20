# billing_watch.py
import os
import json
from datetime import datetime
from pytz import timezone

from facebook_business.adobjects.adaccount import AdAccount
from telegram.error import BadRequest

# Таймзона как в основном боте
ALMATY_TZ = timezone("Asia/Almaty")

# Хранение состояния в volume /data
DATA_DIR = os.getenv("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
STATE_FILE = os.path.join(DATA_DIR, "billing_state.json")


def _load_state() -> dict:
    """Читаем состояние биллингов из файла."""
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(d: dict):
    """Атомарно сохраняем состояние в файл."""
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)


async def _safe_send(bot, chat_id: str, text: str, **kwargs):
    """Отправка сообщений с игнором нефатальных BadRequest."""
    try:
        await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except BadRequest as e:
        # Чтобы биллинговый вотчер не падал из-за проблем с сообщением
        print("[billing_watch] send_message error:", e)


async def _billing_poll_job(
    ctx,
    get_enabled_accounts,
    get_account_name,
    usd_to_kzt,
    kzt_round_up_1000,
    owner_id: int,
    group_chat_id: str | None,
):
    """
    Периодический опрос аккаунтов:
    - читаем прошлые статусы из файла
    - смотрим текущие статусы из Ads API
    - если было 1, стало !=1 — шлём алерт
    - сохраняем новые статусы обратно в файл
    """
    bot = ctx.bot
    now = datetime.now(ALMATY_TZ)
    rate = usd_to_kzt()

    state = _load_state()

    for aid in get_enabled_accounts():
        prev = state.get(aid, {})
        prev_status = prev.get("status")
        # prev_balance = float(prev.get("balance_usd", 0) or 0)  # пока не нужен, но оставим как шаблон

        try:
            info = AdAccount(aid).api_get(
                fields=["name", "account_status", "balance"]
            )
        except Exception as e:
            print(f"[billing_watch] error fetch {aid}: {e}")
            continue

        status = info.get("account_status")
        balance_usd = float(info.get("balance", 0) or 0) / 100.0
        name = info.get("name") or get_account_name(aid)

        # Обновляем состояние
        state[aid] = {
            "status": status,
            "balance_usd": balance_usd,
            "updated_at": now.isoformat(),
        }

        # Интересует именно переход 1 -> !=1 (ушёл в биллинг/блок из активного)
        if prev_status == 1 and status != 1:
            kzt = kzt_round_up_1000(balance_usd * rate)
            kzt_str = f"{int(kzt):,}".replace(",", " ")

            text = (
                f"⚠️ <b>Биллинг по {name}</b>\n"
                f"Статус кабинета изменился: 1 → {status}\n"
                f"Текущий баланс: {balance_usd:.2f} $  |  🇰🇿 {kzt_str} ₸"
            )

            # Личка владельцу
            await _safe_send(bot, str(owner_id), text, parse_mode="HTML")
            # Групповой чат, если задан
            if group_chat_id:
                await _safe_send(
                    bot, str(group_chat_id), text, parse_mode="HTML"
                )

    _save_state(state)


def init_billing_watch(
    app,
    get_enabled_accounts,
    get_account_name,
    usd_to_kzt,
    kzt_round_up_1000,
    owner_id: int,
    group_chat_id: str | None = None,
):
    """
    Вызывается из fb_report.build_app().

    Пример вызова (у тебя уже есть в fb_report.py):

        init_billing_watch(
            app,
            get_enabled_accounts=get_enabled_accounts_in_order,
            get_account_name=get_account_name,
            usd_to_kzt=usd_to_kzt,
            kzt_round_up_1000=kzt_round_up_1000,
            owner_id=253181449,
            group_chat_id=str(DEFAULT_REPORT_CHAT),
        )
    """

    async def job_wrapper(context):
        await _billing_poll_job(
            context,
            get_enabled_accounts,
            get_account_name,
            usd_to_kzt,
            kzt_round_up_1000,
            owner_id,
            group_chat_id,
        )

    # Проверяем биллинг каждые 5 минут
    app.job_queue.run_repeating(
        job_wrapper,
        interval=300,  # 5 минут
        first=10,
        name="billing_watch_poll",
    )
