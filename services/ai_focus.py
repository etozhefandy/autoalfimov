import os
import asyncio
import json
import time
from typing import Any, Dict, List, Optional

import requests


print("[ai_focus] loaded from:", __file__)


def _dbg_env() -> None:
    keys = ["DS_FOCUS", "DS_focus", "DS-focus"]
    present = {k: bool(os.getenv(k)) for k in keys}
    print("[ai_focus] env present:", present)


_dbg_env()


# --- DeepSeek config (safe defaults) ---
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_ENDPOINT = os.getenv("DEEPSEEK_ENDPOINT", "/v1/chat/completions")

# Быстрая модель по умолчанию
DEEPSEEK_MODEL_FAST = os.getenv(
    "DEEPSEEK_MODEL_FAST",
    os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
)
# "Думающая" модель — только если явно включим
DEEPSEEK_MODEL_REASON = os.getenv("DEEPSEEK_MODEL_REASON", "deepseek-reasoner")

# Таймауты и ретраи (чтобы не висеть и не ронять бота)
DEEPSEEK_CONNECT_TIMEOUT = float(os.getenv("DEEPSEEK_CONNECT_TIMEOUT", "10"))
DEEPSEEK_READ_TIMEOUT = float(os.getenv("DEEPSEEK_READ_TIMEOUT", "60"))
DEEPSEEK_RETRIES = int(os.getenv("DEEPSEEK_RETRIES", "2"))
DEEPSEEK_BACKOFF_S = float(os.getenv("DEEPSEEK_BACKOFF_S", "2.0"))


def _get_api_key() -> str | None:
    # новый приоритетный ключ
    k = os.getenv("DEEPSEEK_API_KEY")
    if k:
        return k
    # backward compatibility (как было)
    return os.getenv("DS_FOCUS") or os.getenv("DS_focus") or os.getenv("DS-focus")


def deepseek_chat(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    temperature: float = 0.4,
    max_tokens: int = 256,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    api_key = _get_api_key()
    if not api_key:
        raise RuntimeError("DeepSeek API key is missing (DS-focus)")

    url = f"{DEEPSEEK_BASE_URL.rstrip('/')}{DEEPSEEK_ENDPOINT}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "model": model or DEEPSEEK_MODEL_FAST,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    if extra_params:
        payload.update(extra_params)

    # Один запрос + 1 повтор с небольшим backoff, чтобы не висеть бесконечно.
    last_err: Exception | None = None
    for attempt in range(DEEPSEEK_RETRIES):
        t0 = time.time()
        try:
            raw = json.dumps(payload, ensure_ascii=False)
            print(
                "[ai_focus] deepseek_chat start attempt=",
                attempt + 1,
                "len=",
                len(raw),
            )
        except Exception:
            print(
                "[ai_focus] deepseek_chat start attempt=",
                attempt + 1,
                "(len=unknown)",
            )

        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=(DEEPSEEK_CONNECT_TIMEOUT, DEEPSEEK_READ_TIMEOUT),  # connect, read
            )
            elapsed = round(time.time() - t0, 2)
            print(
                "[ai_focus] deepseek_chat status=",
                resp.status_code,
                "elapsed=",
                elapsed,
                "attempt=",
                attempt + 1,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            elapsed = round(time.time() - t0, 2)
            print(
                "[ai_focus] deepseek_chat error=",
                repr(e),
                "elapsed=",
                elapsed,
                "attempt=",
                attempt + 1,
            )
            if attempt < DEEPSEEK_RETRIES - 1:
                time.sleep(DEEPSEEK_BACKOFF_S * (attempt + 1))

    # Если обе попытки не удались — пробрасываем последнюю ошибку.
    assert last_err is not None
    raise last_err


def get_focus_comment(context: Dict[str, Any]) -> str:
    """Вызывает DeepSeek для генерации текста комментария Фокус-ИИ.

    context — произвольный словарь с метриками и описанием ситуации.
    При отсутствии ключа/ошибке API возвращает базовый fallback-комментарий.
    """
    try:
        system_msg = (
            "Ты — помощник-маркетолог для Facebook Ads. Дано краткое резюме метрик "
            "и сравнение периодов. Кратко оцени ситуацию по CPA/заявкам/спенду "
            "и предложи одно-две действия: оставить бюджет, мягко повысить/понизить "
            "примерно на 20%, либо подождать, если спрос в это время суток обычно ниже. "
            "Обращай особое внимание на частоту показов: если частота высокая и результаты ухудшаются, явно напиши, что аудитория может выгорать и нужны новые креативы или перезапуск. "
            "Если передан список объектов (кампании/адсеты/объявления), можешь добавить в JSON поле 'objects' — "
            "массив рекомендаций по объектам формата: "
            "[{""id"":""..."", ""name"":""..."", ""level"":""campaign""|""adset""|""ad"", ""recommendation"":""increase_budget""|""decrease_budget""|""keep""|""check_creatives"", ""suggested_change_percent"":число, ""confidence"":0-100}]."
        )

        user_msg = (
            "Входные данные в формате JSON:\n" + str(context) + "\n"\
            "Сформируй краткий (2-4 предложения) комментарий на русском, без технических деталей API."
        )

        data = deepseek_chat(
            [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.4,
            max_tokens=256,
        )

        choice = (data.get("choices") or [{}])[0]
        msg = (choice.get("message") or {}).get("content")
        if not msg:
            raise ValueError("empty response")
        return msg.strip()
    except RuntimeError:
        return "Фокус-ИИ: нет доступа к ИИ-сервису (не найден API-ключ). Оцени ситуацию по цифрам выше."
    except Exception as e:
        # Логируем ошибку, чтобы видеть причину в Railway-логах.
        print(f"[ai_focus] DeepSeek error: {e}")
        return (
            "Фокус-ИИ временно недоступен (ошибка ИИ-сервиса). "
            "Ориентируйся по изменениям CPA, заявок и спенда в сравнении периодов."
        )


async def ask_deepseek(messages: List[Dict[str, str]], json_mode: bool = False) -> Dict[str, Any]:
    """Асинхронная обёртка вокруг DeepSeek Chat Completions (thinking-mode).

    Принимает список messages в формате OpenAI (role/content) и, опционально,
    включает JSON-режим ответа через response_format.
    """

    api_key = _get_api_key()
    if not api_key:
        print("[ai_focus] ask_deepseek: DeepSeek API key is missing; returning empty result")
        return {"choices": [{"message": {"content": ""}}], "error": "missing_api_key"}

    url = f"{DEEPSEEK_BASE_URL.rstrip('/')}{DEEPSEEK_ENDPOINT}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "model": DEEPSEEK_MODEL_FAST if not json_mode else DEEPSEEK_MODEL_REASON,
        "messages": messages,
    }

    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    def _do_request() -> Dict[str, Any]:
        last_err: Exception | None = None
        for attempt in range(DEEPSEEK_RETRIES):
            t0 = time.time()
            try:
                raw = json.dumps(payload, ensure_ascii=False)
                print(
                    "[ai_focus] ask_deepseek start attempt=",
                    attempt + 1,
                    "len=",
                    len(raw),
                )
            except Exception:
                print(
                    "[ai_focus] ask_deepseek start attempt=",
                    attempt + 1,
                    "(len=unknown)",
                )

            try:
                resp = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=(DEEPSEEK_CONNECT_TIMEOUT, DEEPSEEK_READ_TIMEOUT),  # connect, read
                )
                elapsed = round(time.time() - t0, 2)
                print(
                    "[ai_focus] ask_deepseek status=",
                    resp.status_code,
                    "elapsed=",
                    elapsed,
                    "attempt=",
                    attempt + 1,
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_err = e
                elapsed = round(time.time() - t0, 2)
                print(
                    "[ai_focus] ask_deepseek error=",
                    repr(e),
                    "elapsed=",
                    elapsed,
                    "attempt=",
                    attempt + 1,
                )
                if attempt < DEEPSEEK_RETRIES - 1:
                    time.sleep(DEEPSEEK_BACKOFF_S * (attempt + 1))

        print("[ai_focus] ask_deepseek failed; returning empty result to avoid crashing bot")
        return {"choices": [{"message": {"content": ""}}], "error": str(last_err)}

    return await asyncio.to_thread(_do_request)
