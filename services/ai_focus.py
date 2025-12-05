import os
from typing import Any, Dict, List, Optional

import requests

DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_ENDPOINT = os.getenv("DEEPSEEK_ENDPOINT", "/v1/chat/completions")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")


def _get_api_key() -> str | None:
    return os.getenv("DS-focus")


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
        "model": model or DEEPSEEK_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    if extra_params:
        payload.update(extra_params)

    resp = requests.post(url, headers=headers, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()


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
            "примерно на 20%, либо подождать, если спрос в это время суток обычно ниже."
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
    except Exception:
        return (
            "Фокус-ИИ временно недоступен (ошибка ИИ-сервиса). "
            "Ориентируйся по изменениям CPA, заявок и спенда в сравнении периодов."
        )
