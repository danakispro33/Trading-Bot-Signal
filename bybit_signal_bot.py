import time
import json
import threading
import os
import re
import hashlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
import math

import ccxt
import pandas as pd
import requests


# ================== ТВОИ ДАННЫЕ ==================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = 5878255923

# Bybit API (для просто сигналов можно оставить пустым)
BYBIT_API_KEY = ""
BYBIT_API_SECRET = ""


# ================== НАСТРОЙКИ ==================
SYMBOLS = [
    "BTC/USDT:USDT",
    "XRP/USDT:USDT",
    "SOL/USDT:USDT",
    "ETH/USDT:USDT",
    "BNB/USDT:USDT",
    "AVAX/USDT:USDT",
    "LINK/USDT:USDT",
    "NEAR/USDT:USDT",
    "DOT/USDT:USDT",
    "XLM/USDT:USDT",
]

TIMEFRAME = "15m"
CHECK_EVERY_SECONDS = 60
COOLDOWN_MINUTES = 90
MIN_CONFIDENCE = 62

# News system config
NEWS_ENABLED = True
NEWS_POLL_SECONDS = 90
NEWS_HTTP_TIMEOUT = 12
NEWS_MAX_ITEMS_PER_POLL = 50
NEWS_STORE_LIMIT = 100
NEWS_SHOW_LIMIT = 10
NEWS_SEEN_LIMIT = 2000
NEWS_IMPORTANCE_THRESHOLD = 55
NEWS_URGENT_THRESHOLD = 80
NEWS_PRICE_CHECK_ENABLED = True
NEWS_PRICE_CHECK_MIN_IMPORTANCE = 65
NEWS_PRICE_CHECK_COOLDOWN_SEC = 180
NEWS_SOURCES = {"cryptopanic": True, "rss": True, "gdelt": False}

CRYPTOPANIC_TOKEN = os.environ.get("CRYPTOPANIC_TOKEN", "")
CRYPTOPANIC_ENDPOINT = "https://cryptopanic.com/api/v2/posts/"
RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://cointelegraph.com/rss",
]
GDELT_DOC_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"

# Engine v2 config
ENGINE_V2_ENABLED = True
ENGINE_V2_USE_MTF = True
ENGINE_V2_USE_LIVE_TRIGGER = True
ENGINE_V2_SETUP_ENABLED = True
ENGINE_V2_ENTRY_ENABLED = True

# Timeframes
BASE_TIMEFRAME = TIMEFRAME
HIGHER_TIMEFRAME = "1h"
LOWER_TIMEFRAME = "5m"

# Setup logic
SETUP_DISTANCE_PCT = 0.25
SETUP_MIN_SCORE = 0.55
SETUP_COOLDOWN_MINUTES = 30
SETUP_TTL_MINUTES = 120

# Trigger logic
TRIGGER_BUFFER_PCT = 0.03
TRIGGER_CONFIRM_MODE = "retest"
TRIGGER_RETEST_MAX_BARS = 6
TRIGGER_MOMENTUM_ATR_MULT = 0.35

# Filters
FILTER_MIN_ADX_SETUP = 16
FILTER_MIN_ADX_ENTRY = 18
FILTER_MIN_VOL_RATIO_SETUP = 1.05
FILTER_MIN_VOL_RATIO_ENTRY = 1.15
FILTER_RSI_MAX_SETUP = 72
FILTER_RSI_MIN_SETUP = 28

# Risk mgmt
RISK_ATR_MULT_SL = 1.6
RISK_MIN_RR = 1.6
RISK_MAX_SL_PCT = 2.5

# Regime thresholds
REGIME_ATR_PCT_HIGH = 1.6
REGIME_ATR_PCT_LOW = 0.6
REGIME_CHOP_ADX_MAX = 14

# Scoring weights
W_TREND = 0.25
W_ADX = 0.20
W_VOL = 0.20
W_PROXIMITY = 0.20
W_PATTERN = 0.15

STATE_FILE = "state.json"
state_lock = threading.Lock()
run_now_request = {"chat_id": None}
_OHLCV_CACHE: Dict[Tuple[str, str], Dict[str, object]] = {}
_LAST_RL_LOG_TS = 0.0


# ================== TELEGRAM ==================
def tg_send(
    text: str,
    chat_id: Optional[int] = None,
    reply_markup: Optional[Dict] = None,
) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id or TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            print(f"[TG] sendMessage failed: {r.status_code} {r.text}")
            return False
        return True
    except Exception as e:
        print(f"[TG] sendMessage exception: {e}")
        return False


def tg_edit_message(
    text: str,
    chat_id: int,
    message_id: int,
    reply_markup: Optional[Dict] = None,
) -> bool:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            print(f"[TG] editMessageText failed: {r.status_code} {r.text}")
            return False
        return True
    except Exception as e:
        print(f"[TG] editMessageText exception: {e}")
        return False


def tg_get_updates(offset: int) -> List[Dict]:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    r = requests.get(url, params={"offset": offset, "timeout": 15}, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")
    data = r.json()
    return data.get("result", [])


def tg_answer_callback(callback_id: str) -> None:
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
        requests.post(url, json={"callback_query_id": callback_id}, timeout=10)
    except Exception as e:
        print(f"[TG] answerCallbackQuery exception: {e}")


def main_keyboard() -> Dict:
    return {
        "keyboard": [
            [{"text": "📊 Статус"}, {"text": "⚡ Сейчас"}],
            [{"text": "📌 Сигналы"}, {"text": "🎯 Confidence"}],
            [{"text": "⚙️ SetConfidence"}, {"text": "⏯ Старт / Пауза"}],
            [{"text": "ℹ️ Помощь"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
    }


def build_help_text() -> str:
    return (
        "ℹ️ Помощь\n"
        "━━━━━━━━━━━━\n"
        "📊 Статус\n"
        "📌 Сигналы\n"
        "🎯 Confidence\n"
        "⚙️ SetConfidence\n"
        "⏯ Старт / Пауза\n"
        "⚡ Сейчас\n"
        "📰 /news\n"
        "📰 /news_on\n"
        "📰 /news_off\n"
        "📰 /news_level\n"
        "📰 /news_sources\n"
        "📰 /news_source\n"
        "📰 /news_test\n"
        "━━━━━━━━━━━━"
    )


def help_inline_keyboard() -> Dict:
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Статус", "callback_data": "cmd:status"},
                {"text": "⚡ Сейчас", "callback_data": "cmd:now"},
            ],
            [
                {"text": "📌 Сигналы", "callback_data": "cmd:signals"},
                {"text": "🎯 Confidence", "callback_data": "cmd:confidence"},
            ],
            [
                {"text": "⚙️ SetConfidence", "callback_data": "cmd:setconfidence"},
            ],
            [
                {"text": "⏯ Старт / Пауза", "callback_data": "cmd:toggle"},
            ],
        ]
    }


# ================== STATE ==================
def load_state() -> Dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: Dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def normalize_symbol(symbol: str) -> str:
    return symbol.split("/")[0]


def format_pairs(separator: str) -> str:
    return separator.join([normalize_symbol(s) for s in SYMBOLS])


def format_last_signal(last_signal: Optional[Dict]) -> str:
    if not last_signal:
        return (
            "📌 Последний сигнал\n"
            "━━━━━━━━━━━━━━━━\n"
            "⏳ Сейчас сигнала нет\n"
            "━━━━━━━━━━━━━━━━"
        )

    direction_map = {"UP": "ВВЕРХ", "DOWN": "ВНИЗ"}
    direction = direction_map.get(last_signal.get("direction"), last_signal.get("direction", ""))
    probability = last_signal.get("probability")
    pair = last_signal.get("pair", "")
    display_pair = pair
    if pair and "/" not in pair:
        display_pair = f"{pair}/USDT"
    return (
        "📌 Последний сигнал\n"
        "━━━━━━━━━━━━━━━━\n"
        f"💱 Пара: {display_pair}\n"
        f"🔀 Направление: {direction}\n"
        f"🎯 Вероятность: {probability}%\n"
        f"💰 Цена: {last_signal.get('price', '')}\n"
        "━━━━━━━━━━━━━━━━"
    )


def format_now_signal(last_signal: Dict) -> str:
    direction_map = {"UP": "ВВЕРХ", "DOWN": "ВНИЗ"}
    direction = direction_map.get(last_signal.get("direction"), last_signal.get("direction", ""))
    probability = last_signal.get("probability")
    return (
        "⚡ <b>ВНЕОЧЕРЕДНОЙ АНАЛИЗ</b>\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🧩 Пара:{last_signal.get('pair', '')}\n"
        f"📈 Направление:{direction}\n"
        f"🎯 Вероятность:{probability:.2f}%\n"
        f"💰 Цена:{last_signal.get('price', '')}\n"
        f"🕒 Таймфрейм:{TIMEFRAME}\n"
        "━━━━━━━━━━━━━━━━"
    )


def probability_bar(p: float, length: int = 10) -> str:
    try:
        filled = int(p * length)  # строго вниз, без round
        filled = max(0, min(length, filled))
        return "▰" * filled + "▱" * (length - filled)
    except Exception:
        return "▱" * length


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def format_price(value: Optional[float]) -> str:
    if value is None:
        return ""
    decimals = 2 if abs(value) >= 1 else 6
    return f"{value:.{decimals}f}"


def compute_display_probability(
    probability: Optional[float],
    info: Dict,
    allow_high_confidence: bool = False,
) -> float:
    if probability is not None:
        max_value = 0.99 if allow_high_confidence else 0.95
        return clamp(probability, 0.01, max_value)

    quality_score = info.get("quality_score")
    if quality_score is None:
        return 0.01

    fallback = 0.5 + 0.5 * float(quality_score)
    return clamp(fallback, 0.01, 0.95)


def handle_command(text: str, chat_id: int, state: Dict) -> None:
    global MIN_CONFIDENCE
    parts = text.strip().split()
    if not parts:
        return

    command = parts[0].lower()

    if command == "/start":
        tg_send(
            "◉ СИСТЕМА ЗАПУЩЕНА\n\n"
            f"🧠 Анализ активов: {len(SYMBOLS)}\n"
            f"⏱ Таймфрейм: {TIMEFRAME}\n"
            f"📊 Минимальная уверенность: {MIN_CONFIDENCE}%\n"
            f"🛡 Антиспам: {COOLDOWN_MINUTES} мин",
            chat_id=chat_id,
            reply_markup=main_keyboard(),
        )
        return

    if command == "/status":
        tg_send(
            "🧠 Статус системы\n"
            "━━━━━━━━━━━━━━━━\n"
            f"🪙 Анализ активов: {len(SYMBOLS)}\n"
            f"⏱ Таймфрейм: {TIMEFRAME}\n"
            f"🔄 Проверка: каждые {CHECK_EVERY_SECONDS} сек\n"
            f"🎯 Мин. уверенность: {MIN_CONFIDENCE}%\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return

    if command == "/signals":
        with state_lock:
            last_signal = state.get("last_signal")
        tg_send(format_last_signal(last_signal), chat_id=chat_id)
        return

    if command == "/confidence":
        tg_send(
            "🎯 НАСТРОЙКА УВЕРЕННОСТИ\n"
            "━━━━━━━━━━━━━━━━\n"
            f"🎯 Минимальная уверенность : {MIN_CONFIDENCE}%\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return

    if command == "/help":
        tg_send(
            build_help_text(),
            chat_id=chat_id,
            reply_markup=help_inline_keyboard(),
        )
        return

    if command == "/news":
        send_recent_news(chat_id, state)
        return

    if command == "/news_on":
        with state_lock:
            settings = state.setdefault("news_settings", {})
            settings["enabled"] = True
            save_state(state)
        tg_send(
            "📰 НОВОСТИ ВКЛЮЧЕНЫ\n"
            "━━━━━━━━━━━━━━━━\n"
            "✅ Автопубликация активна\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return

    if command == "/news_off":
        with state_lock:
            settings = state.setdefault("news_settings", {})
            settings["enabled"] = False
            save_state(state)
        tg_send(
            "📰 НОВОСТИ ОТКЛЮЧЕНЫ\n"
            "━━━━━━━━━━━━━━━━\n"
            "⏸ Автопубликация остановлена\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return

    if command == "/news_level":
        if len(parts) == 2 and parts[1].isdigit():
            value = int(parts[1])
            if 0 <= value <= 100:
                with state_lock:
                    settings = state.setdefault("news_settings", {})
                    settings["importance_threshold"] = value
                    save_state(state)
                tg_send(
                    "📰 ПОРОГ ВАЖНОСТИ\n"
                    "━━━━━━━━━━━━━━━━\n"
                    f"🔥 Новый порог: {value}/100\n"
                    "━━━━━━━━━━━━━━━━",
                    chat_id=chat_id,
                )
                return
        with state_lock:
            settings = state.get("news_settings", {})
            current = settings.get("importance_threshold", NEWS_IMPORTANCE_THRESHOLD)
        tg_send(
            "📰 ПОРОГ ВАЖНОСТИ\n"
            "━━━━━━━━━━━━━━━━\n"
            f"🔥 Текущий порог: {current}/100\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return

    if command == "/news_sources":
        with state_lock:
            settings = state.get("news_settings", {})
            sources = settings.get("sources") or NEWS_SOURCES
        lines = [
            "📰 ИСТОЧНИКИ НОВОСТЕЙ",
            "━━━━━━━━━━━━━━━━",
            f"cryptopanic: {'on' if sources.get('cryptopanic') else 'off'}",
            f"rss: {'on' if sources.get('rss') else 'off'}",
            f"gdelt: {'on' if sources.get('gdelt') else 'off'}",
            "━━━━━━━━━━━━━━━━",
        ]
        tg_send("\n".join(lines), chat_id=chat_id)
        return

    if command == "/news_source":
        if len(parts) == 3:
            source_name = parts[1].lower()
            action = parts[2].lower()
            if source_name in NEWS_SOURCES and action in {"on", "off"}:
                with state_lock:
                    settings = state.setdefault("news_settings", {})
                    sources = settings.setdefault("sources", NEWS_SOURCES.copy())
                    sources[source_name] = action == "on"
                    save_state(state)
                tg_send(
                    "📰 ИСТОЧНИКИ НОВОСТЕЙ\n"
                    "━━━━━━━━━━━━━━━━\n"
                    f"{source_name}: {'on' if action == 'on' else 'off'}\n"
                    "━━━━━━━━━━━━━━━━",
                    chat_id=chat_id,
                )
                return
        tg_send(
            "📰 ИСТОЧНИКИ НОВОСТЕЙ\n"
            "━━━━━━━━━━━━━━━━\n"
            "Формат: /news_source <cryptopanic|rss|gdelt> <on|off>\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return

    if command == "/news_test":
        run_news_test(chat_id, state)
        return

    if command == "/setconfidence":
        if len(parts) == 2 and parts[1].isdigit():
            value = int(parts[1])
            if 1 <= value <= 99:
                with state_lock:
                    MIN_CONFIDENCE = value
                    state["min_confidence"] = value
                    save_state(state)
                tg_send(
                    "✅ НАСТРОЙКА ОБНОВЛЕНА\n"
                    "━━━━━━━━━━━━━━━━\n"
                    f"🎯 Мин. уверенность : {MIN_CONFIDENCE}%\n"
                    "━━━━━━━━━━━━━━━━",
                    chat_id=chat_id,
                )
                return
        if len(parts) == 1:
            with state_lock:
                state["awaiting_confidence"] = True
                save_state(state)
            tg_send(
                "⚙️ УСТАНОВКА УВЕРЕННОСТИ\n"
                "━━━━━━━━━━━━━━━━\n"
                "Введите значение 1–99\n"
                "Например: 65\n"
                "━━━━━━━━━━━━━━━━",
                chat_id=chat_id,
            )
            return
        tg_send("❌ Введите число 1–99.", chat_id=chat_id)
        return

    if command == "/pause":
        with state_lock:
            state["paused"] = True
            save_state(state)
        tg_send(
            "⏸ СИГНАЛЫ НА ПАУЗЕ\n"
            "━━━━━━━━━━━━━━━━\n"
            "⏸ Автоматическая отправка\n"
            "временно остановлена\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return

    if command == "/toggle":
        with state_lock:
            is_paused = state.get("paused", False)
            state["paused"] = not is_paused
            save_state(state)
        if is_paused:
            tg_send("▶️ Бот возобновлён", chat_id=chat_id)
        else:
            tg_send("⏸ Бот поставлен на паузу", chat_id=chat_id)
        return

    if command == "/resume":
        with state_lock:
            state["paused"] = False
            save_state(state)
        tg_send(
            "▶️ СИГНАЛЫ ВКЛЮЧЕНЫ\n"
            "━━━━━━━━━━━━━━━━\n"
            "▶️ Автоматическая отправка\n"
            "сигналов активна\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return

    if command == "/now":
        with state_lock:
            run_now_request["chat_id"] = chat_id
        tg_send(
            "⚡ ВНЕОЧЕРЕДНОЙ АНАЛИЗ\n"
            "━━━━━━━━━━━━━━━━\n"
            "🔍 Анализ выполняется…\n"
            "Результат придёт следующим\n"
            "сообщением\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return


# ================= NEWS SYSTEM =================
@dataclass
class NewsItem:
    provider: str
    provider_id: str
    title: str
    url: str
    published_ts: int
    raw: Dict
    coins: List[str]
    category: str
    importance: int
    urgency: int
    credibility: int
    price_move: Optional[str]
    canonical_key: str


NEWS_TEXT_MAX_LEN = 180
NEWS_CLEAN_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_name",
    "utm_referrer",
    "ref",
    "ref_src",
}
NEWS_SUSPICIOUS_DOMAINS = {"t.me", "twitter.com", "x.com"}

COIN_TICKERS = [
    "BTC", "ETH", "XRP", "SOL", "BNB", "ADA", "DOGE", "TRX", "DOT", "AVAX",
    "LINK", "MATIC", "LTC", "BCH", "XLM", "ATOM", "ETC", "APT", "ARB", "OP",
    "NEAR", "FIL", "ICP", "SUI", "INJ", "AAVE", "UNI", "RUNE", "ALGO", "EGLD",
    "FTM", "KAVA", "HBAR", "XTZ", "FLOW", "GRT", "SNX", "MKR", "DYDX", "IMX",
]
COIN_ALIASES = {
    "bitcoin": "BTC",
    "btc": "BTC",
    "ethereum": "ETH",
    "ether": "ETH",
    "eth": "ETH",
    "ripple": "XRP",
    "xrp": "XRP",
    "solana": "SOL",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "polkadot": "DOT",
    "avalanche": "AVAX",
    "chainlink": "LINK",
    "polygon": "MATIC",
    "litecoin": "LTC",
    "bitcoin cash": "BCH",
    "stellar": "XLM",
    "cosmos": "ATOM",
    "ethereum classic": "ETC",
}
COIN_PATTERN = re.compile(r"\b(" + "|".join(COIN_TICKERS) + r")\b", re.IGNORECASE)
DOLLAR_TICKER_PATTERN = re.compile(r"\$([A-Z]{2,6})\b")


def safe_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > NEWS_TEXT_MAX_LEN:
        text = text[:NEWS_TEXT_MAX_LEN - 1] + "…"
    return text


def normalize_url(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return url.strip()
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or ""
    query_items = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in NEWS_CLEAN_QUERY_KEYS and not k.lower().startswith("utm_")
    ]
    query = urlencode(query_items)
    cleaned = urlunparse((scheme, netloc, path, "", query, ""))
    return cleaned.rstrip("/")


def normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", (title or "").strip().lower())


def to_epoch(value: Optional[str]) -> int:
    if value is None:
        return int(time.time())
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return int(time.time())
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    except Exception:
        try:
            dt = parsedate_to_datetime(text)
        except Exception:
            return int(time.time())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def make_canonical_key(provider: str, provider_id: str, canonical_url: str, title: str) -> str:
    raw_key = f"{provider}|{provider_id}|{canonical_url}|{title}"
    return hashlib.sha1(raw_key.encode("utf-8")).hexdigest()


def build_cross_source_key(canonical_url: str, title_hash: str) -> str:
    raw_key = f"{canonical_url}|{title_hash}"
    return hashlib.sha1(raw_key.encode("utf-8")).hexdigest()


def extract_coins(title: str, text_optional: Optional[str] = None) -> List[str]:
    content = f"{title} {text_optional or ''}".strip()
    coins: List[str] = []
    for ticker in DOLLAR_TICKER_PATTERN.findall(content):
        t = ticker.upper()
        if t in COIN_TICKERS and t not in coins:
            coins.append(t)
    for ticker in COIN_PATTERN.findall(content):
        t = ticker.upper()
        if t not in coins:
            coins.append(t)
    lowered = content.lower()
    for alias, ticker in COIN_ALIASES.items():
        if alias in lowered and ticker not in coins:
            coins.append(ticker)
    return coins[:3]


def classify_category(title: str, text_optional: Optional[str] = None) -> str:
    content = f"{title} {text_optional or ''}".lower()
    if any(word in content for word in ["hack", "exploit", "breach", "drain", "attack"]):
        return "security"
    if any(word in content for word in ["sec", "lawsuit", "regulation", "ban", "compliance"]):
        return "regulation"
    if any(word in content for word in ["listing", "delisting", "exchange", "airdrop"]):
        return "exchange"
    if any(word in content for word in ["etf", "blackrock", "approval", "fed", "cpi", "macro"]):
        return "macro"
    return "market"


def score_credibility(provider: str, repeated_count: int, has_multiple_sources: bool, url: str) -> int:
    base = 70 if provider == "cryptopanic" else 55 if provider == "rss" else 50
    if repeated_count > 1:
        base += 10
    if has_multiple_sources:
        base += 10
    domain = urlparse(url).netloc.lower() if url else ""
    if domain in NEWS_SUSPICIOUS_DOMAINS:
        base -= 15
    return max(0, min(100, base))


def compute_importance(item: NewsItem, repeated_count: int, has_multiple_sources: bool) -> int:
    score = 35
    if item.category in {"security", "regulation", "macro"}:
        score += 20
    if any(coin in {"BTC", "ETH"} for coin in item.coins):
        score += 5
    if repeated_count > 1:
        score += 10
    if has_multiple_sources:
        score += 10
    title = item.title.lower()
    if any(word in title for word in ["breaking", "urgent", "just in"]):
        score += 10
    return max(0, min(100, score))


def compute_urgency(item: NewsItem, repeated_count: int) -> int:
    score = 30
    title = item.title.lower()
    if any(word in title for word in ["breaking", "urgent", "just in"]):
        score += 15
    if item.category in {"security", "regulation"}:
        score += 10
    if repeated_count > 1:
        score += 5
    return max(0, min(100, score))


def get_price_change_1h(exchange: ccxt.bybit, symbol: str) -> Optional[str]:
    parsed = fetch_ohlcv_cached(exchange, symbol, "1h", limit=2, ttl_seconds=120)
    if not parsed:
        return None
    closes = parsed[2]
    if len(closes) < 2 or closes[-2] == 0:
        return None
    pct = (closes[-1] - closes[-2]) / closes[-2] * 100
    return f"{pct:+.2f}%"


def fetch_news_from_provider(provider_name: str, since_ts: int, limit: int) -> List[Dict]:
    if provider_name == "cryptopanic":
        params = {"kind": "news"}
        if CRYPTOPANIC_TOKEN:
            params["auth_token"] = CRYPTOPANIC_TOKEN
        else:
            params["public"] = "true"
        try:
            response = requests.get(
                CRYPTOPANIC_ENDPOINT,
                params=params,
                timeout=NEWS_HTTP_TIMEOUT,
            )
            if response.status_code != 200:
                return []
            data = response.json()
            results = data.get("results", [])
            return results[:limit]
        except Exception:
            return []

    if provider_name == "rss":
        items: List[Dict] = []
        for feed_url in RSS_FEEDS:
            try:
                response = requests.get(feed_url, timeout=NEWS_HTTP_TIMEOUT)
                if response.status_code != 200:
                    continue
                root = ET.fromstring(response.text)
            except Exception:
                continue
            feed_name = urlparse(feed_url).netloc or "rss"
            for item in root.findall(".//item"):
                title = item.findtext("title") or ""
                link = item.findtext("link") or ""
                pub_date = item.findtext("pubDate") or item.findtext("published") or ""
                items.append({
                    "feed": feed_name,
                    "title": title,
                    "link": link,
                    "published": pub_date,
                })
                if len(items) >= limit:
                    break
            if len(items) >= limit:
                break
            for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
                title = entry.findtext("{http://www.w3.org/2005/Atom}title") or ""
                link_el = entry.find("{http://www.w3.org/2005/Atom}link")
                link = link_el.get("href") if link_el is not None else ""
                pub_date = entry.findtext("{http://www.w3.org/2005/Atom}published") or ""
                items.append({
                    "feed": feed_name,
                    "title": title,
                    "link": link,
                    "published": pub_date,
                })
                if len(items) >= limit:
                    break
        return items

    if provider_name == "gdelt":
        params = {
            "query": "cryptocurrency OR bitcoin OR ethereum OR xrp",
            "mode": "ArtList",
            "format": "json",
        }
        try:
            response = requests.get(
                GDELT_DOC_ENDPOINT,
                params=params,
                timeout=NEWS_HTTP_TIMEOUT,
            )
            if response.status_code != 200:
                return []
            data = response.json()
            results = data.get("articles", [])
            return results[:limit]
        except Exception:
            return []

    return []


def parse_provider_items(provider_name: str, raw_items: List[Dict], since_ts: int) -> List[NewsItem]:
    parsed_items: List[NewsItem] = []
    for raw in raw_items:
        if provider_name == "cryptopanic":
            provider_id = str(raw.get("id", ""))
            title = safe_text(raw.get("title"))
            url = raw.get("url") or ""
            published_ts = to_epoch(raw.get("published_at"))
        elif provider_name == "rss":
            provider_id = hashlib.sha1((raw.get("link") or raw.get("title") or "").encode("utf-8")).hexdigest()
            title = safe_text(raw.get("title"))
            url = raw.get("link") or ""
            published_ts = to_epoch(raw.get("published"))
        else:
            provider_id = hashlib.sha1((raw.get("url") or raw.get("title") or "").encode("utf-8")).hexdigest()
            title = safe_text(raw.get("title"))
            url = raw.get("url") or ""
            published_ts = to_epoch(raw.get("seendate") or raw.get("published"))

        if not title:
            continue
        if published_ts < since_ts:
            continue

        canonical_url = normalize_url(url)
        normalized_title = normalize_title(title)
        title_hash = hashlib.sha1(normalized_title.encode("utf-8")).hexdigest()
        canonical_key = make_canonical_key(provider_name, provider_id, canonical_url, normalized_title)
        coins = extract_coins(title)
        category = classify_category(title)
        raw_payload = dict(raw)
        raw_payload.update({
            "canonical_url": canonical_url,
            "normalized_title": normalized_title,
            "title_hash": title_hash,
        })
        parsed_items.append(NewsItem(
            provider=provider_name,
            provider_id=provider_id,
            title=title,
            url=url,
            published_ts=published_ts,
            raw=raw_payload,
            coins=coins,
            category=category,
            importance=0,
            urgency=0,
            credibility=0,
            price_move=None,
            canonical_key=canonical_key,
        ))
    return parsed_items


def news_item_to_dict(item: NewsItem) -> Dict:
    return {
        "provider": item.provider,
        "provider_id": item.provider_id,
        "title": item.title,
        "url": item.url,
        "published_ts": item.published_ts,
        "raw": item.raw,
        "coins": item.coins,
        "category": item.category,
        "importance": item.importance,
        "urgency": item.urgency,
        "credibility": item.credibility,
        "price_move": item.price_move,
        "canonical_key": item.canonical_key,
    }


def news_item_from_dict(data: Dict) -> NewsItem:
    return NewsItem(
        provider=data.get("provider", ""),
        provider_id=data.get("provider_id", ""),
        title=data.get("title", ""),
        url=data.get("url", ""),
        published_ts=int(data.get("published_ts", 0)),
        raw=data.get("raw", {}),
        coins=data.get("coins", []) or [],
        category=data.get("category", "market"),
        importance=int(data.get("importance", 0)),
        urgency=int(data.get("urgency", 0)),
        credibility=int(data.get("credibility", 0)),
        price_move=data.get("price_move"),
        canonical_key=data.get("canonical_key", ""),
    )


def format_news_card(item: NewsItem) -> str:
    coins = ", ".join(item.coins) if item.coins else "—"
    url = item.url or "—"
    if item.urgency >= NEWS_URGENT_THRESHOLD:
        price_line = f"📈 {item.price_move}" if item.price_move else ""
        return (
            "🚨 Срочно\n"
            "━━━━━━━━━━━━━━━━\n"
            f"🪙 {coins}\n"
            f"🏷 {item.category}\n"
            f"🔥 {item.importance}/100  ⏱ {item.urgency}/100\n"
            f"{price_line}\n"
            f"🧠 {item.title}\n"
            f"🔗 {url}\n"
            "━━━━━━━━━━━━━━━━"
        ).replace("\n\n", "\n")
    return (
        "📰 Новость\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🪙 {coins}\n"
        f"🏷 {item.category}\n"
        f"🔥 {item.importance}/100\n"
        f"🧠 {item.title}\n"
        f"🔗 {url}\n"
        "━━━━━━━━━━━━━━━━"
    )


def send_recent_news(chat_id: int, state: Dict) -> None:
    with state_lock:
        items_raw = list(state.get("news", []))
    if not items_raw:
        tg_send(
            "📰 НОВОСТИ\n"
            "━━━━━━━━━━━━━━━━\n"
            "Пока нет новостей\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
        )
        return
    limit = min(len(items_raw), NEWS_SHOW_LIMIT)
    items = [news_item_from_dict(item) for item in items_raw[-limit:]]
    items.reverse()
    chunks: List[str] = []
    current = ""
    for item in items:
        card = format_news_card(item)
        if len(current) + len(card) + 2 > 3500:
            if current:
                chunks.append(current)
            current = card
        else:
            current = f"{current}\n\n{card}" if current else card
    if current:
        chunks.append(current)
    for chunk in chunks:
        tg_send(chunk, chat_id=chat_id)


def prune_news_list(news_list: List[Dict]) -> List[Dict]:
    if len(news_list) <= NEWS_STORE_LIMIT:
        return news_list
    return news_list[-NEWS_STORE_LIMIT:]


def prune_news_seen(news_seen: Dict[str, int]) -> None:
    if len(news_seen) <= NEWS_SEEN_LIMIT:
        return
    sorted_items = sorted(news_seen.items(), key=lambda item: item[1])
    remove_count = max(1, int(len(sorted_items) * 0.2))
    for key, _ in sorted_items[:remove_count]:
        news_seen.pop(key, None)


def news_poll_once(
    exchange: Optional[ccxt.bybit],
    state: Dict,
    publish: bool = True,
    chat_id: Optional[int] = None,
    test_mode: bool = False,
    update_last_poll: bool = True,
) -> List[NewsItem]:
    with state_lock:
        settings = dict(state.get("news_settings", {}))
        enabled = settings.get("enabled", NEWS_ENABLED)
        sources = dict(settings.get("sources") or NEWS_SOURCES)
        threshold = settings.get("importance_threshold", NEWS_IMPORTANCE_THRESHOLD)
        price_check_enabled = settings.get("price_check", NEWS_PRICE_CHECK_ENABLED)
        news_seen = dict(state.get("news_seen", {}))
        news_list = list(state.get("news", []))
        price_last_check = dict(state.get("news_price_last_check", {}))
        last_poll_ts = int(state.get("news_last_poll_ts", 0))

    if not enabled and not test_mode:
        return []

    since_ts = max(last_poll_ts - 3600, 0)
    now_ts = int(time.time())
    raw_all: List[NewsItem] = []
    providers = ["cryptopanic", "rss", "gdelt"]
    for provider in providers:
        if not sources.get(provider):
            continue
        raw_items = fetch_news_from_provider(provider, since_ts, NEWS_MAX_ITEMS_PER_POLL)
        parsed_items = parse_provider_items(provider, raw_items, since_ts)
        raw_all.extend(parsed_items)

    if not raw_all:
        if update_last_poll:
            with state_lock:
                state["news_last_poll_ts"] = now_ts
                save_state(state)
        return []

    title_count: Dict[str, int] = {}
    source_by_title: Dict[str, set] = {}
    for item in raw_all:
        title_hash = item.raw.get("title_hash", "")
        if not title_hash:
            continue
        title_count[title_hash] = title_count.get(title_hash, 0) + 1
        source_by_title.setdefault(title_hash, set()).add(item.provider)

    symbol_map = {normalize_symbol(symbol): symbol for symbol in SYMBOLS}
    new_items: List[NewsItem] = []

    for item in raw_all:
        canonical_url = item.raw.get("canonical_url", "")
        title_hash = item.raw.get("title_hash", "")
        cross_key = build_cross_source_key(canonical_url, title_hash)
        if item.canonical_key in news_seen or cross_key in news_seen:
            continue

        repeated_count = title_count.get(title_hash, 1)
        has_multiple_sources = len(source_by_title.get(title_hash, set())) > 1
        item.credibility = score_credibility(item.provider, repeated_count, has_multiple_sources, canonical_url)
        item.importance = compute_importance(item, repeated_count, has_multiple_sources)
        item.urgency = compute_urgency(item, repeated_count)

        if (
            price_check_enabled
            and exchange is not None
            and len(item.coins) == 1
            and item.importance >= NEWS_PRICE_CHECK_MIN_IMPORTANCE
        ):
            coin = item.coins[0]
            last_check = int(price_last_check.get(coin, 0))
            if now_ts - last_check >= NEWS_PRICE_CHECK_COOLDOWN_SEC:
                symbol = symbol_map.get(coin)
                if symbol:
                    try:
                        price_move = get_price_change_1h(exchange, symbol)
                    except Exception:
                        price_move = None
                    item.price_move = price_move
                    price_last_check[coin] = now_ts

        news_seen[item.canonical_key] = now_ts
        news_seen[cross_key] = now_ts
        new_items.append(item)
        news_list.append(news_item_to_dict(item))

    if not new_items:
        return []

    news_list = prune_news_list(news_list)
    prune_news_seen(news_seen)

    if update_last_poll:
        last_poll_ts = now_ts

    with state_lock:
        state["news"] = news_list
        state["news_seen"] = news_seen
        state["news_price_last_check"] = price_last_check
        if update_last_poll:
            state["news_last_poll_ts"] = last_poll_ts
        save_state(state)

    if publish:
        for item in new_items:
            if item.importance < threshold:
                continue
            if not item.url:
                continue
            tg_send(format_news_card(item))

    if test_mode and chat_id is not None:
        preview = new_items[:3]
        if not preview:
            tg_send(
                "📰 TEST NEWS\n"
                "━━━━━━━━━━━━━━━━\n"
                "Нет свежих новостей\n"
                "━━━━━━━━━━━━━━━━",
                chat_id=chat_id,
            )
        else:
            for item in preview:
                tg_send(format_news_card(item), chat_id=chat_id)

    print(f"[NEWS] fetched {len(raw_all)} items, new {len(new_items)}")
    return new_items


def news_worker(exchange: ccxt.bybit, state: Dict) -> None:
    print("News worker started")
    while True:
        try:
            news_poll_once(exchange, state, publish=True, update_last_poll=True)
        except Exception as e:
            print(f"[NEWS ERROR] {type(e).__name__}: {e}")
        time.sleep(NEWS_POLL_SECONDS)


def _news_test_worker(chat_id: int, state: Dict) -> None:
    try:
        news_poll_once(
            exchange=None,
            state=state,
            publish=False,
            chat_id=chat_id,
            test_mode=True,
            update_last_poll=False,
        )
    except Exception as e:
        print(f"[NEWS TEST ERROR] {type(e).__name__}: {e}")
        tg_send("📰 TEST NEWS: ошибка при выполнении", chat_id=chat_id)


def run_news_test(chat_id: int, state: Dict) -> None:
    tg_send(
        "📰 TEST NEWS\n"
        "━━━━━━━━━━━━━━━━\n"
        "⏳ Проверяю источники…\n"
        "━━━━━━━━━━━━━━━━",
        chat_id=chat_id,
    )
    threading.Thread(target=_news_test_worker, args=(chat_id, state), daemon=True).start()


# ================== INDICATORS ==================
def ema(values: List[float], period: int) -> List[float]:
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def rsi(values: List[float], period: int = 14) -> List[float]:
    gains = []
    losses = []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    if len(gains) < period:
        return [50.0] * len(values)

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    rsi_vals = [50.0] * (period + 1)

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi_vals.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_vals.append(100 - (100 / (1 + rs)))

    return rsi_vals


def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None

    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)

    if len(trs) < period:
        return None

    atr_value = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr_value = (atr_value * (period - 1) + tr) / period
    return atr_value


def adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < (period * 2) + 1:
        return None

    trs = []
    plus_dm = []
    minus_dm = []
    for i in range(1, len(closes)):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        trs.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        )

    if len(trs) < period:
        return None

    tr_smooth = sum(trs[:period])
    plus_smooth = sum(plus_dm[:period])
    minus_smooth = sum(minus_dm[:period])

    dx_values = []
    for i in range(period, len(trs)):
        tr_smooth = tr_smooth - (tr_smooth / period) + trs[i]
        plus_smooth = plus_smooth - (plus_smooth / period) + plus_dm[i]
        minus_smooth = minus_smooth - (minus_smooth / period) + minus_dm[i]

        if tr_smooth == 0:
            dx_values.append(0.0)
            continue

        plus_di = 100 * (plus_smooth / tr_smooth)
        minus_di = 100 * (minus_smooth / tr_smooth)
        di_sum = plus_di + minus_di
        dx = 0.0 if di_sum == 0 else 100 * abs(plus_di - minus_di) / di_sum
        dx_values.append(dx)

    if len(dx_values) < period:
        return None

    adx_value = sum(dx_values[:period]) / period
    for dx in dx_values[period:]:
        adx_value = (adx_value * (period - 1) + dx) / period
    return adx_value


# ================== DATA LAYER ==================
def _is_rate_limit_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "rate limit" in message
        or "too many visits" in message
        or "too many requests" in message
        or "429" in message
    )


def _log_rate_limit_once(context: str) -> None:
    global _LAST_RL_LOG_TS
    now = time.time()
    if now - _LAST_RL_LOG_TS >= 30:
        print(f"[RATE_LIMIT] {context}")
        _LAST_RL_LOG_TS = now


def fetch_ohlcv_cached(
    exchange: ccxt.bybit,
    symbol: str,
    timeframe: str,
    limit: int,
    ttl_seconds: int,
) -> Optional[Tuple[List[float], List[float], List[float], List[float], List[float]]]:
    now = time.time()
    cache_key = (symbol, timeframe)
    cached = _OHLCV_CACHE.get(cache_key)
    if cached and now - float(cached["ts"]) < ttl_seconds:
        return cached["parsed"]  # type: ignore[return-value]
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as e:
        if _is_rate_limit_error(e):
            _log_rate_limit_once(f"fetch_ohlcv {symbol} {timeframe}")
            return None
        print(f"[DATA] fetch_ohlcv error {symbol} {timeframe}: {e}")
        return None
    if not ohlcv:
        return None
    highs = [row[2] for row in ohlcv]
    lows = [row[3] for row in ohlcv]
    closes = [row[4] for row in ohlcv]
    volumes = [row[5] for row in ohlcv]
    timestamps = [row[0] for row in ohlcv]
    parsed = (highs, lows, closes, volumes, timestamps)
    _OHLCV_CACHE[cache_key] = {
        "ts": now,
        "ohlcv": ohlcv,
        "parsed": parsed,
    }
    return parsed


def _parsed_to_dict(
    parsed: Tuple[List[float], List[float], List[float], List[float], List[float]],
) -> Dict[str, List[float]]:
    highs, lows, closes, volumes, timestamps = parsed
    return {
        "highs": highs,
        "lows": lows,
        "closes": closes,
        "volumes": volumes,
        "timestamps": timestamps,
    }


def fetch_ohlcv_raw_cached(
    exchange: ccxt.bybit,
    symbol: str,
    timeframe: str,
    limit: int,
    ttl_seconds: int,
) -> Optional[List[List[float]]]:
    parsed = fetch_ohlcv_cached(exchange, symbol, timeframe, limit, ttl_seconds)
    if parsed is None:
        return None
    cached = _OHLCV_CACHE.get((symbol, timeframe))
    if not cached:
        return None
    return cached.get("ohlcv")  # type: ignore[return-value]


def get_live_price_if_needed(
    exchange: ccxt.bybit,
    symbol: str,
    fallback_price: float,
    has_active_setup: bool,
) -> Optional[float]:
    if not ENGINE_V2_USE_LIVE_TRIGGER:
        return fallback_price
    if not has_active_setup:
        return fallback_price
    try:
        ticker = exchange.fetch_ticker(symbol)
        last_price = ticker.get("last") or ticker.get("close")
        if last_price is None:
            return fallback_price
        return float(last_price)
    except Exception as e:
        if _is_rate_limit_error(e):
            _log_rate_limit_once(f"fetch_ticker {symbol}")
            return None
        return fallback_price


# ================== ANALYSIS ENGINE V2 ==================
def _safe_slope(series: List[float], length: int = 5) -> float:
    if len(series) <= length:
        return 0.0
    return (series[-1] - series[-length]) / max(length, 1)


def _sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _std(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    mean = sum(values[-period:]) / period
    variance = sum((v - mean) ** 2 for v in values[-period:]) / period
    return math.sqrt(variance)


def trend_features(highs: List[float], lows: List[float], closes: List[float]) -> Dict:
    if len(closes) < 200:
        return {}
    ema_fast_series = ema(closes, 50)
    ema_slow_series = ema(closes, 200)
    ema_fast_val = ema_fast_series[-1]
    ema_slow_val = ema_slow_series[-1]
    slope_fast = _safe_slope(ema_fast_series, 5)
    price = closes[-1]
    distance_to_slow = ((price - ema_slow_val) / ema_slow_val) * 100 if ema_slow_val else 0.0

    n = 5
    if len(highs) < n + 1 or len(lows) < n + 1:
        structure = 0
    else:
        higher_high = highs[-1] > max(highs[-n - 1:-1])
        higher_low = lows[-1] > min(lows[-n - 1:-1])
        lower_high = highs[-1] < max(highs[-n - 1:-1])
        lower_low = lows[-1] < min(lows[-n - 1:-1])
        structure = 1 if higher_high and higher_low else -1 if lower_high and lower_low else 0

    return {
        "ema_fast": ema_fast_val,
        "ema_slow": ema_slow_val,
        "ema_fast_slope": slope_fast,
        "distance_to_slow": distance_to_slow,
        "structure": structure,
    }


def momentum_features(closes: List[float]) -> Dict:
    if len(closes) < 20:
        return {}
    rsi_series = rsi(closes, 14)
    rsi_val = rsi_series[-1]
    rsi_slope = _safe_slope(rsi_series, 5)
    roc = ((closes[-1] - closes[-6]) / closes[-6]) * 100 if len(closes) > 6 else 0.0
    return {
        "rsi": rsi_val,
        "rsi_slope": rsi_slope,
        "roc": roc,
    }


def volatility_features(highs: List[float], lows: List[float], closes: List[float]) -> Dict:
    atr_val = atr(highs, lows, closes, 14)
    if atr_val is None:
        return {}
    price = closes[-1]
    atr_pct = (atr_val / price) * 100 if price else 0.0
    bb_sma = _sma(closes, 20)
    bb_std = _std(closes, 20)
    bb_width = 0.0
    if bb_sma is not None and bb_std is not None and bb_sma != 0:
        upper = bb_sma + (2 * bb_std)
        lower = bb_sma - (2 * bb_std)
        bb_width = ((upper - lower) / bb_sma) * 100
    return {
        "atr": atr_val,
        "atr_pct": atr_pct,
        "bb_width": bb_width,
    }


def volume_features(volumes: List[float]) -> Dict:
    vol_sma20 = _sma(volumes, 20)
    if vol_sma20 is None or vol_sma20 <= 0:
        return {}
    vol_ratio = volumes[-1] / vol_sma20
    vol_slope = _safe_slope(volumes, 5)
    return {
        "vol_sma20": vol_sma20,
        "vol_ratio": vol_ratio,
        "vol_slope": vol_slope,
    }


def level_features(highs: List[float], lows: List[float], closes: List[float]) -> Dict:
    n = 20
    if len(highs) < n + 1 or len(lows) < n + 1:
        return {}
    high_n_prev = max(highs[-n - 1:-1])
    low_n_prev = min(lows[-n - 1:-1])
    compression_window = 8
    if len(highs) < compression_window or len(lows) < compression_window:
        compression = 0.0
    else:
        recent_range = max(highs[-compression_window:]) - min(lows[-compression_window:])
        atr_val = atr(highs, lows, closes, 14) or 0.0
        compression = (recent_range / atr_val) if atr_val else 0.0

    price = closes[-1]
    dist_to_high = ((high_n_prev - price) / price) * 100 if price else 0.0
    dist_to_low = ((price - low_n_prev) / price) * 100 if price else 0.0
    return {
        "high_n_prev": high_n_prev,
        "low_n_prev": low_n_prev,
        "dist_to_high_pct": dist_to_high,
        "dist_to_low_pct": dist_to_low,
        "compression": compression,
    }


def build_features(base_data: Dict[str, List[float]]) -> Dict:
    highs = base_data["highs"]
    lows = base_data["lows"]
    closes = base_data["closes"]
    volumes = base_data["volumes"]
    features = {}
    features.update(trend_features(highs, lows, closes))
    features.update(momentum_features(closes))
    features.update(volatility_features(highs, lows, closes))
    features.update(volume_features(volumes))
    features.update(level_features(highs, lows, closes))
    adx_val = adx(highs, lows, closes, 14)
    if adx_val is not None:
        features["adx"] = adx_val
    return features


def detect_regime(features: Dict) -> str:
    atr_pct = features.get("atr_pct", 0)
    adx_val = features.get("adx", 0)
    ema_fast_val = features.get("ema_fast")
    ema_slow_val = features.get("ema_slow")
    trend_up = ema_fast_val is not None and ema_slow_val is not None and ema_fast_val > ema_slow_val
    trend_down = ema_fast_val is not None and ema_slow_val is not None and ema_fast_val < ema_slow_val

    if atr_pct >= REGIME_ATR_PCT_HIGH:
        return "HIGH_VOLATILITY"
    if atr_pct <= REGIME_ATR_PCT_LOW:
        return "LOW_VOLATILITY"
    if adx_val <= REGIME_CHOP_ADX_MAX:
        return "CHOP"
    if adx_val >= 22 and trend_up:
        return "TREND_UP"
    if adx_val >= 22 and trend_down:
        return "TREND_DOWN"
    return "RANGE"


def _score_setup(features: Dict, proximity: float, pattern_score: float) -> float:
    trend_score = 1.0 if abs(features.get("ema_fast", 0) - features.get("ema_slow", 0)) > 0 else 0.0
    adx_score = clamp((features.get("adx", 0) - 14) / 20, 0, 1)
    vol_score = clamp((features.get("vol_ratio", 1) - 1) / 0.8, 0, 1)
    proximity_score = clamp(1 - (proximity / max(SETUP_DISTANCE_PCT, 0.01)), 0, 1)
    setup_score = (
        W_TREND * trend_score
        + W_ADX * adx_score
        + W_VOL * vol_score
        + W_PROXIMITY * proximity_score
        + W_PATTERN * pattern_score
    )
    return clamp(setup_score, 0, 1)


def generate_setups(
    symbol: str,
    base_data: Dict[str, List[float]],
    higher_data: Optional[Dict[str, List[float]]],
    features: Dict,
    regime: str,
) -> List[Dict]:
    if regime in {"CHOP", "HIGH_VOLATILITY"}:
        return []
    if features.get("adx", 0) < FILTER_MIN_ADX_SETUP:
        return []
    if features.get("vol_ratio", 0) < FILTER_MIN_VOL_RATIO_SETUP:
        return []
    rsi_val = features.get("rsi", 50)
    if rsi_val >= FILTER_RSI_MAX_SETUP or rsi_val <= FILTER_RSI_MIN_SETUP:
        return []

    compression = features.get("compression", 0.0)
    pattern_score = clamp(1 - (compression / 4), 0, 1) if compression else 0.0
    setups = []
    price = base_data["closes"][-1]

    def _mtf_direction() -> Optional[str]:
        if not higher_data or not ENGINE_V2_USE_MTF:
            return None
        higher_features = build_features(higher_data)
        ema_fast_val = higher_features.get("ema_fast")
        ema_slow_val = higher_features.get("ema_slow")
        if ema_fast_val is None or ema_slow_val is None:
            return None
        if ema_fast_val > ema_slow_val:
            return "LONG"
        if ema_fast_val < ema_slow_val:
            return "SHORT"
        return None

    mtf_direction = _mtf_direction()
    dist_to_high = abs(features.get("dist_to_high_pct", 999))
    dist_to_low = abs(features.get("dist_to_low_pct", 999))

    if dist_to_high <= SETUP_DISTANCE_PCT:
        if mtf_direction in (None, "LONG"):
            setup_score = _score_setup(features, dist_to_high, pattern_score)
            if setup_score >= SETUP_MIN_SCORE:
                setups.append({
                    "symbol": symbol,
                    "direction": "LONG",
                    "level": features.get("high_n_prev"),
                    "setup_score": setup_score,
                    "created_at": time.time(),
                    "expires_at": time.time() + (SETUP_TTL_MINUTES * 60),
                    "rationale": "trend/volume/compression/level",
                    "distance_pct": dist_to_high,
                })

    if dist_to_low <= SETUP_DISTANCE_PCT:
        if mtf_direction in (None, "SHORT"):
            setup_score = _score_setup(features, dist_to_low, pattern_score)
            if setup_score >= SETUP_MIN_SCORE:
                setups.append({
                    "symbol": symbol,
                    "direction": "SHORT",
                    "level": features.get("low_n_prev"),
                    "setup_score": setup_score,
                    "created_at": time.time(),
                    "expires_at": time.time() + (SETUP_TTL_MINUTES * 60),
                    "rationale": "trend/volume/compression/level",
                    "distance_pct": dist_to_low,
                })
    return setups


def _entry_risk_targets(
    direction: str,
    entry_price: float,
    level: float,
    atr_val: float,
    base_data: Dict[str, List[float]],
) -> Optional[Dict]:
    lows = base_data["lows"]
    highs = base_data["highs"]
    if direction == "LONG":
        swing = min(lows[-20:]) if len(lows) >= 20 else min(lows)
        sl = min(swing, entry_price - (RISK_ATR_MULT_SL * atr_val))
        risk = entry_price - sl
        tp = entry_price + (RISK_MIN_RR * risk)
    else:
        swing = max(highs[-20:]) if len(highs) >= 20 else max(highs)
        sl = max(swing, entry_price + (RISK_ATR_MULT_SL * atr_val))
        risk = sl - entry_price
        tp = entry_price - (RISK_MIN_RR * risk)
    if risk <= 0:
        return None
    sl_pct = (risk / entry_price) * 100 if entry_price else 0
    if sl_pct > RISK_MAX_SL_PCT:
        return None
    return {"sl": sl, "tp": tp, "risk": risk}


def check_trigger(
    setup: Dict,
    live_price: float,
    base_data: Dict[str, List[float]],
    features: Dict,
) -> Optional[Dict]:
    direction = setup["direction"]
    level = setup["level"]
    if level is None:
        return None
    atr_val = features.get("atr")
    if atr_val is None:
        return None
    closes = base_data["closes"]
    highs = base_data["highs"]
    lows = base_data["lows"]
    buffer = level * (TRIGGER_BUFFER_PCT / 100)
    if direction == "LONG":
        breakout_price = level + buffer
        breakout_ok = live_price > breakout_price
        retest_ok = min(lows[-TRIGGER_RETEST_MAX_BARS:]) <= breakout_price if len(lows) >= TRIGGER_RETEST_MAX_BARS else False
        momentum_ok = (highs[-1] - highs[-2]) >= TRIGGER_MOMENTUM_ATR_MULT * atr_val if len(highs) > 1 else False
    else:
        breakout_price = level - buffer
        breakout_ok = live_price < breakout_price
        retest_ok = max(highs[-TRIGGER_RETEST_MAX_BARS:]) >= breakout_price if len(highs) >= TRIGGER_RETEST_MAX_BARS else False
        momentum_ok = (lows[-2] - lows[-1]) >= TRIGGER_MOMENTUM_ATR_MULT * atr_val if len(lows) > 1 else False

    if TRIGGER_CONFIRM_MODE == "close":
        trigger_ok = breakout_ok and ((closes[-1] > breakout_price) if direction == "LONG" else (closes[-1] < breakout_price))
    elif TRIGGER_CONFIRM_MODE == "momentum":
        trigger_ok = breakout_ok and momentum_ok
    else:
        trigger_ok = breakout_ok and retest_ok

    if not trigger_ok:
        return None

    if features.get("adx", 0) < FILTER_MIN_ADX_ENTRY:
        return None
    if features.get("vol_ratio", 0) < FILTER_MIN_VOL_RATIO_ENTRY:
        return None

    entry_price = live_price
    risk_targets = _entry_risk_targets(direction, entry_price, level, atr_val, base_data)
    if not risk_targets:
        return None

    setup_score = setup.get("setup_score", 0.0)
    trigger_bonus = 0.15 if TRIGGER_CONFIRM_MODE == "retest" else 0.1
    entry_score = clamp(setup_score + trigger_bonus, 0, 1)
    confidence = clamp(50 + entry_score * 45, 50, 95)
    return {
        "entry_price": entry_price,
        "sl": risk_targets["sl"],
        "tp": risk_targets["tp"],
        "confidence": confidence,
        "entry_score": entry_score,
    }


def format_setup_message(setup: Dict) -> str:
    symbol = setup["symbol"]
    direction = setup["direction"]
    level = setup["level"]
    distance_pct = setup.get("distance_pct", 0)
    setup_score = setup.get("setup_score", 0) * 100
    timeframe_line = BASE_TIMEFRAME
    if ENGINE_V2_USE_MTF:
        timeframe_line = f"{BASE_TIMEFRAME} (MTF: {HIGHER_TIMEFRAME})"
    return (
        "◉ СЕТАП (РАНО)\n"
        "━━━━━━━━━━━━━━━━\n"
        f"📌 Пара: {normalize_symbol(symbol)} / USDT\n"
        f"📈 Направление: {direction}\n"
        f"🎯 Уровень: {format_price(level)}\n"
        f"📏 До уровня: {distance_pct:.2f}%\n"
        f"🧠 Готовность: {setup_score:.2f}%\n"
        f"⏱ Таймфрейм: {timeframe_line}\n"
        "━━━━━━━━━━━━━━━━\n"
        "⚠️ Это сетап: вход будет при подтверждении."
    )


def format_entry_message(symbol: str, direction: str, entry: Dict) -> str:
    return (
        "◉ СИГНАЛ\n"
        "━━━━━━━━━━━━━━━━\n"
        f"📌 Пара: {normalize_symbol(symbol)} / USDT\n"
        f"📈 Направление: {direction}\n"
        f"🎯 Вход: {format_price(entry['entry_price'])}\n"
        f"🛑 SL: {format_price(entry['sl'])}\n"
        f"✅ TP: {format_price(entry['tp'])}\n"
        f"🎯 Уверенность: {entry['confidence']:.2f}%\n"
        "━━━━━━━━━━━━━━━━"
    )


def cleanup_open_setups(state: Dict) -> None:
    open_setups = state.get("open_setups", {})
    now = time.time()
    to_delete = []
    for key, setup in open_setups.items():
        if setup.get("expires_at", 0) <= now:
            to_delete.append(key)
            continue
        invalidation = setup.get("invalidation")
        if invalidation and now >= invalidation:
            to_delete.append(key)
    for key in to_delete:
        open_setups.pop(key, None)
    state["open_setups"] = open_setups


def engine_v2_cycle(
    exchange: ccxt.bybit,
    state: Dict,
    send_signals: bool,
    allow_cooldown: bool,
) -> Optional[Dict]:
    last_signal = None
    cleanup_open_setups(state)
    for symbol in SYMBOLS:
        # 15m TTL = 25s, 1h TTL = 600s (anti rate-limit, no behavior change)
        base_parsed = fetch_ohlcv_cached(exchange, symbol, BASE_TIMEFRAME, limit=300, ttl_seconds=25)
        if not base_parsed:
            continue
        base_data = _parsed_to_dict(base_parsed)
        if len(base_data["closes"]) < 220:
            continue
        higher_data = None
        if ENGINE_V2_USE_MTF:
            higher_parsed = fetch_ohlcv_cached(exchange, symbol, HIGHER_TIMEFRAME, limit=300, ttl_seconds=600)
            higher_data = _parsed_to_dict(higher_parsed) if higher_parsed else None
        features = build_features(base_data)
        if not features:
            continue
        regime = detect_regime(features)
        setups = generate_setups(symbol, base_data, higher_data, features, regime) if ENGINE_V2_SETUP_ENABLED else []

        print(f"[ENGINE_V2] {symbol} regime={regime} setups={len(setups)}")

        with state_lock:
            setup_memory = state.setdefault("setup_memory", {})
            entry_memory = state.setdefault("entry_memory", {})
            open_setups = state.setdefault("open_setups", {})

        for setup in setups:
            direction = setup["direction"]
            setup_key = f"{symbol}_{direction}"
            with state_lock:
                last_setup_time = setup_memory.get(setup_key, 0)
            if allow_cooldown and time.time() - last_setup_time < SETUP_COOLDOWN_MINUTES * 60:
                continue
            existing = open_setups.get(setup_key)
            if existing:
                continue
            setup["invalidation"] = time.time() + (SETUP_TTL_MINUTES * 60)
            if send_signals:
                tg_send(format_setup_message(setup))
            with state_lock:
                setup_memory[setup_key] = time.time()
                open_setups[setup_key] = setup
                state["setup_memory"] = setup_memory
                state["open_setups"] = open_setups
                save_state(state)

        if not ENGINE_V2_ENTRY_ENABLED:
            continue

        with state_lock:
            open_setups = state.get("open_setups", {}).copy()
        symbol_setups = [(key, setup) for key, setup in open_setups.items() if setup.get("symbol") == symbol]
        if not symbol_setups:
            continue
        live_price = get_live_price_if_needed(
            exchange,
            symbol,
            base_data["closes"][-1],
            has_active_setup=True,
        )
        if live_price is None:
            continue
        for setup_key, setup in symbol_setups:
            if setup.get("symbol") != symbol:
                continue
            direction = setup["direction"]
            entry_key = f"{symbol}_{direction}"
            with state_lock:
                last_entry_time = entry_memory.get(entry_key, 0)
            if allow_cooldown and time.time() - last_entry_time < COOLDOWN_MINUTES * 60:
                continue
            entry = check_trigger(setup, live_price, base_data, features)
            if not entry:
                continue
            if entry["confidence"] < MIN_CONFIDENCE:
                continue

            if send_signals:
                tg_send(format_entry_message(symbol, direction, entry))

            last_signal = {
                "pair": normalize_symbol(symbol),
                "direction": "UP" if direction == "LONG" else "DOWN",
                "confidence": round(entry["confidence"]),
                "probability": round(entry["confidence"], 2),
                "price": entry["entry_price"],
            }
            with state_lock:
                entry_memory[entry_key] = time.time()
                state["entry_memory"] = entry_memory
                state["last_signal"] = last_signal
                open_setups.pop(setup_key, None)
                state["open_setups"] = open_setups
                save_state(state)
    return last_signal


# ================== SIGNAL LOGIC ==================
def compute_signal(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    volumes: List[float],
) -> Tuple[Optional[str], Dict]:
    if len(closes) < 200:
        return None, {}

    price = closes[-1]
    ema_fast = ema(closes, 50)[-1]
    ema_slow = ema(closes, 200)[-1]
    rsi14 = rsi(closes, 14)[-1]
    adx14 = adx(highs, lows, closes, 14)
    atr14 = atr(highs, lows, closes, 14)

    if adx14 is None or atr14 is None:
        return None, {}

    if len(volumes) < 20:
        return None, {}

    volume_sma20 = sum(volumes[-20:]) / 20
    if volume_sma20 <= 0:
        return None, {}
    volume_ratio = volumes[-1] / volume_sma20

    n = 20
    if len(highs) < n + 1 or len(lows) < n + 1:
        return None, {}
    high_n = max(highs[-n:])
    low_n = min(lows[-n:])
    high_n_prev = max(highs[-n - 1:-1])
    low_n_prev = min(lows[-n - 1:-1])
    breakout_up = price > high_n_prev
    breakout_down = price < low_n_prev

    indicator_values = [price, ema_fast, ema_slow, rsi14, adx14, atr14, volume_sma20, volume_ratio]
    if any(math.isnan(v) for v in indicator_values):
        return None, {}

    trend_long = price > ema_slow and ema_fast > ema_slow
    trend_short = price < ema_slow and ema_fast < ema_slow

    def _overheat_penalty(direction: str) -> float:
        if direction == "UP":
            return clamp((rsi14 - 72) / 8, 0, 1)
        return clamp((28 - rsi14) / 8, 0, 1)

    def _build_signal(direction: str) -> Tuple[Optional[str], Dict]:
        if direction == "UP":
            trend_ok = trend_long
            strength_filters = [
                adx14 >= 22,
                volume_ratio >= 1.20,
                breakout_up,
                rsi14 <= 72,
            ]
        else:
            trend_ok = trend_short
            strength_filters = [
                adx14 >= 22,
                volume_ratio >= 1.20,
                breakout_down,
                rsi14 >= 28,
            ]

        passed = sum(1 for ok in strength_filters if ok)
        if not trend_ok or passed < 3:
            return None, {}

        entry = price
        if direction == "UP":
            sl_struct = low_n
            sl = min(sl_struct, entry - 1.2 * atr14)
        else:
            sl_struct = high_n
            sl = max(sl_struct, entry + 1.2 * atr14)

        risk = abs(entry - sl)
        if risk <= 0:
            return None, {}

        if direction == "UP":
            tp = entry + 3 * risk
        else:
            tp = entry - 3 * risk

        if direction == "UP":
            if not (sl < entry < tp):
                return None, {}
        else:
            if not (tp < entry < sl):
                return None, {}

        rr = abs(tp - entry) / risk
        if rr < 3.0:
            return None, {}

        overheat_penalty = _overheat_penalty(direction)
        breakout_flag = breakout_up if direction == "UP" else breakout_down

        score = (
            1.5 * min(adx14 / 40, 1)
            + 1.0 * min(volume_ratio / 2, 1)
            + 1.2 * min(rr / 5, 1)
            + 0.8 * (1 if breakout_flag else 0)
            - 0.6 * overheat_penalty
        )

        confidence = clamp(
            55
            + 6 * passed
            + 12 * min(adx14 / 40, 1)
            + 8 * min(volume_ratio / 2, 1)
            + 10 * min(rr / 5, 1),
            0,
            99,
        )
        if confidence < MIN_CONFIDENCE:
            return None, {}

        info = {
            "price": entry,
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
            "rsi": rsi14,
            "adx": adx14,
            "atr": atr14,
            "volume_ratio": volume_ratio,
            "breakout": breakout_flag,
            "overheat_penalty": overheat_penalty,
            "confidence": round(confidence),
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "quality_score": score,
        }
        return direction, info

    signal_long, info_long = _build_signal("UP")
    signal_short, info_short = _build_signal("DOWN")

    if signal_long and signal_short:
        if info_long.get("quality_score", 0) >= info_short.get("quality_score", 0):
            return signal_long, info_long
        return signal_short, info_short
    if signal_long:
        return signal_long, info_long
    if signal_short:
        return signal_short, info_short
    return None, {}


def run_signal_cycle(
    exchange: ccxt.bybit,
    state: Dict,
    send_signals: bool,
    allow_cooldown: bool = True,
) -> Optional[Dict]:
    if ENGINE_V2_ENABLED:
        return engine_v2_cycle(exchange, state, send_signals, allow_cooldown)

    last_signal = None
    candidates = []

    def _fetch(symbol: str):
        try:
            # 15m TTL = 25s, 1h TTL = 600s (anti rate-limit, no behavior change)
            ohlcv = fetch_ohlcv_raw_cached(exchange, symbol, TIMEFRAME, limit=300, ttl_seconds=25)
            if not ohlcv:
                return symbol, None, RuntimeError("empty ohlcv")
            df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
            df["ts"] = pd.to_datetime(df["ts"], unit="ms")
            df.set_index("ts", inplace=True)
            return symbol, df, None
        except Exception as e:
            return symbol, None, e

    dfs = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_fetch, s) for s in SYMBOLS]
        for fut in as_completed(futures):
            symbol, df, err = fut.result()
            if err is not None:
                print(f"[DATA] fetch error {symbol}: {err}")
                continue
            dfs[symbol] = df

    for symbol in SYMBOLS:
        try:
            df = dfs.get(symbol)
            if df is None:
                continue
            if len(df) < 200:
                continue
            highs = df["high"].tolist()
            lows = df["low"].tolist()
            closes = df["close"].tolist()
            volumes = df["volume"].tolist()

            signal, info = compute_signal(highs, lows, closes, volumes)

            if not signal:
                continue

            key = f"{symbol}_{signal}"
            with state_lock:
                last_time = state.get(key, 0)

            if allow_cooldown and time.time() - last_time < COOLDOWN_MINUTES * 60:
                continue

            candidates.append({
                "symbol": symbol,
                "signal": signal,
                "info": info,
            })

        except Exception as e:
            # Покажем ошибку в консоли, чтобы ты видел, что не так
            print(f"[{symbol}] ERROR: {e}")

    if not candidates:
        return last_signal

    best = max(candidates, key=lambda item: item["info"].get("quality_score", 0))
    symbol = best["symbol"]
    signal = best["signal"]
    info = best["info"]

    side = "LONG" if signal == "UP" else "SHORT"
    overheat_penalty = info.get("overheat_penalty", 0)
    display_probability = clamp(
        55
        + 0.35 * (info.get("adx", 0) - 18)
        + 8 * (info.get("volume_ratio", 1) - 1)
        + 6 * (info.get("rr", 3) - 3)
        - 10 * overheat_penalty,
        55,
        95,
    ) / 100
    numeric_probability = display_probability * 100
    min_confidence = float(MIN_CONFIDENCE)
    if numeric_probability < min_confidence:
        return last_signal
    prob_line = f"{probability_bar(display_probability)} {display_probability*100:.2f}%"

    price = info["price"]
    sl = info.get("sl")
    tp = info.get("tp")
    pair_text = f"{normalize_symbol(symbol)} / USDT"
    timeframe = TIMEFRAME

    sl_line = f"🛑 SL: {format_price(sl)}\n" if sl is not None else ""
    tp_line = f"🎯 TP: {format_price(tp)}\n\n" if tp is not None else "\n"

    msg = (
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 СИГНАЛ {pair_text}\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🪙 Пара: {pair_text}\n"
        f"📍 Направление: {'ВВЕРХ ⬆️' if side == 'LONG' else 'ВНИЗ ⬇️'}\n"
        f"💰 Цена входа: {format_price(price)}\n"
        f"{sl_line}"
        f"{tp_line}"
        "🎯 Вероятность успеха\n"
        f"{prob_line}\n\n"
        f"⏱ Таймфрейм: {timeframe}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

    if send_signals:
        tg_send(
            msg,
        )

    last_signal = {
        "pair": normalize_symbol(symbol),
        "direction": signal,
        "confidence": info["confidence"],
        "probability": round(numeric_probability, 2),
        "price": info["price"],
    }
    key = f"{symbol}_{signal}"
    with state_lock:
        state["last_signal"] = last_signal
        state[key] = time.time()
        save_state(state)

    return last_signal


# ================== MAIN ==================
def command_loop(state: Dict) -> None:
    global MIN_CONFIDENCE
    update_offset = 0
    BUTTON_TO_COMMAND = {
        "📊 Статус": "/status",
        "⚡ Сейчас": "/now",
        "📌 Сигналы": "/signals",
        "🎯 Confidence": "/confidence",
        "⚙️ SetConfidence": "/setconfidence",
        "⏯ Старт / Пауза": "/toggle",
        "ℹ️ Помощь": "/help",
    }
    CALLBACK_TO_COMMAND = {
        "cmd:status": "/status",
        "cmd:signals": "/signals",
        "cmd:confidence": "/confidence",
        "cmd:setconfidence": "/setconfidence",
        "cmd:toggle": "/toggle",
        "cmd:now": "/now",
    }
    # flush old updates on startup (do not process backlog)
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        r = requests.get(url, params={"timeout": 0}, timeout=10)
        data = r.json()
        if data.get("ok") and data.get("result"):
            update_offset = data["result"][-1]["update_id"] + 1
    except Exception as e:
        print(f"[CMD] flush updates error: {e}")
    print("Command loop started")

    while True:
        try:
            updates = tg_get_updates(update_offset)
            for update in updates:
                update_offset = max(update_offset, update.get("update_id", 0) + 1)
                callback_query = update.get("callback_query")
                if callback_query:
                    callback_id = callback_query.get("id")
                    if callback_id:
                        tg_answer_callback(callback_id)
                    data = callback_query.get("data", "")
                    message = callback_query.get("message", {})
                    chat = message.get("chat", {})
                    chat_id = chat.get("id")
                    if chat_id != TELEGRAM_CHAT_ID:
                        continue
                    cmd = CALLBACK_TO_COMMAND.get(data)
                    if cmd:
                        handle_command(cmd, chat_id, state)
                    continue
                message = update.get("message")
                if not message:
                    continue
                chat = message.get("chat", {})
                chat_id = chat.get("id")
                text = message.get("text", "")
                if chat_id != TELEGRAM_CHAT_ID:
                    continue
                with state_lock:
                    awaiting = state.get("awaiting_confidence", False)
                if awaiting:
                    t = (text or "").strip()
                    if t.isdigit():
                        value = int(t)
                        if 1 <= value <= 99:
                            MIN_CONFIDENCE = value
                            with state_lock:
                                state["min_confidence"] = value
                                state["awaiting_confidence"] = False
                                save_state(state)
                            tg_send(
                                "✅ НАСТРОЙКА ОБНОВЛЕНА\n"
                                "━━━━━━━━━━━━━━━━\n"
                                f"🎯 Мин. уверенность : {MIN_CONFIDENCE}%\n"
                                "━━━━━━━━━━━━━━━━",
                                chat_id=chat_id,
                            )
                        else:
                            tg_send("❌ Введите число 1–99.", chat_id=chat_id)
                    else:
                        tg_send("❌ Введите число 1–99.", chat_id=chat_id)
                    continue
                cmd = None
                if text.startswith("/"):
                    cmd = text
                else:
                    cmd = BUTTON_TO_COMMAND.get(text)
                if cmd:
                    handle_command(cmd, chat_id, state)
        except Exception as e:
            print(f"[telegram] ERROR: {e}")

        time.sleep(1)


def signal_loop(exchange: ccxt.bybit, state: Dict) -> None:
    print("Signal loop started")
    next_run = time.time() + CHECK_EVERY_SECONDS

    while True:
        run_now_chat_id = None
        with state_lock:
            run_now_chat_id = run_now_request.get("chat_id")
            if run_now_chat_id is not None:
                run_now_request["chat_id"] = None

        if run_now_chat_id is not None:
            try:
                last_signal = run_signal_cycle(exchange, state, send_signals=False, allow_cooldown=False)
            except Exception as e:
                print(f"[SIGNAL_LOOP] cycle error: {e}")
                last_signal = None
            if last_signal:
                tg_send(format_now_signal(last_signal), chat_id=run_now_chat_id)
            else:
                tg_send(
                    "⚡ ВНЕОЧЕРЕДНОЙ АНАЛИЗ\n"
                    "━━━━━━━━━━━━━━━━\n"
                    "🔎 Сейчас сигнала нет\n"
                    "━━━━━━━━━━━━━━━━",
                    chat_id=run_now_chat_id,
                )

        with state_lock:
            paused = state.get("paused", False)

        if not paused and time.time() >= next_run:
            try:
                run_signal_cycle(exchange, state, send_signals=True)
            except Exception as e:
                print(f"[SIGNAL_LOOP] cycle error: {e}")
            next_run = time.time() + CHECK_EVERY_SECONDS

        time.sleep(1)


def main() -> None:
    global MIN_CONFIDENCE
    if not TELEGRAM_BOT_TOKEN:
        print(
            "ERROR: TELEGRAM_BOT_TOKEN is not set. "
            "Please set environment variable TELEGRAM_BOT_TOKEN."
        )
        raise SystemExit(1)
    exchange = ccxt.bybit({
        "apiKey": BYBIT_API_KEY,
        "secret": BYBIT_API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},  # фьючерсы (USDT Perpetual)
    })

    with state_lock:
        state = load_state()
        default_confidence = MIN_CONFIDENCE
        state.setdefault("min_confidence", default_confidence)
        state.setdefault("awaiting_confidence", False)
        state.setdefault("paused", False)
        state.setdefault("last_signal", None)
        state.setdefault("setup_memory", {})
        state.setdefault("entry_memory", {})
        state.setdefault("open_setups", {})
        state.setdefault("engine_config_version", 1)
        state.setdefault("news", [])
        state.setdefault("news_seen", {})
        state.setdefault("news_settings", {
            "enabled": NEWS_ENABLED,
            "importance_threshold": NEWS_IMPORTANCE_THRESHOLD,
            "sources": NEWS_SOURCES.copy(),
            "price_check": NEWS_PRICE_CHECK_ENABLED,
        })
        state.setdefault("news_price_last_check", {})
        state.setdefault("news_last_poll_ts", 0)
        try:
            MIN_CONFIDENCE = int(state.get("min_confidence", default_confidence))
        except (TypeError, ValueError):
            MIN_CONFIDENCE = default_confidence
            state["min_confidence"] = default_confidence
        save_state(state)

    # Сообщение при старте (должно прийти всегда)
    print(
        f"[ENGINE] v2={ENGINE_V2_ENABLED} base_tf={BASE_TIMEFRAME} "
        f"mtf={ENGINE_V2_USE_MTF} higher_tf={HIGHER_TIMEFRAME}"
    )
    tg_send(
        "◉ СИСТЕМА ЗАПУЩЕНА\n\n"
        f"🧠 Анализ активов: {len(SYMBOLS)}\n"
        f"⏱ Таймфрейм: {TIMEFRAME}\n"
        f"📊 Минимальная уверенность: {MIN_CONFIDENCE}%\n"
        f"🛡 Антиспам: {COOLDOWN_MINUTES} мин"
    )

    with state_lock:
        news_enabled = state.get("news_settings", {}).get("enabled", NEWS_ENABLED)
    if news_enabled:
        news_thread = threading.Thread(target=news_worker, args=(exchange, state), daemon=True)
        news_thread.start()

    command_thread = threading.Thread(target=command_loop, args=(state,), daemon=True)
    signal_thread = threading.Thread(target=signal_loop, args=(exchange, state), daemon=True)
    command_thread.start()
    signal_thread.start()

    while True:
        time.sleep(5)


if __name__ == "__main__":
    main()
