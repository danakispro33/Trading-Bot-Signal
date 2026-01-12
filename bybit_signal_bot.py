import time
import json
import threading
import os
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

STATE_FILE = "state.json"
state_lock = threading.Lock()
run_now_request = {"chat_id": None}


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
            [{"text": "🏦 Биржа"}],
            [{"text": "⚙️ SetConfidence"}, {"text": "⏸ Пауза"}, {"text": "▶️ Резюм"}],
            [{"text": "ℹ️ Помощь"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
    }


ALLOWED_EXCHANGES = {"bybit", "binance", "okx", "bingx"}


def exchange_keyboard() -> Dict:
    return {
        "inline_keyboard": [
            [
                {"text": "Bybit", "callback_data": "exchange:set:bybit"},
                {"text": "Binance", "callback_data": "exchange:set:binance"},
            ],
            [
                {"text": "OKX", "callback_data": "exchange:set:okx"},
                {"text": "BingX", "callback_data": "exchange:set:bingx"},
            ],
            [
                {"text": "⬅ Назад", "callback_data": "menu:help"},
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
    return (
        "📌 Последний сигнал\n"
        "━━━━━━━━━━━━━━━━\n"
        f"💱 Пара: {last_signal.get('pair', '')}\n"
        f"🔀 Направление: {direction}\n"
        f"🎯 Вероятность: {probability}%\n"
        f"💰 Цена: {last_signal.get('price', '')}\n"
        "━━━━━━━━━━━━━━━━"
    )


def build_exchange_url(exchange_id: str, pair_text: str) -> str:
    p = pair_text.replace(" ", "")
    if "/" in p:
        base, quote = p.split("/", 1)
        symbol = base + quote
    else:
        symbol = p
        if symbol.endswith("USDT"):
            base, quote = symbol[:-4], "USDT"
        else:
            base, quote = symbol, "USDT"

    if exchange_id == "bybit":
        return f"https://www.bybit.com/trade/usdt/{symbol}"
    if exchange_id == "binance":
        return f"https://www.binance.com/en/futures/{symbol}"
    if exchange_id == "bingx":
        return f"https://bingx.com/en/futures/forward/{symbol}"
    if exchange_id == "okx":
        return f"https://www.okx.com/trade-swap/{base.lower()}-{quote.lower()}-swap"

    return f"https://www.bybit.com/trade/usdt/{symbol}"


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
            "ℹ️ Помощь\n"
            "━━━━━━━━━━━━\n"
            "📊 Статус\n"
            "📌 Сигналы\n"
            "🎯 Confidence\n"
            "⚙️ SetConfidence\n"
            "⏸ Пауза\n"
            "▶️ Резюм\n"
            "⚡ Сейчас\n"
            "━━━━━━━━━━━━",
            chat_id=chat_id,
            reply_markup={
                "inline_keyboard": [
                    [
                        {"text": "📊 Статус", "callback_data": "cmd:status"},
                        {"text": "⚡ Сейчас", "callback_data": "cmd:now"},
                    ],
                    [
                        {"text": "🎯 Сигналы", "callback_data": "cmd:signals"},
                        {"text": "🧪 Confidence", "callback_data": "cmd:confidence"},
                    ],
                    [
                        {"text": "⚙️ SetConfidence", "callback_data": "cmd:setconfidence"},
                    ],
                    [
                        {"text": "⏸ Пауза", "callback_data": "cmd:pause"},
                        {"text": "▶️ Резюм", "callback_data": "cmd:resume"},
                    ],
                    [
                        {"text": "📈 Биржа", "callback_data": "menu:exchange"},
                    ],
                ]
            },
        )
        return

    if command == "/exchange":
        tg_send(
            "🏦 Выберите биржу\n"
            "━━━━━━━━━━━━━━━━",
            chat_id=chat_id,
            reply_markup=exchange_keyboard(),
        )
        return

    if command == "/setconfidence":
        if len(parts) == 2 and parts[1].isdigit():
            value = int(parts[1])
            if 0 <= value <= 100:
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
                "Введите значение от 1 до 99\n"
                "Например: 65\n"
                "━━━━━━━━━━━━━━━━",
                chat_id=chat_id,
            )
            return
        tg_send("❌ Введите число от 1 до 99.", chat_id=chat_id)
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
    last_signal = None
    candidates = []

    def _fetch(symbol: str):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=300)
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
        with state_lock:
            ex_id = state.get("exchange", "bybit")
        url = build_exchange_url(ex_id, pair_text)
        tg_send(
            msg,
            reply_markup={
                "inline_keyboard": [
                    [{"text": "Открыть на бирже", "url": url}],
                ]
            },
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
        "🏦 Биржа": "/exchange",
        "⚙️ SetConfidence": "/setconfidence",
        "⏸ Пауза": "/pause",
        "▶️ Резюм": "/resume",
        "ℹ️ Помощь": "/help",
    }
    CALLBACK_TO_COMMAND = {
        "cmd:status": "/status",
        "cmd:signals": "/signals",
        "cmd:confidence": "/confidence",
        "cmd:setconfidence": "/setconfidence",
        "cmd:pause": "/pause",
        "cmd:resume": "/resume",
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
                    if data == "menu:exchange":
                        tg_edit_message(
                            text="🏦 Выберите биржу\n━━━━━━━━━━━━━━━━",
                            chat_id=chat_id,
                            message_id=message.get("message_id"),
                            reply_markup=exchange_keyboard(),
                        )
                        continue
                    if data == "menu:help":
                        handle_command("/help", chat_id, state)
                        continue
                    if data.startswith("exchange:set:"):
                        exchange_id = data.split(":")[-1].lower().strip()
                        if exchange_id in ALLOWED_EXCHANGES:
                            with state_lock:
                                state["exchange"] = exchange_id
                                save_state(state)
                            upper = exchange_id.upper()
                            tg_send(
                                f"🤖 Биржа {upper} выбрана\n"
                                f"Ваши сигналы будут переходить на биржу {upper}.",
                                chat_id=chat_id,
                            )
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
                            tg_send("❌ Введите число от 1 до 99.", chat_id=chat_id)
                    else:
                        tg_send("❌ Введите число от 1 до 99.", chat_id=chat_id)
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
        state.setdefault("exchange", "bybit")
        try:
            MIN_CONFIDENCE = int(state.get("min_confidence", default_confidence))
        except (TypeError, ValueError):
            MIN_CONFIDENCE = default_confidence
            state["min_confidence"] = default_confidence
        save_state(state)

    # Сообщение при старте (должно прийти всегда)
    tg_send(
        "◉ СИСТЕМА ЗАПУЩЕНА\n\n"
        f"🧠 Анализ активов: {len(SYMBOLS)}\n"
        f"⏱ Таймфрейм: {TIMEFRAME}\n"
        f"📊 Минимальная уверенность: {MIN_CONFIDENCE}%\n"
        f"🛡 Антиспам: {COOLDOWN_MINUTES} мин"
    )

    command_thread = threading.Thread(target=command_loop, args=(state,), daemon=True)
    signal_thread = threading.Thread(target=signal_loop, args=(exchange, state), daemon=True)
    command_thread.start()
    signal_thread.start()

    while True:
        time.sleep(5)


if __name__ == "__main__":
    main()
