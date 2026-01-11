import time
import json
import threading
import os
from typing import List, Dict, Optional, Tuple

import ccxt
import requests
from probability_engine import get_probability, load_stats, make_key


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
def tg_send(text: str, chat_id: Optional[int] = None) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": chat_id or TELEGRAM_CHAT_ID, "text": text},
        timeout=15
    )
    # Если Telegram вернул ошибку — покажем в консоли
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")


def tg_get_updates(offset: int) -> List[Dict]:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    r = requests.get(url, params={"offset": offset, "timeout": 15}, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")
    data = r.json()
    return data.get("result", [])


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
        return "📈 Последний сигнал:\nПока нет сигналов"

    direction_map = {"UP": "ВВЕРХ", "DOWN": "ВНИЗ"}
    direction = direction_map.get(last_signal.get("direction"), last_signal.get("direction", ""))
    return (
        "📈 Последний сигнал:\n"
        f"Пара: {last_signal.get('pair', '')}\n"
        f"Направление: {direction}\n"
        f"Вероятность: {last_signal.get('confidence', '')}%\n"
        f"Цена: {last_signal.get('price', '')}"
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


def compute_display_probability(probability: Optional[float], info: Dict) -> float:
    if probability is not None:
        return clamp(probability, 0.01, 1.0)

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
            "🤖 Crypto Signal Bot запущен\n"
            f"Пары: {format_pairs(' / ')}\n"
            f"TF: {TIMEFRAME}",
            chat_id=chat_id,
        )
        return

    if command == "/status":
        tg_send(
            "📊 Статус бота:\n"
            f"Пары: {format_pairs(', ')}\n"
            f"TF: {TIMEFRAME}\n"
            f"Проверка: каждые {CHECK_EVERY_SECONDS} сек\n"
            f"Мин. вероятность: {MIN_CONFIDENCE}%",
            chat_id=chat_id,
        )
        return

    if command == "/signals":
        with state_lock:
            last_signal = state.get("last_signal")
        tg_send(format_last_signal(last_signal), chat_id=chat_id)
        return

    if command == "/confidence":
        tg_send(f"🎯 Минимальная вероятность сигнала: {MIN_CONFIDENCE}%", chat_id=chat_id)
        return

    if command == "/help":
        tg_send(
            "📖 Доступные команды:\n"
            "/start\n"
            "/status\n"
            "/signals\n"
            "/confidence\n"
            "/help\n"
            "/setconfidence 65\n"
            "/pause\n"
            "/resume\n"
            "/now",
            chat_id=chat_id,
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
                tg_send(f"✅ Установлено: {value}%", chat_id=chat_id)
                return
        tg_send("❌ Используй: /setconfidence 65", chat_id=chat_id)
        return

    if command == "/pause":
        with state_lock:
            state["paused"] = True
            save_state(state)
        tg_send("⏸️ Сигналы на паузе", chat_id=chat_id)
        return

    if command == "/resume":
        with state_lock:
            state["paused"] = False
            save_state(state)
        tg_send("▶️ Сигналы включены", chat_id=chat_id)
        return

    if command == "/now":
        with state_lock:
            run_now_request["chat_id"] = chat_id
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
def compute_signal(highs: List[float], lows: List[float], closes: List[float]) -> Tuple[Optional[str], Dict]:
    # Нужно достаточно свечей для EMA200
    if len(closes) < 220:
        return None, {}

    price = closes[-1]
    ema50 = ema(closes, 50)[-1]
    ema200 = ema(closes, 200)[-1]
    rsi14 = rsi(closes, 14)[-1]
    adx14 = adx(highs, lows, closes, 14)
    atr14 = atr(highs, lows, closes, 14)

    if adx14 is None or atr14 is None:
        return None, {}

    if adx14 < 20:
        return None, {"adx": adx14}

    # Тренд
    trend_up = 1 if ema50 > ema200 else 0
    trend_down = 1 - trend_up

    # Цена относительно EMA50
    price_up = 1 if price > ema50 else 0
    price_down = 1 - price_up

    # RSI как “импульс”
    rsi_up = min(max((rsi14 - 50) / 20, 0), 1)
    rsi_down = min(max((50 - rsi14) / 20, 0), 1)

    # “сила” движения (в пределах 1%)
    strength = min(abs(price - ema50) / ema50 / 0.01, 1)

    # Итоговые “очки”
    score_up = (0.40 * trend_up) + (0.25 * price_up) + (0.25 * rsi_up) + (0.10 * strength * price_up)
    score_down = (0.40 * trend_down) + (0.25 * price_down) + (0.25 * rsi_down) + (0.10 * strength * price_down)

    total = score_up + score_down
    if total == 0:
        return None, {}

    up_pct = (score_up / total) * 100
    down_pct = 100 - up_pct

    direction = "UP" if up_pct > down_pct else "DOWN"
    confidence = max(up_pct, down_pct)

    info = {
        "price": price,
        "ema50": ema50,
        "ema200": ema200,
        "rsi": rsi14,
        "adx": adx14,
        "atr": atr14,
        "up_pct": round(up_pct),
        "down_pct": round(down_pct),
        "confidence": round(confidence),
    }

    if confidence < MIN_CONFIDENCE:
        return None, info

    if direction == "UP":
        confirmed = closes[-1] > highs[-2] or closes[-1] > closes[-2] + 0.3 * atr14
    else:
        confirmed = closes[-1] < lows[-2] or closes[-1] < closes[-2] - 0.3 * atr14

    if not confirmed:
        return None, info

    if direction == "UP":
        sl = price - 1.5 * atr14
        tp = price + 2.5 * atr14
    else:
        sl = price + 1.5 * atr14
        tp = price - 2.5 * atr14

    rr = abs(tp - price) / max(abs(price - sl), 1e-9)
    if rr < 1.6:
        return None, info

    quality_score = (
        (confidence / 100) * 0.45
        + min(adx14 / 40, 1) * 0.35
        + min(rr / 3, 1) * 0.20
    )

    info.update({
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "quality_score": quality_score,
    })

    return direction, info

    return None, info


def run_signal_cycle(
    exchange: ccxt.bybit,
    state: Dict,
    send_signals: bool,
    allow_cooldown: bool = True,
) -> Optional[Dict]:
    last_signal = None
    candidates = []
    for symbol in SYMBOLS:
        try:
            candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=300)
            highs = [c[2] for c in candles]
            lows = [c[3] for c in candles]
            closes = [c[4] for c in candles]

            signal, info = compute_signal(highs, lows, closes)

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

    stats = load_stats()
    min_samples = stats.get("meta", {}).get("min_samples", 50)
    side = "LONG" if signal == "UP" else "SHORT"
    probability_key = make_key(symbol, TIMEFRAME, side)
    probability = get_probability(probability_key, min_samples=min_samples)
    display_probability = compute_display_probability(probability, info)
    prob_line = f"{probability_bar(display_probability)} {display_probability*100:.2f}%"

    price = info["price"]
    timeframe = TIMEFRAME

    msg = (
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📈 ТОРГОВЫЙ СИГНАЛ\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🪙 Пара: {symbol} / USDT\n"
        f"📍 Направление: {'ВВЕРХ ⬆️' if side == 'LONG' else 'ВНИЗ ⬇️'}\n"
        f"💰 Цена входа: {price}\n"
        f"🛑 SL: {info['sl']}\n"
        f"🎯 TP: {info['tp']}\n\n"
        "🎯 Вероятность успеха\n"
        f"{prob_line}\n\n"
        f"⏱ Таймфрейм: {timeframe}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

    if send_signals:
        tg_send(msg)

    last_signal = {
        "pair": normalize_symbol(symbol),
        "direction": signal,
        "confidence": info["confidence"],
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
    update_offset = 0
    print("Command loop started")

    while True:
        try:
            updates = tg_get_updates(update_offset)
            for update in updates:
                update_offset = max(update_offset, update.get("update_id", 0) + 1)
                message = update.get("message")
                if not message:
                    continue
                chat = message.get("chat", {})
                chat_id = chat.get("id")
                text = message.get("text", "")
                if chat_id != TELEGRAM_CHAT_ID:
                    continue
                if text.startswith("/"):
                    handle_command(text, chat_id, state)
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
            last_signal = run_signal_cycle(exchange, state, send_signals=False, allow_cooldown=False)
            if last_signal:
                tg_send(format_last_signal(last_signal), chat_id=run_now_chat_id)
            else:
                tg_send("🔎 Сейчас сигнала нет", chat_id=run_now_chat_id)

        with state_lock:
            paused = state.get("paused", False)

        if not paused and time.time() >= next_run:
            run_signal_cycle(exchange, state, send_signals=True)
            next_run = time.time() + CHECK_EVERY_SECONDS

        time.sleep(1)


def main() -> None:
    global MIN_CONFIDENCE
    exchange = ccxt.bybit({
        "apiKey": BYBIT_API_KEY,
        "secret": BYBIT_API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},  # фьючерсы (USDT Perpetual)
    })

    with state_lock:
        state = load_state()
        state.setdefault("min_confidence", MIN_CONFIDENCE)
        state.setdefault("paused", False)
        state.setdefault("last_signal", None)
        save_state(state)
        MIN_CONFIDENCE = state.get("min_confidence", MIN_CONFIDENCE)

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
