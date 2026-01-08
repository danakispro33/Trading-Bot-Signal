import time
import json
import threading
from typing import List, Dict, Optional, Tuple

import ccxt
import requests


# ================== ТВОИ ДАННЫЕ ==================
TELEGRAM_BOT_TOKEN = "8320181117:AAEPakY2UCJQyFzkw4xvFkpLYgsm-Fo7Pwg"
TELEGRAM_CHAT_ID = 5878255923

# Bybit API (для просто сигналов можно оставить пустым)
BYBIT_API_KEY = ""
BYBIT_API_SECRET = ""


# ================== НАСТРОЙКИ ==================
SYMBOLS = [
    "XRP/USDT:USDT",
    "SOL/USDT:USDT",
    "BTC/USDT:USDT",
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


# ================== SIGNAL LOGIC ==================
def compute_signal(closes: List[float]) -> Tuple[Optional[str], Dict]:
    # Нужно достаточно свечей для EMA200
    if len(closes) < 220:
        return None, {}

    price = closes[-1]
    ema50 = ema(closes, 50)[-1]
    ema200 = ema(closes, 200)[-1]
    rsi14 = rsi(closes, 14)[-1]

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
        "up_pct": round(up_pct),
        "down_pct": round(down_pct),
        "confidence": round(confidence),
    }

    if confidence >= MIN_CONFIDENCE:
        return direction, info

    return None, info


def run_signal_cycle(
    exchange: ccxt.bybit,
    state: Dict,
    send_signals: bool,
    allow_cooldown: bool = True,
) -> Optional[Dict]:
    last_signal = None
    for symbol in SYMBOLS:
        try:
            candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=300)
            closes = [c[4] for c in candles]

            signal, info = compute_signal(closes)

            if not signal:
                continue

            key = f"{symbol}_{signal}"
            with state_lock:
                last_time = state.get(key, 0)

            if allow_cooldown and time.time() - last_time < COOLDOWN_MINUTES * 60:
                continue

            direction_text = "📈 Потенциальное ПОВЫШЕНИЕ" if signal == "UP" else "📉 Потенциальное ПОНИЖЕНИЕ"

            msg = (
                f"{direction_text}\n"
                f"Пара: {symbol}\n"
                f"TF: {TIMEFRAME}\n"
                f"Цена: {info['price']}\n"
                f"Вверх: {info['up_pct']}% | Вниз: {info['down_pct']}%\n"
                f"EMA50: {round(info['ema50'], 6)}\n"
                f"EMA200: {round(info['ema200'], 6)}\n"
                f"RSI14: {round(info['rsi'], 2)}\n"
                f"Уверенность: {info['confidence']}%"
            )

            if send_signals:
                tg_send(msg)

            last_signal = {
                "pair": normalize_symbol(symbol),
                "direction": signal,
                "confidence": info["confidence"],
                "price": info["price"],
            }
            with state_lock:
                state["last_signal"] = last_signal
                state[key] = time.time()
                save_state(state)

        except Exception as e:
            # Покажем ошибку в консоли, чтобы ты видел, что не так
            print(f"[{symbol}] ERROR: {e}")

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
    next_run = time.time()

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
        "🤖 Бот запущен.\n"
        "Монеты: XRP / SOL / BTC\n"
        "TF: 15m\n"
        f"Мин. уверенность: {MIN_CONFIDENCE}%\n"
        f"Антиспам: {COOLDOWN_MINUTES} мин"
    )

    command_thread = threading.Thread(target=command_loop, args=(state,), daemon=True)
    signal_thread = threading.Thread(target=signal_loop, args=(exchange, state), daemon=True)
    command_thread.start()
    signal_thread.start()

    while True:
        time.sleep(5)


if __name__ == "__main__":
    main()
