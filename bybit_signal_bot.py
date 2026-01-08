import time
import json
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


# ================== TELEGRAM ==================
def tg_send(text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
        timeout=15
    )
    # Если Telegram вернул ошибку — покажем в консоли
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")


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


# ================== MAIN ==================
def main() -> None:
    exchange = ccxt.bybit({
        "apiKey": BYBIT_API_KEY,
        "secret": BYBIT_API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},  # фьючерсы (USDT Perpetual)
    })

    state = load_state()

    # Сообщение при старте (должно прийти всегда)
    tg_send(
        "🤖 Бот запущен.\n"
        "Монеты: XRP / SOL / BTC\n"
        "TF: 15m\n"
        f"Мин. уверенность: {MIN_CONFIDENCE}%\n"
        f"Антиспам: {COOLDOWN_MINUTES} мин"
    )

    while True:
        for symbol in SYMBOLS:
            try:
                candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=300)
                closes = [c[4] for c in candles]

                signal, info = compute_signal(closes)

                if not signal:
                    continue

                key = f"{symbol}_{signal}"
                last_time = state.get(key, 0)

                if time.time() - last_time < COOLDOWN_MINUTES * 60:
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

                tg_send(msg)
                state[key] = time.time()
                save_state(state)

            except Exception as e:
                # Покажем ошибку в консоли, чтобы ты видел, что не так
                print(f"[{symbol}] ERROR: {e}")

        time.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    main()
