from typing import Dict, Optional


def _sl_atr_multiplier(leverage: int) -> float:
    if leverage <= 10:
        return 1.6
    if leverage <= 25:
        return 1.3
    if leverage <= 50:
        return 1.1
    return 0.9


def _min_rr(leverage: int) -> float:
    if leverage <= 10:
        return 1.6
    if leverage <= 25:
        return 1.8
    if leverage <= 50:
        return 2.0
    return 2.2


def evaluate_risk(
    direction: str,
    entry_price: float,
    atr: Optional[float],
    swing_high: float,
    swing_low: float,
    leverage: int,
    position_usd: float,
) -> Dict:
    if entry_price <= 0 or leverage <= 0 or position_usd <= 0:
        return {
            "ok": False,
            "sl": None,
            "tp": None,
            "liq_price": None,
            "risk_usd": 0.0,
            "profit_usd": 0.0,
            "reason": "некорректные входные данные",
        }
    if atr is None or atr <= 0:
        return {
            "ok": False,
            "sl": None,
            "tp": None,
            "liq_price": None,
            "risk_usd": 0.0,
            "profit_usd": 0.0,
            "reason": "ATR недоступен",
        }

    is_long = direction.upper() == "LONG"
    liq_price = entry_price * (1 - 1 / leverage) if is_long else entry_price * (1 + 1 / leverage)
    liq_distance = abs(entry_price - liq_price)
    if liq_distance <= 0:
        return {
            "ok": False,
            "sl": None,
            "tp": None,
            "liq_price": liq_price,
            "risk_usd": 0.0,
            "profit_usd": 0.0,
            "reason": "недостаточная дистанция до ликвидации",
        }

    buffer_distance = liq_distance * 0.15
    min_distance = max(atr * 0.2, liq_distance * 0.1)
    max_distance = liq_distance * 0.85
    if is_long:
        liq_safe = liq_price + buffer_distance
        sl_struct = swing_low
        sl_atr = entry_price - atr * _sl_atr_multiplier(leverage)
    else:
        liq_safe = liq_price - buffer_distance
        sl_struct = swing_high
        sl_atr = entry_price + atr * _sl_atr_multiplier(leverage)

    sl_candidates = [sl_struct, sl_atr, liq_safe]
    valid_candidates = []
    for sl in sl_candidates:
        if sl is None:
            continue
        if is_long:
            if not (liq_safe < sl < entry_price):
                continue
        else:
            if not (entry_price < sl < liq_safe):
                continue
        distance = abs(entry_price - sl)
        if distance < min_distance:
            continue
        if distance > max_distance:
            continue
        valid_candidates.append(sl)

    if not valid_candidates:
        return {
            "ok": False,
            "sl": None,
            "tp": None,
            "liq_price": liq_price,
            "risk_usd": 0.0,
            "profit_usd": 0.0,
            "reason": "слишком высокий риск для выбранного плеча",
        }

    sl = min(valid_candidates, key=lambda value: abs(entry_price - value))
    risk_distance = abs(entry_price - sl)
    rr = _min_rr(leverage)
    if is_long:
        tp = entry_price + risk_distance * rr
    else:
        tp = entry_price - risk_distance * rr

    notional = position_usd * leverage
    risk_usd = notional * risk_distance / entry_price
    profit_usd = notional * abs(tp - entry_price) / entry_price

    return {
        "ok": True,
        "sl": sl,
        "tp": tp,
        "liq_price": liq_price,
        "risk_usd": risk_usd,
        "profit_usd": profit_usd,
        "reason": "",
    }
