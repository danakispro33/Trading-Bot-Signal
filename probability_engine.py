import json
import os
import tempfile
from typing import Dict, Optional


def format_percent(p: float, decimals: int = 2) -> str:
    return f"{p * 100:.{decimals}f}%"


def _default_stats() -> Dict:
    return {"meta": {"min_samples": 50}, "buckets": {}}


def load_stats(path: str = "stats.json") -> Dict:
    if not os.path.exists(path):
        stats = _default_stats()
        _write_stats(path, stats)
        return stats
    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        data = _default_stats()
        _write_stats(path, data)
    if "meta" not in data:
        data["meta"] = {"min_samples": 50}
    if "buckets" not in data:
        data["buckets"] = {}
    return data


def _write_stats(path: str, stats: Dict) -> None:
    directory = os.path.dirname(path) or "."
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=directory) as tmp:
        json.dump(stats, tmp, ensure_ascii=False, indent=2)
        tmp_path = tmp.name
    os.replace(tmp_path, path)


def make_key(symbol: str, timeframe: str, side: str) -> str:
    clean_symbol = symbol.split(":")[0].replace("/", "")
    return f"{clean_symbol}|{timeframe}|{side}"


def get_probability(key: str, min_samples: int = 50) -> Optional[float]:
    stats = load_stats()
    bucket = stats.get("buckets", {}).get(key, {})
    total = bucket.get("total", 0)
    wins = bucket.get("wins", 0)
    if total >= min_samples and total > 0:
        return wins / total
    return None


def record_outcome(key: str, outcome: str) -> None:
    if outcome not in {"win", "loss"}:
        return
    stats = load_stats()
    buckets = stats.setdefault("buckets", {})
    bucket = buckets.setdefault(key, {"wins": 0, "total": 0})
    bucket["total"] = bucket.get("total", 0) + 1
    if outcome == "win":
        bucket["wins"] = bucket.get("wins", 0) + 1
    _write_stats("stats.json", stats)
