from __future__ import annotations

import csv
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, Iterable, List, Optional

import re
import requests


USER_AGENT = "Mozilla/5.0 (compatible; MSXStockAnalyzer/1.0; +https://www.msx.om/)"
REQUEST_TIMEOUT = 20
CACHE_TTL_SECONDS = 900
TECH_CACHE_TTL_SECONDS = 3600
MAX_TECH_WORKERS = 18

MSX_URLS = {
    "companies_list": "https://www.msx.om/companies.aspx/List",
    "financial_performance_list": "https://www.msx.om/Companies-Fin-Pref.aspx/List",
    "market_watch_data": "https://www.msx.om/APIPage.aspx/GetPageData",
    "company_chart": "https://www.msx.om/company-chart-data.aspx?s={symbol}",
}

_CACHE = {"expires_at": 0.0, "payload": None}
_TECH_CACHE: Dict[str, Dict[str, object]] = {}
_SESSION = requests.Session()
_SESSION.trust_env = False
_WARMUP_LOCK = threading.Lock()
_WARMUP_ACTIVE = False


def fetch_msx_dataset(sample_csv_path: Path) -> Dict[str, object]:
    now = time.time()
    sample_rows = load_sample_csv(sample_csv_path)

    try:
        if _CACHE["payload"] and now < _CACHE["expires_at"]:
            live_rows = _CACHE["payload"]["rows"]
            fetched_at = _CACHE["payload"]["fetchedAt"]
        else:
            live_rows = fetch_live_rows()
            fetched_at = datetime.now(timezone.utc).isoformat()
            _CACHE["payload"] = {"rows": live_rows, "fetchedAt": fetched_at}
            _CACHE["expires_at"] = now + CACHE_TTL_SECONDS

        if not live_rows:
            return {
                "stocks": sample_rows,
                "source": "sample",
                "warning": "Live MSX pages were reached but no compatible stock rows were extracted. Showing the built-in sample dataset instead.",
                "fetchedAt": fetched_at,
                "officialSources": list(MSX_URLS.values()),
                "technicalReady": 0,
                "technicalTotal": len(sample_rows),
            }

        stocks = apply_cached_technicals(live_rows)
        ready = sum(1 for row in stocks if row.get("rsi14", 0) > 0)
        total = len(stocks)
        if ready < total:
            schedule_technical_warmup(stocks)
            warning = (
                f"Live MSX data loaded. Technical indicators are warming in the background "
                f"({ready}/{total} stocks ready). Refresh again shortly for more RSI coverage."
            )
        else:
            warning = "Live MSX data and technical indicators are fully loaded."

        return {
            "stocks": stocks,
            "source": "live",
            "warning": warning,
            "fetchedAt": fetched_at,
            "officialSources": list(MSX_URLS.values()),
            "technicalReady": ready,
            "technicalTotal": total,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "stocks": sample_rows,
            "source": "sample",
            "warning": f"Live MSX fetch failed and the app fell back to sample data. Reason: {exc}",
            "fetchedAt": datetime.now(timezone.utc).isoformat(),
            "officialSources": list(MSX_URLS.values()),
            "technicalReady": 0,
            "technicalTotal": len(sample_rows),
        }


def fetch_msx_chart(symbol: str) -> Dict[str, object]:
    technicals = get_symbol_technicals(symbol, force_refresh=False, include_series=True)
    if not technicals:
        return {
            "symbol": symbol,
            "series": [],
            "indicators": {},
            "warning": "No chart data was returned by the official MSX chart endpoint for this symbol.",
        }

    return {
        "symbol": symbol,
        "series": technicals.get("series", []),
        "indicators": {
            "rsi14": technicals.get("rsi14", 0.0),
            "sma20": technicals.get("sma20", 0.0),
            "bollingerUpper": technicals.get("bollingerUpper", 0.0),
            "bollingerMiddle": technicals.get("bollingerMiddle", 0.0),
            "bollingerLower": technicals.get("bollingerLower", 0.0),
            "bollingerPercentB": technicals.get("bollingerPercentB", 0.0),
            "volumeAvg20": technicals.get("volumeAvg20", 0.0),
            "volumeVsAvg20": technicals.get("volumeVsAvg20", 0.0),
        },
        "warning": "",
    }


# MSX market board codes (confirmed working)
MSX_MARKET_BOARDS = [
    {"sHiddens": "0|0|false||0||0"},   # Main Market     (~180 stocks)
    {"sHiddens": "1|0|false||0||0"},   # Parallel Market (~24 stocks)
    {"sHiddens": "2|0|false||0||0"},   # Third Market    (~72 stocks)
]


def fetch_live_rows() -> List[Dict[str, object]]:
    companies   = fetch_webmethod_json(MSX_URLS["companies_list"], {})
    performance = fetch_webmethod_json(MSX_URLS["financial_performance_list"], {})

    # Fetch all market boards and merge results
    all_market_rows: List[Dict[str, object]] = []
    for board_params in MSX_MARKET_BOARDS:
        try:
            market_payload = fetch_webmethod_json(MSX_URLS["market_watch_data"], board_params)
            board_rows = normalize_market_rows(market_payload)
            print(f"[MSX] Board {board_params['sHiddens'].split('|')[0]}: {len(board_rows)} stocks")
            all_market_rows.extend(board_rows)
        except Exception as exc:
            print(f"[MSX] Board {board_params['sHiddens']} fetch failed: {exc}")
            continue

    # Debug: print first performance item keys to help fix PE/yield mapping
    if isinstance(performance, list) and performance:
        import json as _json
        print(f"[MSX DEBUG] financial_performance first item:")
        print(_json.dumps(performance[0], ensure_ascii=False, indent=2))

    company_rows     = normalize_company_rows(companies)
    performance_rows = normalize_performance_rows(performance)
    market_rows      = dedupe_by_ticker(all_market_rows)

    print(f"[MSX] Sources: companies={len(company_rows)}, performance={len(performance_rows)}, market={len(market_rows)}")

    # If market fetch got very few rows, use company list as base
    # (companies endpoint has all listed securities)
    merged = merge_rows(company_rows, performance_rows)
    merged = merge_rows(merged, market_rows)
    print(f"[MSX] After merge: {len(merged)} total rows")
    # Keep all merged rows — don't filter by price/volume
    # Some stocks may not have live prices at the moment of fetch
    equity_rows = filter_bonds(merged)
    print(f"[MSX] Total merged: {len(merged)}, after bond filter: {len(equity_rows)}")
    return equity_rows


def fetch_webmethod_json(url: str, payload: Dict[str, object]) -> object:
    response = _SESSION.post(
        url,
        timeout=REQUEST_TIMEOUT,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Referer": "https://www.msx.om/market-watch-custom.aspx",
            "Origin": "https://www.msx.om",
            "X-Requested-With": "XMLHttpRequest",
        },
        json=payload,
    )
    response.raise_for_status()
    raw = response.json()
    data = raw.get("d") if isinstance(raw, dict) else raw
    if isinstance(data, str):
        if url.endswith("APIPage.aspx/GetPageData") and "|asc|" in data:
            data = data.rsplit("|", 2)[0]
        return json.loads(data)
    return data


def normalize_company_rows(companies: object) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if not isinstance(companies, list):
        return rows

    for item in companies:
        ticker = clean_text(str(item.get("Symbol", "")))
        if not ticker:
            continue
        rows.append(
            {
                "ticker": ticker,
                "company": clean_text(str(item.get("LongNameEn", ""))) or ticker,
                "sector": sector_code_to_name(item.get("Sector")),
            }
        )
    return dedupe_by_ticker(rows)


def normalize_performance_rows(rows: object) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    if not isinstance(rows, list):
        return normalized

    for item in rows:
        ticker = clean_text(str(item.get("Symbol", "")))
        if not ticker:
            continue
        normalized.append(
            {
                "ticker": ticker,
                "company": clean_text(str(item.get("LongNameEn", ""))) or ticker,
                "sector": clean_text(str(item.get("NameEn", ""))) or sector_code_to_name(item.get("Sector")),
                "earningsGrowth": parse_number(item.get("Change_Per")),
                "peRatio": parse_number(
                    item.get("PE") or item.get("PERatio") or item.get("P_E") or item.get("PriceEarning")
                ),
                "pbRatio": parse_number(
                    item.get("PB") or item.get("PBRatio") or item.get("P_B") or item.get("PriceBook")
                ),
                "dividendYield": parse_number(
                    item.get("DividendYield") or item.get("Div_Yield") or item.get("DivYield") or item.get("Yield")
                ),
                "debtToEquity": parse_number(
                    item.get("DebtToEquity") or item.get("D_E") or item.get("Debt_Equity") or item.get("DebtEquity")
                ),
                "roe": parse_number(
                    item.get("ROE") or item.get("ReturnOnEquity") or item.get("Return_Equity")
                ),
                "currentRatio": parse_number(
                    item.get("CurrentRatio") or item.get("Current_Ratio") or item.get("CurrRatio")
                ),
                "marketCapM": parse_number(
                    item.get("MarketCap") or item.get("Market_Cap") or item.get("Mkt_Cap") or item.get("MktCap")
                ),
            }
        )
    return dedupe_by_ticker(normalized)


def normalize_market_rows(payload: object) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    groups = payload.get("Data", []) if isinstance(payload, dict) else []

    for group in groups:
        sector_name = clean_text(str(group.get("SectorNameEn", ""))) or "Unknown"
        for item in group.get("MarketList", []):
            ticker = clean_text(str(item.get("Symbol", "")))
            if not ticker:
                continue

            price = parse_number(item.get("ClosePrice") or item.get("LTP"))
            prev_close = parse_number(item.get("PrevClose"))
            daily_change = parse_number(item.get("Change"))
            if not daily_change and prev_close > 0 and price > 0:
                daily_change = ((price - prev_close) / prev_close) * 100

            normalized.append(
                {
                    "ticker": ticker,
                    "company": clean_text(str(item.get("LongNameEn", ""))) or ticker,
                    "sector": sector_name,
                    "price": price,
                    "dailyChange": daily_change,
                    "priceChange1Y": 0.0,
                    "bidPrice": parse_number(item.get("BidPrice")),
                    "bidVolume": parse_number(item.get("BidVolume")),
                    "askPrice": parse_number(item.get("AskPrice")),
                    "askVolume": parse_number(item.get("AskVolume")),
                    "volume": parse_number(item.get("Volume")),
                    "turnover": parse_number(item.get("Turnover")),
                    "trades": parse_number(item.get("NoOfTrades")),
                }
            )
    return dedupe_by_ticker(normalized)


def merge_rows(base_rows: List[Dict[str, object]], extra_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    merged: Dict[str, Dict[str, object]] = {}

    for row in base_rows:
        merged[row["ticker"]] = fill_defaults(row)

    for row in extra_rows:
        ticker = row["ticker"]
        entry = merged.setdefault(ticker, fill_defaults({"ticker": ticker}))
        for key, value in row.items():
            if key == "ticker":
                continue
            if value in ("", None):
                continue
            # Don't overwrite a real non-zero numeric value with a zero
            existing = entry.get(key)
            if isinstance(value, float) and value == 0.0 and isinstance(existing, float) and existing != 0.0:
                continue
            entry[key] = value

    output = [fill_defaults(row) for row in merged.values() if row.get("ticker")]
    output.sort(key=lambda item: item["ticker"])
    return output


def enrich_with_technicals(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    technicals_map: Dict[str, Dict[str, object]] = {}
    symbols = [row["ticker"] for row in rows if row.get("ticker")]

    with ThreadPoolExecutor(max_workers=MAX_TECH_WORKERS) as executor:
        futures = {
            executor.submit(get_symbol_technicals, symbol, False, False): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                technicals_map[symbol] = future.result() or {}
            except Exception:
                technicals_map[symbol] = {}

    enriched: List[Dict[str, object]] = []
    for row in rows:
        technicals = technicals_map.get(row["ticker"], {})
        merged = fill_defaults({**row, **technicals})

        if merged["price"] <= 0 and technicals.get("latestClose", 0.0) > 0:
            merged["price"] = technicals["latestClose"]
        if merged["volume"] <= 0 and technicals.get("latestVolume", 0.0) > 0:
            merged["volume"] = technicals["latestVolume"]
        if merged["priceChange1Y"] == 0.0 and technicals.get("priceChange1Y", 0.0):
            merged["priceChange1Y"] = technicals["priceChange1Y"]

        bid_volume = merged["bidVolume"]
        ask_volume = merged["askVolume"]
        total_interest = bid_volume + ask_volume
        merged["buyPressure"] = round((bid_volume / total_interest) * 100, 1) if total_interest > 0 else 0.0
        merged["demandScore"] = round(
            clamp_number((merged["buyPressure"] * 0.65) + (min(merged["volumeVsAvg20"], 3.0) / 3.0 * 35.0), 0.0, 100.0),
            1,
        )
        enriched.append(merged)

    return enriched


def apply_cached_technicals(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    enriched: List[Dict[str, object]] = []
    for row in rows:
        cached = _TECH_CACHE.get(row["ticker"], {})
        merged = fill_defaults({**row, **build_cached_response(cached, include_series=False)})
        if merged["price"] <= 0 and merged["latestClose"] > 0:
            merged["price"] = merged["latestClose"]
        if merged["volume"] <= 0 and merged["latestVolume"] > 0:
            merged["volume"] = merged["latestVolume"]
        if merged["priceChange1Y"] == 0.0 and merged.get("priceChange1Y", 0.0):
            merged["priceChange1Y"] = merged["priceChange1Y"]

        bid_volume = merged["bidVolume"]
        ask_volume = merged["askVolume"]
        total_interest = bid_volume + ask_volume
        merged["buyPressure"] = round((bid_volume / total_interest) * 100, 1) if total_interest > 0 else 0.0
        merged["demandScore"] = round(
            clamp_number((merged["buyPressure"] * 0.65) + (min(merged["volumeVsAvg20"], 3.0) / 3.0 * 35.0), 0.0, 100.0),
            1,
        )
        enriched.append(merged)
    return enriched


def schedule_technical_warmup(rows: List[Dict[str, object]]) -> None:
    global _WARMUP_ACTIVE
    with _WARMUP_LOCK:
        if _WARMUP_ACTIVE:
            return
        _WARMUP_ACTIVE = True

    symbols = [row["ticker"] for row in rows if row.get("ticker") and row.get("rsi14", 0) == 0]

    def worker() -> None:
        global _WARMUP_ACTIVE
        try:
            enrich_with_technicals([{"ticker": symbol} for symbol in symbols])
        finally:
            with _WARMUP_LOCK:
                _WARMUP_ACTIVE = False

    threading.Thread(target=worker, daemon=True).start()


def get_symbol_technicals(symbol: str, force_refresh: bool = False, include_series: bool = False) -> Dict[str, object]:
    cached = _TECH_CACHE.get(symbol)
    now = time.time()
    if cached and not force_refresh and now < float(cached.get("expires_at", 0)):
        return build_cached_response(cached, include_series)

    response = _SESSION.get(
        MSX_URLS["company_chart"].format(symbol=symbol),
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    raw_rows = response.json()
    series = normalize_chart_rows(raw_rows)
    technicals = calculate_technicals(series)
    cached_payload = {
        "expires_at": now + TECH_CACHE_TTL_SECONDS,
        **technicals,
    }
    _TECH_CACHE[symbol] = cached_payload
    return build_cached_response(cached_payload, include_series)


def build_cached_response(cached: Dict[str, object], include_series: bool) -> Dict[str, object]:
    result = {
        "latestClose": cached.get("latestClose", 0.0),
        "latestVolume": cached.get("latestVolume", 0.0),
        "rsi14": cached.get("rsi14", 0.0),
        "sma20": cached.get("sma20", 0.0),
        "bollingerUpper": cached.get("bollingerUpper", 0.0),
        "bollingerMiddle": cached.get("bollingerMiddle", 0.0),
        "bollingerLower": cached.get("bollingerLower", 0.0),
        "bollingerPercentB": cached.get("bollingerPercentB", 0.0),
        "volumeAvg20": cached.get("volumeAvg20", 0.0),
        "volumeVsAvg20": cached.get("volumeVsAvg20", 0.0),
        "priceChange1Y": cached.get("priceChange1Y", 0.0),
    }
    if include_series:
        result["series"] = cached.get("series", [])
    return result


def normalize_chart_rows(rows: object) -> List[Dict[str, object]]:
    if not isinstance(rows, list):
        return []

    by_day: Dict[str, Dict[str, object]] = {}
    for item in rows:
        point_time = parse_chart_date(item)
        if point_time is None:
            continue
        day_key = point_time.strftime("%Y-%m-%d")
        close_value = parse_number(item.get("LTP") or item.get("Value") or item.get("ClosePrice"))
        volume_value = parse_number(item.get("Volume"))
        turnover_value = parse_number(item.get("Turnover"))
        if close_value <= 0:
            continue
        by_day[day_key] = {
            "date": day_key,
            "close": close_value,
            "volume": volume_value,
            "turnover": turnover_value,
        }

    output = list(by_day.values())
    output.sort(key=lambda item: item["date"])
    return output


def parse_chart_date(item: Dict[str, object]) -> Optional[datetime]:
    if item.get("Date"):
        raw = str(item["Date"]).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue

    if all(key in item for key in ["Year", "Month", "Day"]):
        try:
            return datetime(
                int(item["Year"]),
                int(item["Month"]),
                int(item["Day"]),
                int(item.get("Hour", 0)),
                int(item.get("Minute", 0)),
            )
        except (TypeError, ValueError):
            return None
    return None


def calculate_technicals(series: List[Dict[str, object]]) -> Dict[str, object]:
    closes = [point["close"] for point in series]
    volumes = [point["volume"] for point in series]
    latest_close = closes[-1] if closes else 0.0
    latest_volume = volumes[-1] if volumes else 0.0

    enriched_series: List[Dict[str, object]] = []
    for index, point in enumerate(series):
        point_copy = dict(point)
        rsi_slice = calculate_rsi(closes[: index + 1], 14)
        bands = calculate_bollinger(closes[: index + 1], 20, 2)
        point_copy["rsi14"] = round(rsi_slice, 2) if rsi_slice else None
        point_copy["bollingerUpper"] = round(bands["upper"], 4) if bands["upper"] else None
        point_copy["bollingerMiddle"] = round(bands["middle"], 4) if bands["middle"] else None
        point_copy["bollingerLower"] = round(bands["lower"], 4) if bands["lower"] else None
        enriched_series.append(point_copy)

    rsi14 = calculate_rsi(closes, 14)
    sma20 = calculate_sma(closes, 20)
    bollinger = calculate_bollinger(closes, 20, 2)
    volume_avg20 = calculate_sma(volumes, 20)
    volume_vs_avg20 = (latest_volume / volume_avg20) if volume_avg20 > 0 else 0.0

    return {
        "series": enriched_series[-180:],
        "latestClose": round(latest_close, 4),
        "latestVolume": round(latest_volume, 2),
        "rsi14": round(rsi14, 2) if rsi14 else 0.0,
        "sma20": round(sma20, 4) if sma20 else 0.0,
        "bollingerUpper": round(bollinger["upper"], 4) if bollinger["upper"] else 0.0,
        "bollingerMiddle": round(bollinger["middle"], 4) if bollinger["middle"] else 0.0,
        "bollingerLower": round(bollinger["lower"], 4) if bollinger["lower"] else 0.0,
        "bollingerPercentB": round(bollinger["percentB"], 2) if bollinger["percentB"] else 0.0,
        "volumeAvg20": round(volume_avg20, 2) if volume_avg20 else 0.0,
        "volumeVsAvg20": round(volume_vs_avg20, 2) if volume_vs_avg20 else 0.0,
        "priceChange1Y": round(calculate_price_change_1y(series), 2),
    }


def calculate_price_change_1y(series: List[Dict[str, object]]) -> float:
    if len(series) < 2:
        return 0.0
    anchor_index = max(0, len(series) - 252)
    start_close = series[anchor_index]["close"]
    end_close = series[-1]["close"]
    if start_close <= 0:
        return 0.0
    return ((end_close - start_close) / start_close) * 100


def calculate_sma(values: List[float], period: int) -> float:
    if len(values) < period:
        return 0.0
    return mean(values[-period:])


def calculate_bollinger(values: List[float], period: int, std_multiplier: float) -> Dict[str, float]:
    if len(values) < period:
        return {"upper": 0.0, "middle": 0.0, "lower": 0.0, "percentB": 0.0}

    window = values[-period:]
    middle = mean(window)
    deviation = pstdev(window)
    upper = middle + (std_multiplier * deviation)
    lower = middle - (std_multiplier * deviation)
    latest = values[-1]
    percent_b = ((latest - lower) / (upper - lower) * 100) if upper != lower else 0.0
    return {"upper": upper, "middle": middle, "lower": lower, "percentB": percent_b}


def calculate_rsi(values: List[float], period: int) -> float:
    if len(values) <= period:
        return 0.0

    gains: List[float] = []
    losses: List[float] = []
    for index in range(1, period + 1):
        delta = values[index] - values[index - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = mean(gains)
    avg_loss = mean(losses)
    for index in range(period + 1, len(values)):
        delta = values[index] - values[index - 1]
        avg_gain = ((avg_gain * (period - 1)) + max(delta, 0.0)) / period
        avg_loss = ((avg_loss * (period - 1)) + max(-delta, 0.0)) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


BOND_KEYWORDS = re.compile(
    r"(?i)(sukuk|corporate.?bond|govt.?bond|government.?bond|sovereign.?bond"
    r"|t-bill|treasury.?bill|fixed.?income|debt.?securit"
    r"|corporate bonds|trust certificate)",
    re.IGNORECASE,
)

BOND_TICKER_PREFIXES = ("CB", "SK", "TB", "GS")


def is_bond(row: Dict[str, object]) -> bool:
    """Return True if this row looks like a bond, sukuk, or debt instrument."""
    ticker  = str(row.get("ticker", "")).strip().upper()
    company = str(row.get("company", "")).strip()
    sector  = str(row.get("sector", "")).strip()
    if BOND_KEYWORDS.search(company) or BOND_KEYWORDS.search(sector):
        return True
    if any(ticker.startswith(p) and len(ticker) > len(p) and ticker[len(p):].isdigit()
           for p in BOND_TICKER_PREFIXES):
        return True
    return False


def filter_bonds(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Remove corporate bonds, sukuk, and government debt instruments."""
    kept = []
    removed = []
    for row in rows:
        if not row.get("ticker", "").strip():
            continue
        if is_bond(row):
            removed.append(row.get("ticker", "?"))
            continue
        kept.append(row)
    if removed:
        print(f"[MSX] Filtered bonds/sukuk: {removed}")
    return kept or rows


def fill_defaults(row: Dict[str, object]) -> Dict[str, object]:
    defaults = {
        "company": "",
        "sector": "Unknown",
        "price": 0.0,
        "marketCapM": 0.0,
        "peRatio": 0.0,
        "pbRatio": 0.0,
        "dividendYield": 0.0,
        "earningsGrowth": 0.0,
        "dailyChange": 0.0,
        "priceChange1Y": 0.0,
        "debtToEquity": 0.0,
        "roe": 0.0,
        "currentRatio": 0.0,
        "bidPrice": 0.0,
        "bidVolume": 0.0,
        "askPrice": 0.0,
        "askVolume": 0.0,
        "volume": 0.0,
        "turnover": 0.0,
        "trades": 0.0,
        "rsi14": 0.0,
        "sma20": 0.0,
        "bollingerUpper": 0.0,
        "bollingerMiddle": 0.0,
        "bollingerLower": 0.0,
        "bollingerPercentB": 0.0,
        "volumeAvg20": 0.0,
        "volumeVsAvg20": 0.0,
        "buyPressure": 0.0,
        "demandScore": 0.0,
        "latestClose": 0.0,
        "latestVolume": 0.0,
    }
    completed = {**defaults, **row}
    completed["company"] = completed.get("company") or completed["ticker"]
    completed["sector"] = completed.get("sector") or "Unknown"
    return completed


def load_sample_csv(path: Path) -> List[Dict[str, object]]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [
            fill_defaults(
                {
                    "ticker": row["ticker"],
                    "company": row["company"],
                    "sector": row["sector"],
                    "price": parse_number(row["price"]),
                    "marketCapM": parse_number(row["marketCapM"]),
                    "peRatio": parse_number(row["peRatio"]),
                    "pbRatio": parse_number(row["pbRatio"]),
                    "dividendYield": parse_number(row["dividendYield"]),
                    "earningsGrowth": parse_number(row["earningsGrowth"]),
                    "priceChange1Y": parse_number(row["priceChange1Y"]),
                    "debtToEquity": parse_number(row["debtToEquity"]),
                    "roe": parse_number(row["roe"]),
                    "currentRatio": parse_number(row["currentRatio"]),
                }
            )
            for row in reader
        ]


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def parse_number(value: Optional[object]) -> float:
    if value is None:
        return 0.0
    cleaned = str(value).strip()
    if not cleaned:
        return 0.0
    cleaned = cleaned.replace(",", "").replace("%", "").replace("OMR", "").strip()
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    if negative:
        cleaned = cleaned[1:-1]
    try:
        number = float(cleaned)
    except ValueError:
        return 0.0
    return -number if negative else number


def dedupe_by_ticker(rows: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    best_rows: Dict[str, Dict[str, object]] = {}
    for row in rows:
        ticker = str(row.get("ticker", "")).strip()
        if not ticker:
            continue
        previous = best_rows.get(ticker)
        if previous is None or richness_score(row) > richness_score(previous):
            best_rows[ticker] = row
    return list(best_rows.values())


def richness_score(row: Dict[str, object]) -> int:
    score = 0
    for value in row.values():
        if isinstance(value, (int, float)) and value > 0:
            score += 1
        elif isinstance(value, str) and value.strip():
            score += 1
    return score


def sector_code_to_name(value: object) -> str:
    mapping = {
        "1": "Financial",
        "2": "Services",
        "3": "Industrial",
    }
    return mapping.get(str(value).strip(), "Unknown")


def clamp_number(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))
