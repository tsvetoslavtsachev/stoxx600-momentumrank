# MomentumRank S&P 500 — fetch_data.py v10
#
# Промени спрямо v9:
# - Phase 1.5: Retry missing tickers с threads=False (избягва SQLite contention)
# - Stale record fallback: ако даден тикер няма fresh prices, но съществува
#   в предишния data.json, запазваме предишния запис с "stale": true
#   → това гарантира правилен weight normalization и ranking дори при failure
# - Двустепенен threshold: fresh_rate (warn) + total_rate (hard fail)
# - Enriched meta: fresh/stale/retry counters

import json
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from io import StringIO

import numpy as np
import pandas as pd
import requests
import yfinance as yf

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_FILE = "data.json"
META_FILE = "data_meta.json"
LOOKBACK_DAYS = 400

# Bulk download
BATCH_SIZE = 100
BATCH_PAUSE = 2
BULK_MAX_RETRIES = 3

# Retry phase (Phase 1.5)
RETRY_SLEEP = 1.5
RETRY_MAX_ATTEMPTS = 2

# Fundamentals loop
MCAP_SLEEP = (1.5, 2.5)
MCAP_MAX_RETRIES = 3

# Threshold logic
MIN_TOTAL_RATE = 0.60   # fresh + stale под това → hard fail, пази стария data.json
MIN_FRESH_RATE = 0.95   # fresh под това → warn в meta, но commit-ваме

SECTOR_MAP = {
    "Information Technology": "Technology",
    "Health Care": "Healthcare",
    "Financials": "Financial Services",
    "Consumer Discretionary": "Consumer Cyclical",
    "Consumer Staples": "Consumer Defensive",
    "Communication Services": "Communication Services",
    "Materials": "Basic Materials",
    "Industrials": "Industrials",
    "Real Estate": "Real Estate",
    "Energy": "Energy",
    "Utilities": "Utilities",
}


# ── curl_cffi session ────────────────────────────────────────────────────────
def make_session():
    if not HAS_CURL_CFFI:
        return None
    try:
        return curl_requests.Session(impersonate="chrome")
    except Exception as e:
        print(f"WARN: curl_cffi session init failed: {e}")
        return None


# ── Зареди предишен data.json (пълни записи) ──────────────────────────────────
def load_previous_data():
    """Връща {symbol: full_record_dict} от предишен run."""
    if not os.path.exists(OUTPUT_FILE):
        return {}
    try:
        with open(OUTPUT_FILE, "r") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return {}
        return {r["symbol"]: r for r in data if "symbol" in r}
    except Exception as e:
        print(f"WARN: could not load previous {OUTPUT_FILE}: {e}")
        return {}


# ── S&P 500 list ──────────────────────────────────────────────────────────────
def get_sp500_tickers():
    url = (
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies"
        "/main/data/constituents.csv"
    )
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text))
    df.columns = [c.strip() for c in df.columns]

    records = []
    for _, row in df.iterrows():
        sym = str(row.get("Symbol", row.iloc[0])).strip().replace(".", "-")
        name = str(row.get("Name", row.iloc[1])).strip()
        raw_s = str(row.get("Sector", row.iloc[2])).strip()
        sector = SECTOR_MAP.get(raw_s, raw_s)
        records.append({"symbol": sym, "name": name, "sector": sector})
    return records


# ── Phase 1.5: Retry missing tickers (threads=False) ──────────────────────────
def retry_missing_prices(missing_symbols, start_dt, end_dt, session=None):
    """
    Per-ticker retry за тикери, които fail-наха в bulk fetch-а.
    threads=False избягва SQLite cache contention, която е най-честата причина
    за грешки от типа 'database is locked'.
    """
    result = {}
    if not missing_symbols:
        return result

    print(f"  Retry phase: {len(missing_symbols)} tickers (threads=False)")
    for sym in missing_symbols:
        success = False
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                time.sleep(RETRY_SLEEP + random.uniform(0, 0.5))
                data = yf.download(
                    tickers=[sym],
                    start=start_dt.strftime("%Y-%m-%d"),
                    end=end_dt.strftime("%Y-%m-%d"),
                    auto_adjust=True,
                    actions=False,
                    progress=False,
                    threads=False,        # ← key: no concurrency, no SQLite race
                    session=session,
                )

                if data.empty:
                    continue

                # Single-ticker fetch → flat columns, без MultiIndex
                if isinstance(data.columns, pd.MultiIndex):
                    # yfinance понякога връща MultiIndex дори за един тикер
                    if (sym, "Close") in data.columns:
                        prices = data[(sym, "Close")].dropna().astype(float)
                        volumes = data[(sym, "Volume")].fillna(0).astype(float)
                    else:
                        continue
                else:
                    if "Close" not in data.columns:
                        continue
                    prices = data["Close"].dropna().astype(float)
                    volumes = data["Volume"].fillna(0).astype(float)

                if len(prices) >= 60:
                    result[sym] = {"prices": prices, "volumes": volumes}
                    print(f"    ✓ recovered {sym}")
                    success = True
                    break
            except Exception as e:
                if attempt >= RETRY_MAX_ATTEMPTS:
                    print(f"    ✗ {sym}: {e}")

        if not success and sym not in result:
            # Няма какво повече да правим на този етап — stale fallback идва по-късно
            pass

    return result


# ── Phase 1: Bulk price download ──────────────────────────────────────────────
def bulk_download_prices(tickers, start_dt, end_dt, session=None):
    result = {}
    symbols = [t["symbol"] for t in tickers]

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} tickers...", flush=True)

        data = pd.DataFrame()
        for attempt in range(1, BULK_MAX_RETRIES + 1):
            try:
                data = yf.download(
                    tickers=batch,
                    start=start_dt.strftime("%Y-%m-%d"),
                    end=end_dt.strftime("%Y-%m-%d"),
                    auto_adjust=True,
                    actions=False,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                    session=session,
                )
                break
            except Exception as e:
                if attempt < BULK_MAX_RETRIES:
                    wait = 2 ** attempt + random.uniform(0, 1)
                    print(f"    retry {attempt}: {e} (wait {wait:.1f}s)")
                    time.sleep(wait)
                else:
                    print(f"    FAILED batch {batch_num}: {e}")

        if data.empty:
            continue

        for sym in batch:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if (sym, "Close") not in data.columns:
                        continue
                    prices = data[(sym, "Close")].dropna().astype(float)
                    volumes = data[(sym, "Volume")].fillna(0).astype(float)
                else:
                    if len(batch) != 1 or "Close" not in data.columns:
                        continue
                    prices = data["Close"].dropna().astype(float)
                    volumes = data["Volume"].fillna(0).astype(float)

                if len(prices) >= 60:
                    result[sym] = {"prices": prices, "volumes": volumes}
            except Exception as e:
                print(f"    WARN {sym}: extract failed: {e}")

        if i + BATCH_SIZE < len(symbols):
            time.sleep(BATCH_PAUSE)

    # ── Phase 1.5: retry на missing ──────────────────────────────────────────
    missing = [t["symbol"] for t in tickers if t["symbol"] not in result]
    if missing:
        recovered = retry_missing_prices(missing, start_dt, end_dt, session=session)
        result.update(recovered)
        return result, len(recovered)

    return result, 0


# ── Phase 2: Fundamentals ─────────────────────────────────────────────────────
def fetch_fundamentals(symbol, session=None):
    for attempt in range(1, MCAP_MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(symbol, session=session) if session else yf.Ticker(symbol)
            market_cap = 0
            avg_volume = 0

            try:
                fi = ticker.fast_info
                market_cap = int(getattr(fi, "market_cap", 0) or 0)
                avg_volume = int(getattr(fi, "three_month_average_volume", 0) or 0)
            except Exception:
                pass

            if market_cap == 0 or avg_volume == 0:
                try:
                    info = ticker.info
                    if market_cap == 0:
                        market_cap = int(info.get("marketCap", 0) or 0)
                    if avg_volume == 0:
                        avg_volume = int(
                            info.get("averageDailyVolume3Month", 0)
                            or info.get("averageVolume", 0)
                            or 0
                        )
                except Exception:
                    pass

            return market_cap, avg_volume

        except Exception as e:
            err = str(e).lower()
            is_rate_limit = "rate" in err or "429" in err or "too many" in err
            if is_rate_limit and attempt < MCAP_MAX_RETRIES:
                wait = 5 * (2 ** attempt) + random.uniform(0, 2)
                print(f"    {symbol}: rate limited, wait {wait:.1f}s")
                time.sleep(wait)
                continue
            if attempt >= MCAP_MAX_RETRIES:
                return 0, 0

    return 0, 0


# ── Изчисления (идентични с v9) ───────────────────────────────────────────────
def calc_return(prices, trading_days):
    if prices is None or len(prices) < trading_days + 1:
        return 0.0
    return round((prices.iloc[-1] / prices.iloc[-(trading_days + 1)] - 1) * 100, 2)


def calc_volatility(prices, trading_days=252):
    if prices is None or len(prices) < 22:
        return 0.0
    rets = np.log(prices / prices.shift(1)).dropna()
    if len(rets) > trading_days:
        rets = rets.iloc[-trading_days:]
    return round(float(rets.std() * math.sqrt(252) * 100), 2)


def calc_sharpe(prices, trading_days=252, rf=0.045):
    if prices is None or len(prices) < 22:
        return 0.0
    rets = np.log(prices / prices.shift(1)).dropna()
    if len(rets) > trading_days:
        rets = rets.iloc[-trading_days:]
    ann_ret = float(rets.mean() * 252)
    ann_vol = float(rets.std() * math.sqrt(252))
    if ann_vol <= 0:
        return 0.0
    return round((ann_ret - rf) / ann_vol, 2)


def calc_drawdown(prices):
    if prices is None or len(prices) < 2:
        return 0.0
    roll_max = prices.expanding().max()
    dd_series = (prices - roll_max) / roll_max * 100
    return round(float(dd_series.min()), 2)


def calc_momentum_score(r1m, r3m, r6m, r12m, vol, sharpe, market_cap):
    def sig(x, scale):
        exp_arg = max(-50, min(50, -x / scale))
        return 100.0 / (1.0 + math.exp(exp_arg))

    s12 = sig(r12m, 30)
    s6 = sig(r6m, 20)
    s3 = sig(r3m, 15)
    s1 = sig(r1m, 10)
    s_sh = sig(sharpe, 1.0)
    s_vol = 100.0 / (1.0 + math.exp(max(-50, min(50, (vol - 25) / 10))))

    if market_cap >= 200e9:
        s_cap = 100
    elif market_cap >= 50e9:
        s_cap = 75
    elif market_cap >= 10e9:
        s_cap = 50
    elif market_cap > 0:
        s_cap = 25
    else:
        s_cap = 50

    return round(
        s12 * 0.30 + s6 * 0.25 + s3 * 0.20 + s1 * 0.10
        + s_sh * 0.10 + s_vol * 0.03 + s_cap * 0.02,
        1,
    )


# ── Обработка на един тикер ───────────────────────────────────────────────────
def process_ticker(info, prices, volumes, market_cap, avg_volume):
    sym = info["symbol"]
    if prices is None or len(prices) < 60:
        return None

    if avg_volume == 0 and volumes is not None and len(volumes) > 0:
        lookback_vol = volumes.iloc[-63:] if len(volumes) >= 63 else volumes
        if (lookback_vol > 0).any():
            avg_volume = int(lookback_vol[lookback_vol > 0].mean())

    r1m = calc_return(prices, 21)
    r3m = calc_return(prices, 63)
    r6m = calc_return(prices, 126)
    r12m = calc_return(prices, 252)
    vol = calc_volatility(prices)
    shp = calc_sharpe(prices)
    dd = calc_drawdown(prices)

    price = round(float(prices.iloc[-1]), 2)
    day_chg = round((prices.iloc[-1] / prices.iloc[-2] - 1) * 100, 2) if len(prices) >= 2 else 0.0

    p52 = prices.iloc[-252:] if len(prices) >= 252 else prices
    high52 = round(float(p52.max()), 2)
    low52 = round(float(p52.min()), 2)
    dd52 = round((price - high52) / high52 * 100, 2) if high52 > 0 else 0.0

    score = calc_momentum_score(r1m, r3m, r6m, r12m, vol, shp, market_cap)

    return {
        "symbol": sym,
        "name": info["name"],
        "sector": info["sector"],
        "price": price,
        "marketCap": market_cap,
        "return12m": r12m,
        "return6m": r6m,
        "return3m": r3m,
        "return1m": r1m,
        "volatility": vol,
        "avgVolume": avg_volume,
        "dayChange": day_chg,
        "sharpe": shp,
        "drawdown": dd,
        "high52w": high52,
        "low52w": low52,
        "drawdown52w": dd52,
        "momentumScore": score,
        "weight": 0.0,
        "stale": False,
    }


# ── Stale fallback: запази предишен запис за липсващи тикери ─────────────────
def build_stale_record(prev_record, current_info):
    """
    Копира предишен запис и го маркира като stale.
    current_info идва от актуалния S&P 500 list — в случай че името/секторът
    са се променили, актуализираме тези две полета от текущия source.
    """
    rec = dict(prev_record)  # shallow copy
    rec["name"] = current_info["name"]
    rec["sector"] = current_info["sector"]
    rec["stale"] = True
    rec["weight"] = 0.0  # ще се преизчисли в main()
    # rank ще се преизчисли в main()
    return rec


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("MomentumRank — fetch_data.py v10")
    print("=" * 52)

    if not HAS_CURL_CFFI:
        print("WARN: curl_cffi not installed — Yahoo rate limit risk")

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    print(f"Period : {start_dt.date()} → {end_dt.date()}")

    prev_data = load_previous_data()
    if prev_data:
        print(f"Prev   : {len(prev_data)} records available for fallback")

    print("Loading S&P 500 list...")
    tickers = get_sp500_tickers()
    ticker_info_map = {t["symbol"]: t for t in tickers}
    print(f"  {len(tickers)} tickers loaded")
    print()

    session = make_session()
    if session:
        print("Session: curl_cffi Chrome-impersonation ENABLED")
    else:
        print("Session: default requests (higher rate-limit risk)")
    print()

    # ── Phase 1 + 1.5 ─────────────────────────────────────────────────────
    print("Phase 1: Bulk price download (+ retry on miss)")
    print("-" * 52)
    price_data, retry_recovered = bulk_download_prices(
        tickers, start_dt, end_dt, session=session
    )
    print(f"  Got prices for {len(price_data)} / {len(tickers)} tickers")
    if retry_recovered:
        print(f"  Retry recovered: {retry_recovered}")
    print()

    # ── Phase 2: Fundamentals ─────────────────────────────────────────────
    print("Phase 2: Fundamentals (marketCap, avgVolume)")
    print("-" * 52)
    fundamentals = {}
    mcap_fallback_used = []
    total = len(tickers)

    for i, t in enumerate(tickers, 1):
        sym = t["symbol"]
        if sym not in price_data:
            fundamentals[sym] = (0, 0)
            continue

        time.sleep(random.uniform(*MCAP_SLEEP))
        mcap, avol = fetch_fundamentals(sym, session=session)

        # Hybrid fallback: fresh prices + stale marketCap е OK
        if mcap == 0 and sym in prev_data:
            fallback = prev_data[sym].get("marketCap", 0)
            if fallback > 0:
                mcap = fallback
                mcap_fallback_used.append(sym)
        if avol == 0 and sym in prev_data:
            fallback = prev_data[sym].get("avgVolume", 0)
            if fallback > 0:
                avol = fallback

        fundamentals[sym] = (mcap, avol)

        if i % 50 == 0:
            cap_ok = sum(1 for v in fundamentals.values() if v[0] > 0)
            print(f"  [{i:3d}/{total}] cumulative marketCap OK: {cap_ok}")

    print()

    # ── Phase 3: Metrics ──────────────────────────────────────────────────
    print("Phase 3: Metrics & momentum score")
    print("-" * 52)
    results = []
    for t in tickers:
        sym = t["symbol"]
        if sym not in price_data:
            continue
        prices = price_data[sym]["prices"]
        volumes = price_data[sym]["volumes"]
        mcap, avol = fundamentals.get(sym, (0, 0))
        rec = process_ticker(t, prices, volumes, mcap, avol)
        if rec:
            results.append(rec)
    fresh_count = len(results)
    print(f"  Fresh records: {fresh_count}")

    # ── Phase 4: Stale fallback за липсващите ────────────────────────────
    current_symbols = {t["symbol"] for t in tickers}
    fresh_symbols = {r["symbol"] for r in results}
    missing_symbols = current_symbols - fresh_symbols
    stale_used = []

    for sym in missing_symbols:
        if sym in prev_data:
            stale_rec = build_stale_record(prev_data[sym], ticker_info_map[sym])
            results.append(stale_rec)
            stale_used.append(sym)

    if stale_used:
        print(f"  Stale fallback used: {len(stale_used)} records")
        print(f"    Symbols: {', '.join(stale_used[:10])}"
              + (f" ... +{len(stale_used) - 10} more" if len(stale_used) > 10 else ""))

    truly_missing = missing_symbols - set(stale_used)
    if truly_missing:
        print(f"  WARN: {len(truly_missing)} tickers have neither fresh nor stale data")
        print(f"    Symbols: {', '.join(list(truly_missing)[:10])}")
    print()

    # ── Weight normalization + sort + rank (върху ВСИЧКИ записи) ─────────
    total_cap = sum(r["marketCap"] for r in results if r["marketCap"] > 0)
    for r in results:
        if total_cap > 0 and r["marketCap"] > 0:
            r["weight"] = round(r["marketCap"] / total_cap * 100, 4)
        else:
            r["weight"] = 0.0

    results.sort(key=lambda x: x["momentumScore"], reverse=True)
    for rank, r in enumerate(results, 1):
        r["rank"] = rank

    # ── Meta ──────────────────────────────────────────────────────────────
    total_count = len(results)
    fresh_rate = fresh_count / len(tickers) if tickers else 0
    total_rate = total_count / len(tickers) if tickers else 0

    meta = {
        "updated": end_dt.strftime("%Y-%m-%d %H:%M UTC"),
        "count": total_count,
        "fresh_count": fresh_count,
        "stale_count": len(stale_used),
        "retry_recovered": retry_recovered,
        "mcap_fallback_used": len(mcap_fallback_used),
        "vol_ok": sum(1 for r in results if r["avgVolume"] > 0),
        "cap_ok": sum(1 for r in results if r["marketCap"] > 0),
        "fresh_rate": round(fresh_rate, 4),
        "total_rate": round(total_rate, 4),
        "stale": False,
        "warnings": [],
    }

    if fresh_rate < MIN_FRESH_RATE:
        warn = (
            f"Only {fresh_count}/{len(tickers)} fresh records "
            f"({fresh_rate:.1%} < {MIN_FRESH_RATE:.0%})"
        )
        meta["warnings"].append(warn)
        print(f"WARN: {warn}")

    # Hard fail threshold (even fresh+stale не стига)
    if total_rate < MIN_TOTAL_RATE and prev_data:
        print(f"HARD FAIL: only {total_count} < {int(len(tickers) * MIN_TOTAL_RATE)} records")
        print(f"           KEEPING previous data.json")
        meta["stale"] = True
        meta["stale_reason"] = f"fetch returned {total_count}/{len(tickers)}"
        with open(META_FILE, "w") as f:
            json.dump(meta, f, indent=2)
        sys.exit(1)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, separators=(",", ":"))
    with open(META_FILE, "w") as f:
        json.dump(meta, f, indent=2)

    print()
    print(f"Done          : {total_count} records → {OUTPUT_FILE}")
    print(f"  fresh       : {fresh_count} ({fresh_rate:.1%})")
    print(f"  stale       : {len(stale_used)}")
    print(f"avgVolume OK  : {meta['vol_ok']}/{total_count}")
    print(f"marketCap OK  : {meta['cap_ok']}/{total_count}")
    print(f"Top 5         : {[r['symbol'] for r in results[:5]]}")
    print(f"Updated       : {meta['updated']}")

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
