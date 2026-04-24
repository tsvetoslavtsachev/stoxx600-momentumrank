# MomentumRank STOXX Europe 600 — fetch_data.py v12
#
# Архитектура (радикално опростена спрямо v10/v11):
#   Phase 0: Constituent discovery от iShares EXSA ETF holdings CSV
#   Phase 1: Bulk price download (yf.download batches, curl_cffi session)
#   Phase 1.5: Per-ticker retry с threads=False (SQLite lock recovery)
#   Phase 2 ПРЕМАХНАТ (няма повече yfinance fundamentals fetch!)
#   Phase 3: Metrics + scoring (size_score от iShares weight, не marketCap)
#   Phase 4: Stale fallback за missing tickers
#
# Ключови промени спрямо v11:
#   - Няма marketCap fetch → 0 HTTP заявки към yfinance за fundamentals
#   - avgVolume се изчислява от OHLCV volumes (от Phase 1)
#   - size_score_from_weight() замества calc на cap bracket
#   - data.json: премахнати marketCap, marketCapSource, computed weight
#                оставен само "weight" (authoritative от iShares)
#
# Общо HTTP заявки към Yahoo: ~6 (само Phase 1 batches) вместо ~620 (v10).
# Очакван runtime: 3-5 минути вместо 18-20 минути.

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

ISHARES_URL = (
    "https://www.ishares.com/de/privatanleger/de/produkte/251931/"
    "ishares-stoxx-europe-600-ucits-etf-de-fund/1478358465952.ajax"
    "?fileType=csv&fileName=EXSA_holdings&dataType=fund"
)
ISHARES_REFERER = (
    "https://www.ishares.com/de/privatanleger/de/produkte/251931/"
    "ishares-stoxx-europe-600-ucits-etf-de-fund"
)

BATCH_SIZE = 100
BATCH_PAUSE = 2
BULK_MAX_RETRIES = 3

RETRY_SLEEP = 1.5
RETRY_MAX_ATTEMPTS = 2

MIN_TOTAL_RATE = 0.60
MIN_FRESH_RATE = 0.95


# ── Exchange → Yahoo suffix mapping (от iShares Börse column) ────────────────
EXCHANGE_SUFFIX = {
    "London Stock Exchange":              ".L",
    "Xetra":                              ".DE",
    "Frankfurt Stock Exchange":           ".F",
    "Nyse Euronext - Euronext Paris":     ".PA",
    "Euronext Paris":                     ".PA",
    "SIX Swiss Exchange":                 ".SW",
    "Euronext Amsterdam":                 ".AS",
    "Nyse Euronext - Euronext Amsterdam": ".AS",
    "Nasdaq Stockholm":                   ".ST",
    "Bolsa De Madrid":                    ".MC",
    "Borsa Italiana":                     ".MI",
    "Nasdaq Helsinki":                    ".HE",
    "Nyse Euronext - Euronext Brussels":  ".BR",
    "Euronext Brussels":                  ".BR",
    "Oslo Bors":                          ".OL",
    "Oslo Bors Asa":                      ".OL",
    "Nasdaq Copenhagen":                  ".CO",
    "Wiener Boerse":                      ".VI",
    "Euronext Lisbon":                    ".LS",
    "Euronext Dublin":                    ".IR",
    "Irish Stock Exchange":               ".IR",
    "Warsaw Stock Exchange":              ".WA",
    "Athens Stock Exchange":              ".AT",
    "Tel Aviv Stock Exchange":            ".TA",
    # iShares OMX/legacy variants (смесва старо и ново naming)
    "Omx Nordic Exchange Copenhagen A/S": ".CO",
    "Nasdaq Omx Nordic":                  ".ST",   # generic — но в STOXX 600 всички са шведски
    "Nasdaq Omx Helsinki Ltd.":           ".HE",
    "Nasdaq Omx Stockholm Ab":            ".ST",
    "Nasdaq Omx Copenhagen A/S":          ".CO",
    # Alternative naming variants
    "Wiener Boerse Ag":                        ".VI",
    "Warsaw Stock Exchange/Equities/Main Market": ".WA",
    "Irish Stock Exchange - All Market":       ".IR",
    "Nyse Euronext - Euronext Lisbon":         ".LS",
}

# Country-based fallback: ако exchange не е в EXCHANGE_SUFFIX, ползва country.
# Така бъдещи rename-вания на борси в iShares не chupят скрипта.
COUNTRY_SUFFIX_FALLBACK = {
    "Sweden":         ".ST",
    "Finland":        ".HE",
    "Denmark":        ".CO",
    "Norway":         ".OL",
    "Poland":         ".WA",
    "Austria":        ".VI",
    "Ireland":        ".IR",
    "Portugal":       ".LS",
    "Germany":        ".DE",
    "United Kingdom": ".L",
    "France":         ".PA",
    "Switzerland":    ".SW",
    "Netherlands":    ".AS",
    "Spain":          ".MC",
    "Italy":          ".MI",
    "Belgium":        ".BR",
    "Greece":         ".AT",
    "Israel":         ".TA",
    "Luxembourg":     ".LU",
}

# Ticker overrides: известни несъвпадения между iShares и Yahoo.
# Ключ = Yahoo suffix (напр. ".SW"), стойност = dict от {iShares ticker: Yahoo ticker}.
# Добавяй само след ръчна проверка че Yahoo-тикера работи.
TICKER_OVERRIDES_BY_EXCHANGE = {
    ".SW": {
        # Roche Holding Genussschein: iShares "ROP" vs Yahoo "ROG"
        "ROP": "ROG",
    },
    ".L": {
        # BT Group Class A: iShares "BT-A" (след middle-dot conversion) vs Yahoo "BT-A"
        # (тук override не е нужен, dash-conversion вече го прави)
    },
}

SECTOR_DE_EN = {
    "IT":                         "Technology",
    "Informationstechnologie":    "Technology",
    "Financials":                 "Financial Services",
    "Finanzwesen":                "Financial Services",
    "Gesundheitsversorgung":      "Healthcare",
    "Gesundheit":                 "Healthcare",
    "Nichtzyklische Konsumgüter": "Consumer Defensive",
    "Basiskonsumgüter":           "Consumer Defensive",
    "Zyklische Konsumgüter":      "Consumer Cyclical",
    "Nicht-Basiskonsumgüter":     "Consumer Cyclical",
    "Energie":                    "Energy",
    "Industrie":                  "Industrials",
    "Versorger":                  "Utilities",
    "Grundstoffe":                "Basic Materials",
    "Materialien":                "Basic Materials",
    "Kommunikation":              "Communication Services",
    "Kommunikationsdienste":      "Communication Services",
    "Immobilien":                 "Real Estate",
    # English pass-through
    "Technology":                 "Technology",
    "Healthcare":                 "Healthcare",
    "Consumer Defensive":         "Consumer Defensive",
    "Consumer Cyclical":          "Consumer Cyclical",
    "Energy":                     "Energy",
    "Industrials":                "Industrials",
    "Utilities":                  "Utilities",
    "Basic Materials":            "Basic Materials",
    "Communication Services":     "Communication Services",
    "Real Estate":                "Real Estate",
    "Financial Services":         "Financial Services",
}

COUNTRY_DE_EN = {
    "Deutschland":            "Germany",
    "Frankreich":             "France",
    "Vereinigtes Königreich": "United Kingdom",
    "Schweiz":                "Switzerland",
    "Niederlande":            "Netherlands",
    "Schweden":               "Sweden",
    "Spanien":                "Spain",
    "Italien":                "Italy",
    "Finnland":               "Finland",
    "Belgien":                "Belgium",
    "Norwegen":               "Norway",
    "Dänemark":               "Denmark",
    "Österreich":             "Austria",
    "Portugal":               "Portugal",
    "Irland":                 "Ireland",
    "Luxemburg":              "Luxembourg",
    "Polen":                  "Poland",
    "Griechenland":           "Greece",
    "Israel":                 "Israel",
}


# ── Helpers ──────────────────────────────────────────────────────────────────
def make_session():
    if not HAS_CURL_CFFI:
        return None
    try:
        return curl_requests.Session(impersonate="chrome")
    except Exception as e:
        print(f"WARN: curl_cffi session init failed: {e}")
        return None


def parse_de_number(s):
    """'1.234,56' → 1234.56"""
    if s is None or s == "":
        return 0.0
    try:
        return float(str(s).replace(".", "").replace(",", "."))
    except (ValueError, TypeError):
        return 0.0


def load_previous_data():
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


# ── Phase 0: Fetch constituents from iShares ─────────────────────────────────
def fetch_ishares_constituents():
    print("Phase 0: Fetch constituents from iShares")
    print("-" * 52)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv,application/csv,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        "Referer": ISHARES_REFERER,
    }

    resp = None
    last_exc = None
    for attempt in range(1, 4):
        try:
            resp = requests.get(ISHARES_URL, headers=headers, timeout=30)
            resp.raise_for_status()
            print(f"  Downloaded {len(resp.text)} chars (status {resp.status_code})")
            break
        except Exception as e:
            last_exc = e
            print(f"  attempt {attempt}: {e}")
            if attempt < 3:
                time.sleep(3 * attempt)

    if resp is None or resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch iShares CSV: {last_exc}")

    lines = resp.text.strip().split("\n")
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Emittententicker") or line.startswith('"Emittententicker"'):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Could not find 'Emittententicker' header in CSV")

    csv_body = "\n".join(lines[header_idx:])
    df = pd.read_csv(StringIO(csv_body), sep=",", quotechar='"')
    df = df[df["Anlageklasse"].str.strip() == "Aktien"].copy()

    print(f"  Parsed {len(df)} equity holdings")

    records = []
    skipped_exchange = 0
    unknown_sectors = set()

    for _, row in df.iterrows():
        raw_ticker = str(row["Emittententicker"]).strip().strip('"')
        name       = str(row["Name"]).strip().strip('"')
        sector_de  = str(row["Sektor"]).strip().strip('"')
        country_de = str(row["Standort"]).strip().strip('"')
        exchange   = str(row["Börse"]).strip().strip('"')
        currency   = str(row["Marktwährung"]).strip().strip('"')

        weight_pct  = parse_de_number(row["Gewichtung (%)"])
        price_local = parse_de_number(row["Kurs"])

        country_en = COUNTRY_DE_EN.get(country_de, country_de)

        # Primary: exact exchange match
        suffix = EXCHANGE_SUFFIX.get(exchange)

        # Fallback: country-based suffix (ако exchange е unknown)
        if suffix is None:
            suffix = COUNTRY_SUFFIX_FALLBACK.get(country_en)
            if suffix is not None:
                print(f"    INFO unknown exchange '{exchange}' → fallback to "
                      f"{suffix} (country={country_en}, {raw_ticker} - {name})")

        if suffix is None:
            print(f"    WARN unknown exchange '{exchange}' AND unknown country "
                  f"'{country_en}' ({raw_ticker} - {name})")
            skipped_exchange += 1
            continue

        sector_en = SECTOR_DE_EN.get(sector_de)
        if sector_en is None:
            unknown_sectors.add(sector_de)
            sector_en = sector_de

        ticker_clean = raw_ticker.upper().replace(" ", "-").replace("/", "-")
        # Strip trailing dot (iShares UK convention: "BP.", "NG." = ordinary shares)
        # Иначе получаваме "BP..L" което Yahoo не разпознава.
        ticker_clean = ticker_clean.rstrip(".")
        # Middle dot → dash (UK dual-class: "BT.A" → "BT-A.L" в Yahoo)
        ticker_clean = ticker_clean.replace(".", "-")

        # Skip празни или малформирани тикери (iShares понякога има "-" редове)
        if not ticker_clean or ticker_clean in ("-", "--"):
            print(f"    WARN skip malformed ticker '{raw_ticker}' ({name})")
            skipped_exchange += 1
            continue

        # Ticker override: известни несъвпадения между iShares и Yahoo
        # Ключ е pre-suffix тикера, стойност е новият pre-suffix тикер.
        override_key = ticker_clean
        if override_key in TICKER_OVERRIDES_BY_EXCHANGE.get(suffix, {}):
            new_ticker = TICKER_OVERRIDES_BY_EXCHANGE[suffix][override_key]
            print(f"    INFO ticker override: {ticker_clean}{suffix} → "
                  f"{new_ticker}{suffix} ({name})")
            ticker_clean = new_ticker

        yahoo_symbol = ticker_clean + suffix

        records.append({
            "symbol":         yahoo_symbol,
            "raw_ticker":     raw_ticker,
            "name":           name,
            "sector":         sector_en,
            "sector_de":      sector_de,
            "country":        country_en,
            "exchange":       exchange,
            "currency":       currency,
            "suffix":         suffix,
            "price_ishares":  price_local,
            "weight":         weight_pct,  # authoritative iShares weight
        })

    if unknown_sectors:
        print(f"    Unknown sectors (kept raw): {sorted(unknown_sectors)}")
    if skipped_exchange:
        print(f"  Skipped {skipped_exchange} due to unknown exchange")
    print(f"  Final: {len(records)} tickers ready for yfinance")
    print()

    return records


# ── Phase 1.5: Retry missing ─────────────────────────────────────────────────
def retry_missing_prices(missing_symbols, start_dt, end_dt, session=None):
    result = {}
    if not missing_symbols:
        return result

    print(f"  Retry phase: {len(missing_symbols)} tickers (threads=False)")
    for sym in missing_symbols:
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
                    threads=False,
                    session=session,
                )
                if data.empty:
                    continue

                if isinstance(data.columns, pd.MultiIndex):
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
                    break
            except Exception as e:
                if attempt >= RETRY_MAX_ATTEMPTS:
                    print(f"    ✗ {sym}: {e}")

    return result


# ── Phase 1: Bulk price download ─────────────────────────────────────────────
def bulk_download_prices(records, start_dt, end_dt, session=None):
    result = {}
    symbols = [r["symbol"] for r in records]

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} tickers...",
              flush=True)

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

    missing = [r["symbol"] for r in records if r["symbol"] not in result]
    recovered = 0
    if missing:
        retry_result = retry_missing_prices(missing, start_dt, end_dt, session=session)
        result.update(retry_result)
        recovered = len(retry_result)

    return result, recovered


# ── Metrics ──────────────────────────────────────────────────────────────────
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


def calc_sharpe(prices, trading_days=252, rf=0.025):
    """rf=2.5% ≈ 10Y Bund yield."""
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


def size_score_from_weight(weight_pct):
    """
    iShares weight → size score [0-100].
    Brackets:
      >= 2.0%  → 100 (mega, ~3-5 companies like ASML, HSBC, AstraZeneca)
      >= 1.0%  →  75 (large, ~15-20 companies)
      >= 0.3%  →  50 (mid, ~120-140 companies)
      >  0%    →  25 (small-cap tail, ~400+ companies)
       = 0     →  50 (defensive neutral)
    """
    if weight_pct >= 2.0:
        return 100
    elif weight_pct >= 1.0:
        return 75
    elif weight_pct >= 0.3:
        return 50
    elif weight_pct > 0:
        return 25
    else:
        return 50


def calc_momentum_score(r1m, r3m, r6m, r12m, vol, sharpe, weight_pct):
    def sig(x, scale):
        exp_arg = max(-50, min(50, -x / scale))
        return 100.0 / (1.0 + math.exp(exp_arg))

    s12 = sig(r12m, 30)
    s6 = sig(r6m, 20)
    s3 = sig(r3m, 15)
    s1 = sig(r1m, 10)
    s_sh = sig(sharpe, 1.0)
    s_vol = 100.0 / (1.0 + math.exp(max(-50, min(50, (vol - 25) / 10))))
    s_size = size_score_from_weight(weight_pct)

    return round(
        s12 * 0.30 + s6 * 0.25 + s3 * 0.20 + s1 * 0.10
        + s_sh * 0.10 + s_vol * 0.03 + s_size * 0.02,
        1,
    )


# ── Process single ticker ────────────────────────────────────────────────────
def process_ticker(info, prices, volumes):
    sym = info["symbol"]
    if prices is None or len(prices) < 60:
        return None

    # avgVolume изцяло от OHLCV volumes (не от yfinance.info)
    avg_volume = 0
    if volumes is not None and len(volumes) > 0:
        lookback_vol = volumes.iloc[-63:] if len(volumes) >= 63 else volumes
        if (lookback_vol > 0).any():
            avg_volume = int(lookback_vol[lookback_vol > 0].mean())

    weight_pct = info["weight"]

    r1m = calc_return(prices, 21)
    r3m = calc_return(prices, 63)
    r6m = calc_return(prices, 126)
    r12m = calc_return(prices, 252)
    vol = calc_volatility(prices)
    shp = calc_sharpe(prices)
    dd = calc_drawdown(prices)

    price = round(float(prices.iloc[-1]), 4)
    day_chg = (
        round((prices.iloc[-1] / prices.iloc[-2] - 1) * 100, 2)
        if len(prices) >= 2 else 0.0
    )

    p52 = prices.iloc[-252:] if len(prices) >= 252 else prices
    high52 = round(float(p52.max()), 4)
    low52 = round(float(p52.min()), 4)
    dd52 = round((price - high52) / high52 * 100, 2) if high52 > 0 else 0.0

    score = calc_momentum_score(r1m, r3m, r6m, r12m, vol, shp, weight_pct)

    return {
        "symbol":        sym,
        "name":          info["name"],
        "sector":        info["sector"],
        "country":       info["country"],
        "exchange":      info["exchange"],
        "currency":      info["currency"],
        "price":         price,
        "weight":        round(weight_pct, 4),  # authoritative iShares weight
        "return12m":     r12m,
        "return6m":      r6m,
        "return3m":      r3m,
        "return1m":      r1m,
        "volatility":    vol,
        "avgVolume":     avg_volume,
        "dayChange":     day_chg,
        "sharpe":        shp,
        "drawdown":      dd,
        "high52w":       high52,
        "low52w":        low52,
        "drawdown52w":   dd52,
        "momentumScore": score,
        "stale":         False,
    }


def build_stale_record(prev_record, current_info):
    rec = dict(prev_record)
    rec["name"] = current_info["name"]
    rec["sector"] = current_info["sector"]
    rec["country"] = current_info["country"]
    rec["exchange"] = current_info["exchange"]
    rec["weight"] = round(current_info["weight"], 4)
    rec["stale"] = True

    # Премахни legacy полета ако ги има в старата data.json (от v10/v11)
    for legacy_key in ("marketCap", "marketCapSource", "iSharesWeight"):
        rec.pop(legacy_key, None)

    return rec


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("MomentumRank STOXX Europe 600 — fetch_data.py v12")
    print("=" * 55)

    if not HAS_CURL_CFFI:
        print("WARN: curl_cffi not installed — Yahoo rate limit risk")

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    print(f"Period : {start_dt.date()} → {end_dt.date()}")

    prev_data = load_previous_data()
    if prev_data:
        print(f"Prev   : {len(prev_data)} records available for fallback")
    print()

    # Phase 0
    records = fetch_ishares_constituents()
    if len(records) < 500:
        print(f"ERROR: only {len(records)} constituents — aborting")
        sys.exit(2)

    ticker_info_map = {r["symbol"]: r for r in records}

    session = make_session()
    if session:
        print("Session: curl_cffi Chrome-impersonation ENABLED")
    else:
        print("Session: default requests (higher rate-limit risk)")
    print()

    # Phase 1 + 1.5
    print("Phase 1: Bulk price download (+ retry)")
    print("-" * 52)
    price_data, retry_recovered = bulk_download_prices(
        records, start_dt, end_dt, session=session
    )
    print(f"  Got prices for {len(price_data)} / {len(records)} tickers")
    if retry_recovered:
        print(f"  Retry recovered: {retry_recovered}")
    print()

    # Phase 3: Metrics (no separate Phase 2!)
    print("Phase 3: Metrics & momentum score")
    print("-" * 52)
    results = []
    for r in records:
        sym = r["symbol"]
        if sym not in price_data:
            continue
        prices = price_data[sym]["prices"]
        volumes = price_data[sym]["volumes"]
        rec = process_ticker(r, prices, volumes)
        if rec:
            results.append(rec)
    fresh_count = len(results)
    print(f"  Fresh records: {fresh_count}")
    print()

    # Phase 4: Stale fallback
    current_symbols = {r["symbol"] for r in records}
    fresh_symbols = {x["symbol"] for x in results}
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
              + (f" ... +{len(stale_used) - 10}" if len(stale_used) > 10 else ""))

    truly_missing = missing_symbols - set(stale_used)
    if truly_missing:
        print(f"  WARN: {len(truly_missing)} tickers без никакви данни")
        print(f"    Symbols: {', '.join(list(truly_missing)[:10])}")
    print()

    # Sort by momentum score, assign rank
    results.sort(key=lambda x: x["momentumScore"], reverse=True)
    for rank, r in enumerate(results, 1):
        r["rank"] = rank

    # Meta
    total_count = len(results)
    fresh_rate = fresh_count / len(records) if records else 0
    total_rate = total_count / len(records) if records else 0

    meta = {
        "updated":         end_dt.strftime("%Y-%m-%d %H:%M UTC"),
        "source":          "iShares EXSA + yfinance",
        "count":           total_count,
        "fresh_count":     fresh_count,
        "stale_count":     len(stale_used),
        "missing_count":   len(truly_missing),
        "retry_recovered": retry_recovered,
        "fresh_rate":      round(fresh_rate, 4),
        "total_rate":      round(total_rate, 4),
        "vol_ok":          sum(1 for r in results if r["avgVolume"] > 0),
        "stale":           False,
        "warnings":        [],
    }

    if fresh_rate < MIN_FRESH_RATE:
        warn = (f"Only {fresh_count}/{len(records)} fresh "
                f"({fresh_rate:.1%} < {MIN_FRESH_RATE:.0%})")
        meta["warnings"].append(warn)
        print(f"WARN: {warn}")

    if total_rate < MIN_TOTAL_RATE and prev_data:
        print(f"HARD FAIL: only {total_count} records — keeping previous data.json")
        meta["stale"] = True
        meta["stale_reason"] = f"fetch returned {total_count}/{len(records)}"
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
    print(f"  missing     : {len(truly_missing)}")
    print(f"avgVolume OK  : {meta['vol_ok']}/{total_count}")
    print(f"Top 5         : {[r['symbol'] for r in results[:5]]}")
    print(f"Updated       : {meta['updated']}")

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
