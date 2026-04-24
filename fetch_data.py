# MomentumRank STOXX Europe 600 - fetch_data.py v1
#
# Fetch-ва 600-те компании на STOXX Europe 600 индекса от Wikipedia,
# добавя Yahoo Finance суфикси по страна, изтегля 400 дни история
# и изчислява momentum метрики идентично с S&P 500 версията.
#
# Особености спрямо S&P 500 версията:
#   - Суфикс mapping по страна (17 борси)
#   - Пазарна капитализация се нормализира в EUR (GBp → GBP → EUR)
#   - Добавено поле "country" и "currency" за dashboard-а
#   - ICB сектори → GICS-style имена за съвместимост с frontend

import json, time, random, math, sys
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import numpy as np
import requests
import yfinance as yf

OUTPUT_FILE   = "data.json"
LOOKBACK_DAYS = 400
RATE_SLEEP    = (0.35, 0.75)
MAX_RETRIES   = 3

# ── Yahoo Finance суфикси по страна ──────────────────────────────────────────
COUNTRY_SUFFIX = {
    "United Kingdom":  ".L",
    "Germany":         ".DE",
    "France":          ".PA",
    "Switzerland":     ".SW",
    "Netherlands":     ".AS",
    "Sweden":          ".ST",
    "Spain":           ".MC",
    "Italy":           ".MI",
    "Finland":         ".HE",
    "Belgium":         ".BR",
    "Norway":          ".OL",
    "Denmark":         ".CO",
    "Austria":         ".VI",
    "Portugal":        ".LS",
    "Ireland":         ".IR",
    "Luxembourg":      ".LU",
    "Poland":          ".WA",
    "Greece":          ".AT",
    "Israel":          ".TA",
    "Bermuda":         ".L",    # обикновено листнати в Лондон
}

# Fallback суфикси при неуспех (опитваме в ред)
COUNTRY_SUFFIX_FALLBACKS = {
    "United Kingdom": [".L", ".IL"],
    "Ireland":        [".IR", ".L", ".ID"],
    "Luxembourg":     [".LU", ".DE", ".PA"],
    "Switzerland":    [".SW", ".S"],     # някои на .S (SIX Exchange)
}

# ── ICB → GICS-style имена (за съответствие с frontend на S&P 500) ───────────
SECTOR_MAP = {
    "Industrial Goods and Services":        "Industrials",
    "Industrials":                          "Industrials",
    "Health Care":                          "Healthcare",
    "Banks":                                "Financial Services",
    "Financial Services":                   "Financial Services",
    "Insurance":                            "Financial Services",
    "Consumer Products and Services":       "Consumer Cyclical",
    "Automobiles and Parts":                "Consumer Cyclical",
    "Retail":                               "Consumer Cyclical",
    "Travel and Leisure":                   "Consumer Cyclical",
    "Media":                                "Communication Services",
    "Telecommunications":                   "Communication Services",
    "Technology":                           "Technology",
    "Chemicals":                            "Basic Materials",
    "Basic Resources":                      "Basic Materials",
    "Construction and Materials":           "Industrials",
    "Real Estate":                          "Real Estate",
    "Energy":                               "Energy",
    "Utilities":                            "Utilities",
    "Food, Beverage and Tobacco":           "Consumer Defensive",
    "Personal Care, Drug and Grocery Stores": "Consumer Defensive",
}


# ── Зареди STOXX 600 списък от Wikipedia ─────────────────────────────────────

def get_stoxx600_tickers():
    """
    Парсва Wikipedia STOXX Europe 600 страницата и добавя Yahoo суфикси.
    Връща list[dict] с: symbol, name, sector, country, currency
    """
    url = "https://en.wikipedia.org/wiki/STOXX_Europe_600"
    resp = requests.get(url, timeout=20, headers={"User-Agent": "MomentumRank-Bot/1.0"})
    resp.raise_for_status()

    dfs = pd.read_html(StringIO(resp.text))
    # Таблицата с конституентите е най-голямата (~534 реда, 5 колони)
    main_df = max(dfs, key=lambda d: len(d))
    main_df.columns = [str(c).strip() for c in main_df.columns]

    # Намери правилните колони
    ticker_col  = next(c for c in main_df.columns if "ticker" in c.lower() or "symbol" in c.lower())
    name_col    = next(c for c in main_df.columns if "company" in c.lower() or "name" in c.lower())
    sector_col  = next((c for c in main_df.columns if "sector" in c.lower() or "icb" in c.lower()), None)
    country_col = next((c for c in main_df.columns if "country" in c.lower()), None)

    records = []
    for _, row in main_df.iterrows():
        raw_ticker  = str(row[ticker_col]).strip()
        name        = str(row[name_col]).strip()
        raw_sector  = str(row[sector_col]).strip() if sector_col else ""
        country     = str(row[country_col]).strip() if country_col else "Unknown"

        if not raw_ticker or raw_ticker == "nan":
            continue

        # Добави Yahoo суфикс
        suffix  = COUNTRY_SUFFIX.get(country, ".L")
        # Почисти тикера: интервали → тире, специални символи
        ticker_clean = (
            raw_ticker
            .replace(" ", "-")
            .replace("/", "-")
            .upper()
        )
        yahoo_sym = ticker_clean + suffix

        # Нормализиран сектор
        sector = SECTOR_MAP.get(raw_sector, raw_sector)

        # Валута по суфикс
        ccy_map = {
            ".L": "GBp", ".DE": "EUR", ".PA": "EUR", ".SW": "CHF",
            ".AS": "EUR", ".ST": "SEK", ".MC": "EUR", ".MI": "EUR",
            ".HE": "EUR", ".BR": "EUR", ".OL": "NOK", ".CO": "DKK",
            ".VI": "EUR", ".LS": "EUR", ".IR": "EUR", ".LU": "EUR",
            ".WA": "PLN", ".AT": "EUR", ".TA": "ILS", ".S": "CHF",
        }
        currency = ccy_map.get(suffix, "EUR")

        records.append({
            "symbol":   yahoo_sym,
            "raw_sym":  raw_ticker,
            "name":     name,
            "sector":   sector,
            "country":  country,
            "currency": currency,
            "suffix":   suffix,
        })

    print(f"  Loaded {len(records)} constituents from Wikipedia")
    return records


# ── EUR конверсия за пазарна капитализация ────────────────────────────────────

# Приблизителни FX курсове спрямо EUR (актуализирани периодично от скрипта)
_FX_TO_EUR = {}

def get_fx_rates():
    """Вземи приблизителни FX курсове към EUR чрез yfinance."""
    global _FX_TO_EUR
    pairs = {
        "GBp": ("GBPEUR=X", 0.01),   # GBp = GBP/100
        "GBP": ("GBPEUR=X", 1.0),
        "CHF": ("CHFEUR=X", 1.0),
        "SEK": ("SEKEUR=X", 1.0),
        "NOK": ("NOKEUR=X", 1.0),
        "DKK": ("DKKEUR=X", 1.0),
        "PLN": ("PLNEUR=X", 1.0),
        "ILS": ("ILSEUR=X", 1.0),
        "EUR": (None,        1.0),
        "USD": ("USDEUR=X", 1.0),
    }
    rates = {"EUR": 1.0}
    for ccy, (pair, scale) in pairs.items():
        if pair is None:
            rates[ccy] = 1.0
            continue
        try:
            t   = yf.Ticker(pair)
            fx  = getattr(t.fast_info, "last_price", None) or getattr(t.fast_info, "regular_market_price", None)
            if fx and fx > 0:
                rates[ccy] = float(fx) * scale
        except Exception:
            pass
    # Fallback defaults ако API не отговаря
    defaults = {"GBp": 0.01176, "GBP": 1.176, "CHF": 1.048, "SEK": 0.0873,
                "NOK": 0.0856, "DKK": 0.134, "PLN": 0.232, "ILS": 0.248}
    for ccy, val in defaults.items():
        if ccy not in rates or rates[ccy] <= 0:
            rates[ccy] = val
    _FX_TO_EUR = rates
    return rates

def to_eur(value, currency):
    rate = _FX_TO_EUR.get(currency, 1.0)
    return value * rate


# ── Fetch данни за един тикер ─────────────────────────────────────────────────

def fetch_ticker_data(info, start_dt, end_dt):
    """
    Опитва primary суфикс. При неуспех опитва fallback суфикси.
    Връща (prices, volumes, market_cap_eur, avg_volume, used_symbol, currency)
    """
    country = info.get("country", "")
    base    = info["raw_sym"].replace(" ", "-").replace("/", "-").upper()
    suffix  = info["suffix"]
    primary = base + suffix

    # Списък с тикери за опитване
    candidates = [primary]
    for fb in COUNTRY_SUFFIX_FALLBACKS.get(country, []):
        alt = base + fb
        if alt != primary:
            candidates.append(alt)

    for symbol in candidates:
        result = _try_fetch(symbol, start_dt, end_dt, info.get("currency", "EUR"))
        if result is not None:
            return result + (symbol,)

    return None, None, 0, 0, primary


def _try_fetch(symbol, start_dt, end_dt, currency):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(symbol)
            hist   = ticker.history(
                start=start_dt.strftime("%Y-%m-%d"),
                end=end_dt.strftime("%Y-%m-%d"),
                auto_adjust=True,
                actions=False,
            )
            if hist.empty or len(hist) < 60:
                return None

            prices  = hist["Close"].dropna().astype(float)
            volumes = hist["Volume"].fillna(0).astype(float)

            # Пазарна капитализация
            market_cap_local = 0
            avg_volume       = 0
            actual_currency  = currency

            try:
                fi = ticker.fast_info
                market_cap_local = float(getattr(fi, "market_cap", 0) or 0)
                avg_volume       = int(getattr(fi, "three_month_average_volume", 0) or 0)
                actual_currency  = getattr(fi, "currency", currency) or currency
            except Exception:
                pass

            # Fallback volume от история
            if avg_volume == 0 and len(volumes) > 0:
                lb = volumes.iloc[-63:] if len(volumes) >= 63 else volumes
                avg_volume = int(lb[lb > 0].mean()) if (lb > 0).any() else 0

            # Fallback cap от .info
            if market_cap_local == 0:
                try:
                    inf = ticker.info
                    market_cap_local = float(inf.get("marketCap", 0) or 0)
                    actual_currency  = inf.get("currency", currency) or currency
                    if avg_volume == 0:
                        avg_volume = int(inf.get("averageDailyVolume3Month", 0) or inf.get("averageVolume", 0) or 0)
                except Exception:
                    pass

            # Конвертирай market cap в EUR
            market_cap_eur = int(to_eur(market_cap_local, actual_currency))

            return prices, volumes, market_cap_eur, avg_volume

        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt + random.uniform(0, 1))
            else:
                return None
    return None


# ── Изчисления ────────────────────────────────────────────────────────────────

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
    """rf = 2.5% (приблизителен 10Y Bund yield)"""
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
    roll_max  = prices.expanding().max()
    dd_series = (prices - roll_max) / roll_max * 100
    return round(float(dd_series.min()), 2)


def calc_momentum_score(r1m, r3m, r6m, r12m, vol, sharpe, market_cap_eur):
    def sig(x, scale):
        return 100.0 / (1.0 + math.exp(max(-50, min(50, -x / scale))))

    s12  = sig(r12m, 30)
    s6   = sig(r6m,  20)
    s3   = sig(r3m,  15)
    s1   = sig(r1m,  10)
    s_sh = sig(sharpe, 1.0)
    s_vol = 100.0 / (1.0 + math.exp(max(-50, min(50, (vol - 25) / 10))))

    # Market cap thresholds в EUR
    if   market_cap_eur >= 200e9: s_cap = 100
    elif market_cap_eur >=  50e9: s_cap = 75
    elif market_cap_eur >=  10e9: s_cap = 50
    elif market_cap_eur >       0: s_cap = 25
    else:                          s_cap = 50

    return round(
        s12  * 0.30 + s6   * 0.25 + s3    * 0.20 + s1    * 0.10 +
        s_sh * 0.10 + s_vol * 0.03 + s_cap * 0.02,
        1,
    )


# ── Обработка на един тикер ───────────────────────────────────────────────────

def process_ticker(info, start_dt, end_dt):
    time.sleep(random.uniform(*RATE_SLEEP))

    prices, volumes, market_cap_eur, avg_volume, used_sym = fetch_ticker_data(
        info, start_dt, end_dt
    )

    if prices is None or len(prices) < 60:
        return None

    r1m  = calc_return(prices, 21)
    r3m  = calc_return(prices, 63)
    r6m  = calc_return(prices, 126)
    r12m = calc_return(prices, 252)
    vol  = calc_volatility(prices)
    shp  = calc_sharpe(prices)
    dd   = calc_drawdown(prices)

    price   = round(float(prices.iloc[-1]), 4)
    day_chg = round((prices.iloc[-1] / prices.iloc[-2] - 1) * 100, 2) if len(prices) >= 2 else 0.0

    p52    = prices.iloc[-252:] if len(prices) >= 252 else prices
    high52 = round(float(p52.max()), 4)
    low52  = round(float(p52.min()), 4)
    dd52   = round((price - high52) / high52 * 100, 2) if high52 > 0 else 0.0

    score = calc_momentum_score(r1m, r3m, r6m, r12m, vol, shp, market_cap_eur)

    return {
        "symbol":        used_sym,
        "name":          info["name"],
        "sector":        info["sector"],
        "country":       info["country"],
        "currency":      info["currency"],
        "price":         price,
        "marketCap":     market_cap_eur,   # в EUR
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
        "weight":        0.0,  # попълва се в main()
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("MomentumRank - STOXX Europe 600 - fetch_data.py v1")
    print("=" * 55)

    end_dt   = datetime.utcnow()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    print(f"Period : {start_dt.date()} → {end_dt.date()}")

    # FX rates
    print("Fetching FX rates...")
    rates = get_fx_rates()
    print(f"  GBp={rates.get('GBp'):.5f}  CHF={rates.get('CHF'):.4f}  "
          f"SEK={rates.get('SEK'):.5f}  NOK={rates.get('NOK'):.5f}  "
          f"DKK={rates.get('DKK'):.5f}")

    print("Loading STOXX 600 list from Wikipedia...")
    tickers = get_stoxx600_tickers()
    print(f"  {len(tickers)} tickers loaded")
    print()

    results = []
    skipped = []
    total   = len(tickers)

    for i, t in enumerate(tickers, 1):
        sym = t["symbol"]
        print(f"  [{i:3d}/{total}] {sym:<16}", end="", flush=True)

        rec = process_ticker(t, start_dt, end_dt)

        if rec:
            results.append(rec)
            cap_b = rec["marketCap"] / 1e9 if rec["marketCap"] else 0
            vol_m = rec["avgVolume"] / 1e6 if rec["avgVolume"] else 0
            print(
                f"  score={rec['momentumScore']:.1f}"
                f"  r12m={rec['return12m']:+.1f}%"
                f"  vol={vol_m:.1f}M"
                f"  cap=€{cap_b:.1f}B"
                f"  [{rec['country'][:3]}]"
            )
        else:
            skipped.append(sym)
            print("  SKIPPED")

        if i % 50 == 0:
            print(f"  --- checkpoint {i}/{total} ---")
            time.sleep(3)

    # Нормализирай weight = % от общ market cap в EUR
    total_cap = sum(r["marketCap"] for r in results if r["marketCap"] > 0)
    for r in results:
        if total_cap > 0 and r["marketCap"] > 0:
            r["weight"] = round(r["marketCap"] / total_cap * 100, 4)
        else:
            r["weight"] = 0.0

    # Сортирай и добави rank
    results.sort(key=lambda x: x["momentumScore"], reverse=True)
    for rank, r in enumerate(results, 1):
        r["rank"] = rank

    # Запази data.json (директен масив — съвместимост с index.html)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, separators=(",", ":"))

    # Meta файл за debugging
    meta = {
        "updated":  datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "count":    len(results),
        "skipped":  len(skipped),
        "vol_ok":   sum(1 for r in results if r["avgVolume"] > 0),
        "cap_ok":   sum(1 for r in results if r["marketCap"] > 0),
        "skipped_list": skipped[:30],
    }
    with open("data_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print()
    print(f"Done         : {len(results)} records → {OUTPUT_FILE}")
    print(f"Skipped      : {len(skipped)}")
    print(f"avgVolume OK : {meta['vol_ok']}/{len(results)}")
    print(f"marketCap OK : {meta['cap_ok']}/{len(results)}")
    print(f"Top 5        : {[r['symbol'] for r in results[:5]]}")
    print(f"Updated      : {meta['updated']}")

    if meta["vol_ok"] < len(results) * 0.75:
        print("WARNING: >25% missing avgVolume")
        sys.exit(1)


if __name__ == "__main__":
    main()
