"""
momentum_core.py — shared core for the MomentumRank twins.

VENDORED FILE. This exact file is byte-identical in two repos:
  - SP500-momentumrank    (S&P 500,           rf=4.5%, size from marketCap)
  - stoxx600-momentumrank (STOXX Europe 600,  rf=2.5%, size from iShares weight)

Canonical source + sync tool + the US/EU parameter journal live in tsachev-ops:
  initiatives/22-repo-revision-program/shared/momentum_core.py
  initiatives/22-repo-revision-program/shared/sync_momentum_core.py
  initiatives/22-repo-revision-program/shared/momentum-twins-diff-journal.md

DO NOT edit a repo copy in isolation: edit the canonical, run sync_momentum_core.py
(it overwrites both repo copies and verifies a matching sha256), commit both. A fix
made here propagates to BOTH dashboards — that is the whole point of vendoring it.

Everything in this module is the price-math + Yahoo plumbing the two dashboards
share verbatim. Anything that differs between the US and EU pipelines — constituent
discovery, the size-score source, the risk-free rate, the output record schema, the
meta block — stays in each repo's fetch_data.py and is passed in as a PARAMETER
(rf for calc_sharpe, a precomputed size_score for momentum_blend). The US/EU values
are NEVER hardcoded in this module.
"""

from __future__ import annotations

import json
import math
import os
import random
import time

import numpy as np
import pandas as pd
import yfinance as yf

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False


# ── Shared download config ────────────────────────────────────────────────────
BATCH_SIZE = 100
BATCH_PAUSE = 2
BULK_MAX_RETRIES = 3

RETRY_SLEEP = 1.5
RETRY_MAX_ATTEMPTS = 2


# ── curl_cffi session ─────────────────────────────────────────────────────────
def make_session():
    if not HAS_CURL_CFFI:
        return None
    try:
        return curl_requests.Session(impersonate="chrome")
    except Exception as e:
        print(f"WARN: curl_cffi session init failed: {e}")
        return None


# ── Зареди предишен data.json (пълни записи) ───────────────────────────────────
def load_previous_data(output_file="data.json"):
    """Връща {symbol: full_record_dict} от предишен run."""
    if not os.path.exists(output_file):
        return {}
    try:
        with open(output_file, "r") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return {}
        return {r["symbol"]: r for r in data if "symbol" in r}
    except Exception as e:
        print(f"WARN: could not load previous {output_file}: {e}")
        return {}


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

                # Single-ticker fetch → flat columns, но yfinance понякога връща
                # MultiIndex дори за един тикер.
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


# ── Phase 1: Bulk price download ──────────────────────────────────────────────
def bulk_download_prices(records, start_dt, end_dt, session=None):
    """records = iterable of dicts that each carry a "symbol" key."""
    result = {}
    symbols = [r["symbol"] for r in records]

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
    missing = [r["symbol"] for r in records if r["symbol"] not in result]
    recovered = 0
    if missing:
        retry_result = retry_missing_prices(missing, start_dt, end_dt, session=session)
        result.update(retry_result)
        recovered = len(retry_result)

    return result, recovered


# ── Metrics (price-only, market-agnostic) ─────────────────────────────────────
def calc_return(prices, trading_days):
    # Insufficient history → NaN, NOT 0.0. A forced 0.0 looks like a real "flat"
    # return: sig(0)=50, a neutral signal that silently pulls a partial-history
    # stock's momentum score toward the middle. NaN instead drops the term from
    # the score (see momentum_blend) and is flagged as partial_history.
    if prices is None or len(prices) < trading_days + 1:
        return float("nan")
    return round((prices.iloc[-1] / prices.iloc[-(trading_days + 1)] - 1) * 100, 2)


def calc_volatility(prices, trading_days=252):
    if prices is None or len(prices) < 22:
        return 0.0
    rets = np.log(prices / prices.shift(1)).dropna()
    if len(rets) > trading_days:
        rets = rets.iloc[-trading_days:]
    return round(float(rets.std() * math.sqrt(252) * 100), 2)


def calc_sharpe(prices, rf, trading_days=252):
    """rf (risk-free rate) is a US/EU parameter passed by each repo's fetch_data.py
    (S&P 500: 4.5% ≈ US T-bill; STOXX 600: 2.5% ≈ 10Y Bund). It is required — there
    is deliberately no default, so a caller can never silently inherit the wrong
    market's rate."""
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


def _is_missing(x):
    return x is None or (isinstance(x, float) and math.isnan(x))


def momentum_blend(r1m, r3m, r6m, r12m, vol, sharpe, size_score):
    """Shared momentum blend. `size_score` (0-100) is a US/EU parameter computed by
    each repo's fetch_data.py from its own size source — S&P 500 from marketCap
    brackets, STOXX 600 from iShares weight brackets — so the blend itself stays
    identical across both dashboards."""
    def sig(x, scale):
        exp_arg = max(-50, min(50, -x / scale))
        return 100.0 / (1.0 + math.exp(exp_arg))

    # (weight, component) pairs. A missing input (NaN — e.g. a return window the
    # stock lacks the history for) is dropped and its weight redistributed across
    # the present components, so missing history does NOT silently pull the score
    # toward the neutral sig(0)=50. With full data the present weights sum to 1.0,
    # so the result is identical to the original weighted blend.
    terms = []
    if not _is_missing(r12m):
        terms.append((0.30, sig(r12m, 30)))
    if not _is_missing(r6m):
        terms.append((0.25, sig(r6m, 20)))
    if not _is_missing(r3m):
        terms.append((0.20, sig(r3m, 15)))
    if not _is_missing(r1m):
        terms.append((0.10, sig(r1m, 10)))
    if not _is_missing(sharpe):
        terms.append((0.10, sig(sharpe, 1.0)))
    if not _is_missing(vol):
        terms.append((0.03, 100.0 / (1.0 + math.exp(max(-50, min(50, (vol - 25) / 10))))))

    # The size bucket is always present (caller passes a neutral 50 when size is
    # unknown), so the score is never fully undefined.
    terms.append((0.02, size_score))

    weight_total = sum(w for w, _ in terms)
    if weight_total == 0:
        return float("nan")
    return round(sum(w * c for w, c in terms) / weight_total, 1)
