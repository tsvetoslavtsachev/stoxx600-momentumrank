"""
Tests for the momentum scoring engine — focused on the missing-data trap.

Before the fix, calc_return returned 0.0 when a stock lacked enough history for
a window (e.g. a recent STOXX 600 addition with < 12 months of data). A 0.0
return maps to sig(0)=50 — a neutral signal — silently dragging a strong stock's
score toward the middle. Now calc_return returns NaN, and calc_momentum_score
drops the missing term and reweights onto the windows that exist.

Run from the repo root:  python -m pytest tests/ -v
"""

from __future__ import annotations

import math

import pandas as pd

from fetch_data import calc_momentum_score, calc_return, process_ticker, size_score_from_weight


def _series(n: int, start: float = 100.0, step: float = 0.5) -> pd.Series:
    idx = pd.date_range("2023-01-01", periods=n, freq="B")
    return pd.Series([start + i * step for i in range(n)], index=idx)


def _sig(x: float, scale: float) -> float:
    return 100.0 / (1.0 + math.exp(max(-50, min(50, -x / scale))))


def _s_vol(vol: float) -> float:
    return 100.0 / (1.0 + math.exp(max(-50, min(50, (vol - 25) / 10))))


def _info(symbol="NEW", weight=1.5):
    return {
        "symbol": symbol, "name": "Co", "sector": "Tech", "country": "DE",
        "exchange": "XETRA", "currency": "EUR", "weight": weight,
    }


# ── calc_return ───────────────────────────────────────────────────────────────

def test_calc_return_nan_on_insufficient_history():
    s = _series(30)
    assert math.isnan(calc_return(s, 252))
    assert math.isnan(calc_return(s, 126))
    assert not math.isnan(calc_return(s, 21))


# ── Regression guard: full data unchanged ─────────────────────────────────────

def test_score_identical_with_full_data():
    got = calc_momentum_score(
        r1m=5.0, r3m=8.0, r6m=12.0, r12m=20.0, vol=18.0, sharpe=1.2, weight_pct=2.5
    )
    s_size = size_score_from_weight(2.5)  # 2.5 → 100
    expected = round(
        _sig(20.0, 30) * 0.30 + _sig(12.0, 20) * 0.25 + _sig(8.0, 15) * 0.20
        + _sig(5.0, 10) * 0.10 + _sig(1.2, 1.0) * 0.10 + _s_vol(18.0) * 0.03
        + s_size * 0.02,
        1,
    )
    assert got == expected


# ── The fix: missing long-window returns reweight instead of neutralizing ──────

def test_missing_long_returns_reweight_not_neutralize():
    strong = dict(r1m=15.0, r3m=12.0, vol=16.0, sharpe=1.5, weight_pct=2.5)
    fake_zero = calc_momentum_score(r6m=0.0, r12m=0.0, **strong)
    reweighted = calc_momentum_score(r6m=float("nan"), r12m=float("nan"), **strong)

    assert not math.isnan(reweighted)
    assert reweighted > fake_zero


def test_partial_score_matches_manual_reweight():
    weight_pct = 0.5
    got = calc_momentum_score(
        r1m=6.0, r3m=9.0, r6m=float("nan"), r12m=float("nan"),
        vol=20.0, sharpe=1.0, weight_pct=weight_pct,
    )
    num = (
        _sig(9.0, 15) * 0.20 + _sig(6.0, 10) * 0.10 + _sig(1.0, 1.0) * 0.10
        + _s_vol(20.0) * 0.03 + size_score_from_weight(weight_pct) * 0.02
    )
    den = 0.20 + 0.10 + 0.10 + 0.03 + 0.02
    assert got == round(num / den, 1)


# ── process_ticker flags partial history ───────────────────────────────────────

def test_process_ticker_flags_partial_history():
    prices = _series(100)
    volumes = pd.Series([1e6] * 100, index=prices.index)

    rec = process_ticker(_info("NEW", 1.5), prices, volumes)

    assert rec["dataQuality"] == "partial_history"
    assert math.isnan(rec["return12m"])
    assert math.isnan(rec["return6m"])
    assert not math.isnan(rec["return1m"])
    assert not math.isnan(rec["momentumScore"])


def test_process_ticker_full_history_is_ok():
    prices = _series(300)
    volumes = pd.Series([1e6] * 300, index=prices.index)

    rec = process_ticker(_info("OLD", 1.5), prices, volumes)

    assert rec["dataQuality"] == "ok"
    assert not math.isnan(rec["return12m"])
