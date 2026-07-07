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


# ── Skip-month property: the 12-1 window excludes the most recent month ─────────
# (audit 2026-07-07, П2б). The canonical momentum signal is 12-1: the window ends
# ~21 trading days BEFORE the last bar, so the most recent month never enters it
# (short-term reversal). These lock the skip so a future edit cannot quietly drop
# back to 12-0.

def test_skip_month_matches_manual_iloc():
    # 12-1 return == prices.iloc[-22] / prices.iloc[-253] - 1  (close[t-252] → close[t-21]).
    prices = _series(300, start=100.0, step=0.7)
    manual = round((prices.iloc[-22] / prices.iloc[-253] - 1) * 100, 2)
    assert calc_return(prices, 252, skip=21) == manual


def test_skip_month_excludes_last_month():
    prices = _series(300)  # enough history for the 253-bar window
    r_12_1 = calc_return(prices, 252, skip=21)

    tampered = prices.copy()
    tampered.iloc[-21:] = tampered.iloc[-21:] * 1.5  # blow up ONLY the skipped month
    assert calc_return(tampered, 252, skip=21) == r_12_1  # skipped month does not enter 12-1

    # Witness: the plain 12-0 point-to-point return DOES move when the last month
    # changes — so the equality above is a real skip, not a degenerate no-op.
    assert calc_return(tampered, 252, skip=0) != calc_return(prices, 252, skip=0)


# ── Regression guard: full data unchanged ─────────────────────────────────────

def test_score_identical_with_full_data():
    """All inputs present → present weights sum to 0.87 → score is the weighted
    blend renormalised by that total. r1m and vol are NOT part of the formula
    (П2б: reversal term dropped, vol double-count removed)."""
    got = calc_momentum_score(
        r1m=5.0, r3m=8.0, r6m=12.0, r12m=20.0, vol=18.0, sharpe=1.2, weight_pct=2.5
    )
    s_size = size_score_from_weight(2.5)  # 2.5 → 100
    num = (
        _sig(20.0, 30) * 0.30 + _sig(12.0, 20) * 0.25 + _sig(8.0, 15) * 0.20
        + _sig(1.2, 1.0) * 0.10 + s_size * 0.02
    )
    den = 0.30 + 0.25 + 0.20 + 0.10 + 0.02
    assert got == round(num / den, 1)


# ── The fix: missing long-window returns reweight instead of neutralizing ──────

def test_missing_long_returns_reweight_not_neutralize():
    strong = dict(r1m=15.0, r3m=12.0, vol=16.0, sharpe=1.5, weight_pct=2.5)
    fake_zero = calc_momentum_score(r6m=0.0, r12m=0.0, **strong)
    reweighted = calc_momentum_score(r6m=float("nan"), r12m=float("nan"), **strong)

    assert not math.isnan(reweighted)
    assert reweighted > fake_zero


def test_partial_score_matches_manual_reweight():
    """Exact arithmetic: r6m/r12m missing → present weights r3m+sharpe+size = 0.32,
    renormalised (r1m and vol are not part of the formula)."""
    weight_pct = 0.5
    got = calc_momentum_score(
        r1m=6.0, r3m=9.0, r6m=float("nan"), r12m=float("nan"),
        vol=20.0, sharpe=1.0, weight_pct=weight_pct,
    )
    num = _sig(9.0, 15) * 0.20 + _sig(1.0, 1.0) * 0.10 + size_score_from_weight(weight_pct) * 0.02
    den = 0.20 + 0.10 + 0.02
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


# ── Formula contract: golden vectors (одит 2026-07-07, П2б) ───────────────────
# The expected scores below are PINNED NUMERIC LITERALS computed from the formula
# as signed off on 2026-07-07 after П2б (weights 0.30/0.25/0.20/0.10/0.02, scales
# 30/20/15, Sharpe scale 1.0; r12m is the 12-1 skip-month window; the r1m reversal
# term and the standalone inverted-vol term are EXCLUDED). Unlike the recompute-
# style guard above, nothing here re-derives the expected value through the formula
# — so ANY change to weights, scales or the sigmoid breaks this test. That is the
# point: it is a contract. If a formula change is intended, update these literals
# consciously, in their own commit, with an explicit sign-off — never silently.

from momentum_core import momentum_blend  # noqa: E402  (vendored core, byte-identical in both repos)


def test_contract_golden_vectors():
    nan = float("nan")
    cases = [
        # (r1m,  r3m,   r6m,   r12m,  vol,  sharpe, size) -> pinned score
        ((5.0,   8.0,   12.0,  20.0,  18.0,  1.2,  50.0),  65.8),  # typical positive
        ((-5.0, -10.0, -15.0, -25.0,  35.0, -0.8,  25.0),  31.6),  # bearish
        ((25.0,  45.0,  80.0, 120.0,  45.0,  2.5, 100.0),  96.9),  # saturating bull
        ((5.0,   8.0,   12.0,  nan,   18.0,  1.2,  50.0),  65.7),  # r12m missing -> reweight
    ]
    for args, want in cases:
        assert momentum_blend(*args) == want, (args, want)


def test_contract_r1m_and_vol_excluded():
    # П2б: r1m (short-term reversal) and the standalone vol term (double-count) are
    # NOT part of the score. Changing ONLY them must leave the blend untouched — this
    # locks the exclusion so a future edit cannot silently re-add either term.
    base = momentum_blend(5.0, 8.0, 12.0, 20.0, 18.0, 1.2, 50.0)
    assert momentum_blend(999.0, 8.0, 12.0, 20.0, 999.0, 1.2, 50.0) == base
    assert momentum_blend(-30.0, 8.0, 12.0, 20.0, 3.0, 1.2, 50.0) == base


def test_contract_only_size_present():
    # Every price term missing -> the ever-present size bucket IS the score.
    nan = float("nan")
    assert momentum_blend(nan, nan, nan, nan, nan, nan, 75.0) == 75.0
