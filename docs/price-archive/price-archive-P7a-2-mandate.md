# price-archive P7a-2 — STOXX600 stock citizen (MANDATE)

> **STATUS: build-ready mandate, written in the cloud/dashboard session.** The
> price-archive monorepo (`collectors/`, `initiatives/`, the P7a template) is NOT
> reachable here, so this mandate is committed to the dashboard repo as a handoff —
> the desktop monorepo session (`C:\Projects`) executes the build (TASK 2) against the
> real `collectors/`. Mirror of the P7a (SP500) mandate; reuses the P1 backend.
>
> Inputs carried in:
> - FX-ticker risk research: `docs/price-archive/P7a-2-foreign-exchange-risk-investigation.md`
>   (same branch). The pinned decisions below are its output.
> - P7a (SP500) is ✅ pushed (collectors `52a9308` · ops `5dc357b`) — this is its STOXX twin.

---

## 1. Scope
- **STOXX600 current members — 597 (verified** against live iShares EXSA holdings;
  `data_meta.json count: 597`, 596 fresh / 1 stale / 3 missing).
- ~6y TA depth (`history_period_stock`, already exists in `fetch_prices.py`).
- **Survivorship-flagged** (current-members-only → `backtest_valid: false`).
- Mirror of P7a: **REUSE** `register_catalog.py` `entry()` / `fetch_prices.py` /
  `to_datacore.py` / push path + dual-basis + split_factor. **Nothing new in the backend.**
- **Out of scope** (separate phases): P7b stable-id · P8 real backfill + CI + split-heal
  (confirm split-heal also covers EU corporate actions) · P9 STOXX dashboard cut-over ·
  PR of the dashboard handoff branch (stays a branch; fold its content here, then optionally delete).

## 2. Pinned decisions (from research handoff)
1. **Universe source** = iShares EXSA holdings CSV — reuse the dashboard Phase-0 fetch
   (`stoxx600-momentumrank/fetch_data.py::fetch_ishares_constituents`). 597 current members.
2. **family** = `"stock"`; survivorship = `backtest_valid: false` + `members_basis: current-only`
   (machine-readable on EVERY STOXX series — same flag shape as the SP500 stocks).
3. **series_id** = `px_` + `lower(symbol)` with `.`→`_` and `-`→`_`, **exchange suffix retained**.
   - `BT-A.L → px_bt_a_l`, `SAN.MC → px_san_mc`, `NOVO-B.CO → px_novo_b_co`.
   - Retaining the suffix makes cross-archive collision **structurally zero** (bare SP500/ETF
     ids can never equal a suffixed STOXX id) AND disambiguates intra-STOXX base-dups
     (`SAN.PA` Sanofi vs `SAN.MC` Santander; also `UNI`×2, `BOL`×2).
4. **CURRENCY / QUOTE BASIS — DECISION (option a, recommended; sign-off if you prefer b):**
   - **(a — ADOPTED)** Store the **RAW** price as fetched (London `.L`/GBP comes in **pence/GBX**);
     record `currency` + `quote_basis` per series in the catalog entry; the **consumer normalizes
     on read** (÷100 for GBX). Matches the archive philosophy "store raw, recompute on read",
     same spirit as `split_factor`. No lossy transform baked into the archive.
   - (b — alternative) Normalize GBX→GBP at fetch (÷100) so all series are in major units.
     Simpler consumers, but bakes a transform in; rejected to keep the archive raw-faithful.
   - **Either way:** `currency` + `quote_basis` are stored per series. Note suffix ≠ currency —
     `IHG.L` and `CPG.L` are quoted in **USD** on the LSE, so currency comes from iShares,
     never inferred from the suffix.
5. **category** = the 11-bucket sector (reuse dashboard `SECTOR_DE_EN` normalization:
   Technology, Financial Services, Industrials, Energy, Consumer Defensive, Communication
   Services, Healthcare, Utilities, Basic Materials, Consumer Cyclical, Real Estate).
6. **per-family depth** = `history_period_stock` (~6y, already exists — do not add a new knob).
7. **REUSE** `register_catalog` `entry()` / fetch / push + dual-basis + split_factor exactly
   as P7a. `momentum_core` is `auto_adjust=True` only → for STOXX, add **raw** Close + derive
   **split_factor** alongside adjusted (the dual-basis the P7a stock path already produces).

## 3. Ticker / exchange resolution (REUSE, do not re-derive)
Reuse the dashboard's battle-tested resolver in `stoxx600-momentumrank/fetch_data.py`:
- `EXCHANGE_SUFFIX` (16 exchanges: `.L .DE .F .PA .SW .AS .ST .MC .MI .HE .BR .OL .CO .VI .IR .LS .WA .AT .TA`),
- `COUNTRY_SUFFIX_FALLBACK` (resilient to iShares exchange renames),
- `TICKER_OVERRIDES_BY_EXCHANGE` (e.g. Roche `ROP`→`ROG`, BT `BT-A` dash handling).

Observed universe shape (597): suffix counts `.L 124, .DE 70, .PA 69, .SW 60, .ST 51, .MI 40,
.AS 35, .MC 28, .OL 23, .CO 23, .HE 19, .WA 18, .BR 17, .VI 9, .IR 6, .LS 5`; **zero bare tickers**;
currencies `EUR 298, GBP 122, CHF 60, SEK 51, NOK 23, DKK 23, PLN 18, USD 2`; ~43 dashed
B-share classes; exactly one dot (the suffix) per symbol — no multi-dot.

## 4. Build plan (TASK 2 — desktop monorepo session)
1. **config** (`collectors/price/config.yaml`): add **597 STOXX records** with
   `symbol` (suffixed, e.g. `SAP.DE`), `currency`, `quote_basis`, `family: stock`, `sector`.
   Generate from iShares EXSA (reuse dashboard Phase-0) — do not hand-list.
2. **`register_catalog.py` `entry()`**: ensure it carries `currency` + `quote_basis` through
   to the catalog entry for stock family (mirror P7a entry(); extend only for the two new fields).
3. **`fetch_prices.py`**: pass suffixed tickers; apply the decision-4 GBX policy (option a:
   keep raw, tag `quote_basis`); produce raw + adjusted (dual-basis) + `split_factor`.
4. **tests**: new `tests/test_stoxx.py` (or extend `tests/test_stock.py`) covering the
   verify gates in §5. Reuse `test_stock.py` fixtures where possible.
5. **Verify against a TEMP root** (never the real archive). Then ultracode adversarial on the gate.

## 5. Verify gates (mirror P7a + STOXX-specific)
- **identity**: every `px_<stoxx>` registered in the catalog **before** any write.
- **negative cardinal**: root unset / real / EMPTY → **REFUSED**; `ALLOW_REAL` set → push **REFUSED**.
- **survivorship**: `backtest_valid:false` machine-readable on **every** STOXX series.
- **no regression**: ETF (132) + SP500 (503) series unchanged.
- **collisions = 0**: suffix-retained series_id → **0 duplicate symbol/series_id across the
  1232-series union** (132 ETF + 503 SP500 + 597 STOXX). Verify programmatically.
- **currency + quote_basis present** on every STOXX series; **GBX is not ×100 wrong**
  (assert a known `.L`/GBP pence series carries `quote_basis: GBX` and raw magnitude).
- **dual-basis + split_factor**: test a suffixed name (`SAP.DE` / `MC.PA`) + a known EU split
  → `split_factor` ≠ 1 path exercised; raw vs adjusted differ correctly.
- **catalog-once**: catalog loaded once on the union (not N×).
- **untouched**: the operational 96-series data-core catalog is NOT modified.
- **temp-root only**: zero prices written to the real data-core.

## 6. Discipline
- **Strangler**: the real price-archive stays **READ-only** until sign-off; real register = P8.
- **Cardinal**: temp root only, zero prices in data-core.
- **Finalize**: log to HANDOFF + checkpoint + journal + memory `project_init22_p7a2_stoxx_citizen`
  (do NOT duplicate the already-done P7a fail-closed-family / empty-root-hygiene work).
- **Push** only own paths, **with approval**. ASCII commit messages.
- **Git gotcha** (from P7a): credential manager hangs in CC → push/fetch with the gh helper:
  `git -c credential.helper= -c credential.helper='!gh auth git-credential' push origin <branch>`
  (+ `pull --rebase --autostash` on CI auto-commit conflict).
