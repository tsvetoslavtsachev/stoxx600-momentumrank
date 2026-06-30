#!/usr/bin/env python3
"""CI guard (INIT-22 P9): fail RED if stoxx600-momentumrank prices silently fell back to yfinance.

Coverage-floor + allow-list (the P6-part-2 / macro-satellite cardinality pattern), adapted to this
repo's DYNAMIC iShares-EXSA universe with a known quarantine gap (ROG.SW). The catastrophe this
catches is a SILENT MASS fallback (e.g. the archive checkout failed -> the whole universe quietly
reverted to un-audited yfinance). A handful of non-allow-listed fetches (a newly-added EXSA
constituent not yet registered) are WARNED, not failed. Run only when the read PATs are set (the
workflow gates it); absent the secrets the yfinance fallback is legitimate and this is skipped.

NOTE currency: stoxx600-momentumrank sources normalize_currency=FALSE (raw GBX preserved); this
guard only checks PROVENANCE (base vs fetch), not units.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE = Path(__file__).resolve().parent / "price_source.json"

# Known, expected fetch fallback (NOT a silent regression): symbol whose px_* series is absent from
# the archive catalog. Documented in P8b-HON-case-to-resolve.md (KAZUS 2) + the P9 mandate.
ALLOWLIST = {"ROG.SW"}  # Roche -- P8c quarantine, Yahoo no-data

FLOOR_FRAC = 0.90  # >= 90% of the symbols sourced this run must be base-sourced


def main() -> int:
    if not SOURCE.exists():
        print(f"assert_base_sourced: {SOURCE} missing -- fetch_data.py must run first",
              file=sys.stderr)
        return 1
    payload = json.loads(SOURCE.read_text(encoding="utf-8"))
    by_symbol = payload.get("by_symbol", {})
    if not by_symbol:
        print("assert_base_sourced: price_source empty -- nothing sourced this run -- OK.")
        return 0

    base = sorted(t for t, s in by_symbol.items() if s == "base")
    fetch = sorted(t for t, s in by_symbol.items() if s != "base")
    covered = len(by_symbol)
    base_frac = len(base) / covered if covered else 0.0
    unexpected = [t for t in fetch if t not in ALLOWLIST]

    print(f"assert_base_sourced: {len(base)}/{covered} base ({base_frac:.1%}); "
          f"{len(fetch)} fetch ({len(unexpected)} unexpected, "
          f"{len(fetch) - len(unexpected)} allow-listed).")
    if unexpected:
        print("  WARNING -- fetched (not allow-listed; check archive registration / index churn):",
              file=sys.stderr)
        for t in unexpected:
            print(f"    - {t}", file=sys.stderr)

    if base_frac < FLOOR_FRAC:
        print(f"FAIL: base fraction {base_frac:.1%} < floor {FLOOR_FRAC:.0%} -- the strangler "
              "silently reverted to yfinance. Check the data-core/price-archive checkout + "
              "PYTHONPATH + DATACORE_ROOT, and any per-symbol archive gap.", file=sys.stderr)
        return 1

    print(f"OK: base sourcing intact ({base_frac:.1%} base; {len(unexpected)} tolerated churn/gap).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
