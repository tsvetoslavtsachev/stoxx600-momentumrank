"""
iShares EXSA (STOXX Europe 600) holdings: download + robust CSV parse.

Same file in both repos (STOXX600-rotationradar/src, stoxx600-momentumrank/).
Edit one, copy to the other.

History: from 2026-09-23 iShares serves a new site. The old endpoint
  .../1478358465952.ajax?fileType=csv&fileName=EXSA_holdings&dataType=fund
answers 200 text/html (the product page), so the header search found nothing.
The CSV now lives at .../latest-holdings.csv in the same column format.
From 2026-11-13 that link answers 404 too; the first URL is now the Swiss
professional site (English columns, '.' decimals, ’ thousands), whose values
_to_german_values() rewrites to the German form. Every successful fetch is
cross-checked against Xtrackers (same index, other provider); a divergence or
an Xtrackers outage is logged, never fatal (df.attrs["crosscheck"]).

parse_holdings_csv() returns a DataFrame with the canonical German column
names (Emittententicker, Name, Sektor, Anlageklasse, Gewichtung (%), Kurs,
Standort, Börse, Marktwährung), equities only. Headers are detected by a set of
known aliases (German and English site variants), so a renamed or quoted
header, a BOM, a different preamble or a ';' delimiter do not break it.
On failure it raises IsharesHoldingsError naming what was actually received.
"""
from __future__ import annotations

import csv
import io
import re
import time

import pandas as pd
import requests

_PRODUCT = "https://www.ishares.com/de/privatanleger/de/produkte/251931"

# Tried in order; the first response that parses wins.
ISHARES_URLS = (
    # Swiss professional site, English columns (verified by hand 22.11.2026,
    # holdings as of 19.11.2026, 601 equities). Values are normalised to the
    # German form below, so callers see the same strings as from the DE site.
    "https://www.ishares.com/ch/professionals/en/products/251931/"
    "ishares-stoxx-europe-600-ucits-etf-de-fund/1495092304805.ajax"
    "?fileType=csv&fileName=EXSA_holdings&dataType=fund",
    # DE site link from 10.10.2026; 404 since 13.11.2026.
    f"{_PRODUCT}/ishares-stoxx-europe-600-ucits-etf-de/latest-holdings.csv",
    # Legacy endpoint (before 2026-09-23). Kept as fallback in case it comes back.
    f"{_PRODUCT}/ishares-stoxx-europe-600-ucits-etf-de-fund/1478358465952.ajax"
    "?fileType=csv&fileName=EXSA_holdings&dataType=fund",
)
ISHARES_REFERER = f"{_PRODUCT}/ishares-stoxx-europe-600-ucits-etf-de-fund"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,text/plain,*/*",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Referer": ISHARES_REFERER,
}

# Canonical column -> known header spellings (compared after normalisation).
COLUMN_ALIASES = {
    "Emittententicker": ("Emittententicker", "Ticker", "Issuer Ticker", "Emittent Ticker"),
    "Name": ("Name", "Security Name", "Wertpapiername"),
    "Sektor": ("Sektor", "Sector"),
    "Anlageklasse": ("Anlageklasse", "Asset Class"),
    "Gewichtung (%)": ("Gewichtung (%)", "Gewichtung", "Weight (%)", "Weight"),
    "Kurs": ("Kurs", "Price"),
    "Standort": ("Standort", "Location", "Location of Risk", "Land"),
    "Börse": ("Börse", "Boerse", "Exchange"),
    "Marktwährung": ("Marktwährung", "Marktwaehrung", "Market Currency", "Währung", "Currency"),
}
REQUIRED = tuple(COLUMN_ALIASES)
EQUITY_CLASSES = {"aktien", "equity"}

# Below this many equity rows the file is truncated or not the EXSA holdings.
MIN_EQUITIES = 400

# English site values -> the German labels the DE site used (taken from the
# fixture exsa_holdings_2026-10-08.csv). Callers map the German labels.
SECTOR_EN_TO_DE = {
    "Energy": "Energie", "Financials": "Finanzwesen", "Health Care": "Gesundheitsversorgung",
    "Information Technology": "IT", "Real Estate": "Immobilien", "Industrials": "Industrie",
    "Communication": "Kommunikation", "Communication Services": "Kommunikation",
    "Materials": "Materialien", "Consumer Staples": "Nichtzyklische Konsumgüter",
    "Utilities": "Versorger", "Consumer Discretionary": "Zyklische Konsumgüter",
}
COUNTRY_EN_TO_DE = {
    "Belgium": "Belgien", "Germany": "Deutschland", "Denmark": "Dänemark", "Finland": "Finnland",
    "France": "Frankreich", "Greece": "Griechenland", "Ireland": "Irland", "Italy": "Italien",
    "Netherlands": "Niederlande", "Norway": "Norwegen", "Poland": "Polen", "Portugal": "Portugal",
    "Sweden": "Schweden", "Switzerland": "Schweiz", "Spain": "Spanien",
    "United Kingdom": "Vereinigtes Königreich", "Austria": "Österreich",
    "Luxembourg": "Luxemburg", "Czech Republic": "Tschechien", "Hungary": "Ungarn",
}
NUMERIC_COLUMNS = ("Gewichtung (%)", "Kurs")

# Second source for a cross-check (no tickers, so it cannot replace iShares).
XTRACKERS_URL = ("https://etf.deutsche-am.com/etfdata/export/GBR/ENG/excel/product/"
                 "constituent/LU0328475792/")


class IsharesHoldingsError(RuntimeError):
    """The iShares response is not a parseable holdings CSV."""


def _norm(s: str) -> str:
    s = str(s).replace("﻿", "").replace("\xa0", " ").strip().strip('"').strip()
    return re.sub(r"\s+", " ", s).casefold()


_ALIAS_TO_CANON = {_norm(a): canon for canon, aliases in COLUMN_ALIASES.items() for a in aliases}
_TICKER_ALIASES = {_norm(a) for a in COLUMN_ALIASES["Emittententicker"]}


def _describe(text: str, content_type: str | None = None) -> str:
    """Short human description of what was received, for error messages."""
    head = text.lstrip("﻿")[:2000]
    parts = [f"{len(text)} chars"]
    if content_type:
        parts.append(f"content-type={content_type!r}")
    if re.match(r"\s*<(!doctype|html)", head, re.I):
        title = re.search(r"<title>\s*([^<]{0,120})", head, re.I)
        parts.append("HTML page" + (f" titled {title.group(1).strip()!r}" if title else ""))
    first = next((ln for ln in head.splitlines() if ln.strip()), "")
    parts.append(f"first line {first[:160]!r}")
    return ", ".join(parts)


def decode_bytes(raw: bytes) -> str:
    """UTF-8 (with or without BOM); cp1252 if that fails. Not trusting the
    response charset: a missing one makes requests fall back to latin-1."""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


def _find_header(lines: list[str]) -> tuple[int, str]:
    """Index of the header row and its delimiter. Header = a row whose cells
    include a ticker alias and at least 4 other known columns."""
    for i, line in enumerate(lines[:60]):
        for delim in (",", ";", "\t"):
            if delim not in line:
                continue
            cells = {_norm(c) for c in next(csv.reader([line], delimiter=delim))}
            if cells & _TICKER_ALIASES and len(cells & set(_ALIAS_TO_CANON)) >= 5:
                return i, delim
    raise LookupError


def parse_holdings_csv(text: str, content_type: str | None = None,
                       min_equities: int = MIN_EQUITIES) -> pd.DataFrame:
    """Parse an iShares holdings CSV into canonical German columns, equities only."""
    lines = text.lstrip("﻿").splitlines()
    try:
        header_idx, delim = _find_header(lines)
    except LookupError:
        raise IsharesHoldingsError(
            "No holdings header (ticker column such as 'Emittententicker'/'Ticker') "
            f"in iShares response: {_describe(text, content_type)}"
        ) from None

    df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])), sep=delim,
                     quotechar='"', dtype=str, keep_default_na=False)
    df = df.rename(columns={c: _ALIAS_TO_CANON.get(_norm(c), c) for c in df.columns})
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise IsharesHoldingsError(
            f"iShares holdings CSV lacks columns {missing}; got {list(df.columns)}")

    for c in REQUIRED:
        df[c] = df[c].astype(str).str.replace("\xa0", " ", regex=False).str.strip()
    df = df[df["Anlageklasse"].str.casefold().isin(EQUITY_CLASSES)].copy()
    if len(df) < min_equities:
        raise IsharesHoldingsError(
            f"iShares holdings CSV has only {len(df)} equity rows (< {min_equities}); "
            f"{_describe(text, content_type)}")
    return _to_german_values(df).reset_index(drop=True)


def _decimal_mark(values) -> str:
    """',' (German site) or '.' (English/Swiss site), read from the weights:
    a weight like '3,75' or '3.75' always carries the decimal mark."""
    commas = sum(1 for v in values if re.fullmatch(r"\d+,\d+", v))
    dots = sum(1 for v in values if re.fullmatch(r"\d+\.\d+", v))
    return "," if commas >= dots else "."


def _de_number(s: str, mark: str) -> str:
    """Rewrite a number to the German form '1288,20' (no thousands mark)."""
    s = s.replace("’", "").replace("'", "").replace(" ", "")
    if mark == ".":
        s = s.replace(",", "")
    else:
        s = s.replace(".", "").replace(",", ".")
    try:
        float(s)
    except ValueError:
        return ""
    return s.replace(".", ",")


def _to_german_values(df: pd.DataFrame) -> pd.DataFrame:
    """English-site values -> German-site values (sector, country, numbers).
    German input passes through unchanged."""
    mark = _decimal_mark(df["Gewichtung (%)"])
    for c in NUMERIC_COLUMNS:
        df[c] = [_de_number(v, mark) for v in df[c]]
    df["Sektor"] = df["Sektor"].map(lambda v: SECTOR_EN_TO_DE.get(v, v))
    df["Standort"] = df["Standort"].map(lambda v: COUNTRY_EN_TO_DE.get(v, v))
    return df


_NAME_STOP = {"AG", "SA", "PLC", "NV", "N", "SE", "AB", "ASA", "SPA", "INC", "LTD", "HOLDING",
              "HOLDINGS", "GROUP", "CLASS", "A", "B", "REG", "THE", "DE", "OYJ", "AS", "CORP",
              "PAR", "PS", "ORD"}


def _name_key(s: str) -> str:
    words = [w for w in re.sub(r"[^A-Z0-9 ]", " ", str(s).upper()).split() if w not in _NAME_STOP]
    return " ".join(words[:2])


def crosscheck(ishares: pd.DataFrame, xtr: pd.DataFrame) -> dict:
    """Compare iShares EXSA with Xtrackers (same index) by name and weight.
    xtr: columns Name, Weighting (fraction), Type of Security."""
    x = xtr[xtr["Type of Security"].astype(str).str.casefold().eq("equities")].copy()
    x["w"] = pd.to_numeric(x["Weighting"], errors="coerce") * 100
    i = ishares.copy()
    i["w"] = pd.to_numeric(i["Gewichtung (%)"].str.replace(",", "."), errors="coerce")
    wi = i.groupby(i["Name"].map(_name_key))["w"].sum()
    wx = x.groupby(x["Name"].map(_name_key))["w"].sum()
    both = wi.index.intersection(wx.index)
    diff = (wi[both] - wx[both]).abs()
    res = {
        "n_ishares": int(len(i)), "n_xtrackers": int(len(x)),
        "matched_weight_pct": round(float(wi[both].sum()), 2),
        "weight_diff_p95": round(float(diff.quantile(0.95)), 4) if len(diff) else None,
        "weight_diff_max": round(float(diff.max()), 4) if len(diff) else None,
    }
    res["ok"] = bool(abs(res["n_ishares"] - res["n_xtrackers"]) <= 15
                     and res["matched_weight_pct"] >= 95
                     and (res["weight_diff_p95"] or 0) <= 0.05)
    return res


def crosscheck_xtrackers(ishares: pd.DataFrame, timeout: int = 30, log=print) -> dict | None:
    """Download Xtrackers constituents and cross-check. Never raises: the
    second source is a witness, not a gate; its outage must not stop the run."""
    try:
        resp = requests.get(XTRACKERS_URL, headers={"User-Agent": HTTP_HEADERS["User-Agent"]},
                            timeout=timeout)
        resp.raise_for_status()
        raw = pd.read_excel(io.BytesIO(resp.content), header=None)
        hdr = next(i for i in range(min(30, len(raw)))
                   if {"Name", "Weighting"} <= set(map(str, raw.iloc[i].tolist())))
        xtr = raw.iloc[hdr + 1:].set_axis([str(c) for c in raw.iloc[hdr]], axis=1)
        res = crosscheck(ishares, xtr)
    except Exception as e:  # noqa: BLE001 (witness only)
        log(f"  WARN Xtrackers cross-check unavailable: {type(e).__name__}: {e}")
        return None
    tag = "OK" if res["ok"] else "WARN"
    log(f"  {tag} Xtrackers cross-check: {res}")
    return res


def fetch_holdings(urls=ISHARES_URLS, attempts: int = 3, timeout: int = 30,
                   log=print) -> pd.DataFrame:
    """Download and parse EXSA holdings. Tries each URL (with retries on
    network errors); raises IsharesHoldingsError listing every outcome."""
    outcomes = []
    for url in urls:
        for attempt in range(1, attempts + 1):
            try:
                resp = requests.get(url, headers=HTTP_HEADERS, timeout=timeout)
                resp.raise_for_status()
            except requests.RequestException as e:
                outcomes.append(f"{url} attempt {attempt}: {e}")
                if attempt < attempts:
                    time.sleep(3 * attempt)
                continue
            text = decode_bytes(resp.content)
            ctype = resp.headers.get("content-type")
            log(f"  iShares: {len(text)} chars, status {resp.status_code}, "
                f"content-type {ctype!r} from {url}")
            try:
                df = parse_holdings_csv(text, ctype)
            except IsharesHoldingsError as e:
                outcomes.append(f"{url}: {e}")
                break  # a format problem will not fix itself on retry
            log(f"  iShares: parsed {len(df)} equity holdings")
            df.attrs["crosscheck"] = crosscheck_xtrackers(df, log=log)
            return df
    raise IsharesHoldingsError("Could not get EXSA holdings:\n  " + "\n  ".join(outcomes))
