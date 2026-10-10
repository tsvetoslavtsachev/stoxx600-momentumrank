"""Regression: iShares EXSA holdings CSV format (new site, from 2026-09-23).

The fixture is the real latest-holdings.csv downloaded 2026-10-10 (as of 08.Okt.2026).
"""
from pathlib import Path

import pytest

from ishares_holdings import IsharesHoldingsError, decode_bytes, parse_holdings_csv

FIXTURE = Path(__file__).parent / "fixtures" / "exsa_holdings_2026-10-08.csv"


def _sample() -> str:
    return decode_bytes(FIXTURE.read_bytes())


def test_new_format_parses_all_equities():
    df = parse_holdings_csv(_sample())
    assert len(df) == 602
    assert set(df["Anlageklasse"]) == {"Aktien"}
    assert df.loc[0, "Emittententicker"] == "ASML"
    assert df.loc[0, "Börse"] == "Euronext Amsterdam"
    assert "Vereinigtes Königreich" in set(df["Standort"])


def test_bom_quoted_header_and_semicolons():
    text = _sample()
    lines = text.splitlines()
    i = next(n for n, ln in enumerate(lines) if ln.startswith("Emittententicker"))
    lines[i] = ",".join(f'"{c}"' for c in lines[i].split(","))
    df = parse_holdings_csv("\ufeff" + "\n".join(lines))
    assert len(df) == 602
    semi = "\n".join(ln.replace('","', '";"') for ln in lines[i + 1:])
    df2 = parse_holdings_csv(lines[i].replace(",", ";") + "\n" + semi)
    assert len(df2) == 602


def test_english_headers_are_aliased():
    text = _sample()
    de = "Emittententicker,Name,Sektor,Anlageklasse,Marktwert,Gewichtung (%),Nominalwert,Nominale,Kurs,Standort,Börse,Marktwährung"
    en = "Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Shares,Price,Location,Exchange,Market Currency"
    assert de in text
    df = parse_holdings_csv(text.replace(de, en))
    assert len(df) == 602 and "Emittententicker" in df.columns


def test_html_page_gives_clear_error():
    html = "<!doctype html>\n<html><head><title>iShares STOXX Europe 600 UCITS ETF (DE) | EXSA | iShares</title>"
    with pytest.raises(IsharesHoldingsError, match=r"HTML page titled 'iShares STOXX Europe 600"):
        parse_holdings_csv(html, "text/html; charset=utf-8")


def test_truncated_file_is_rejected():
    head = "\n".join(_sample().splitlines()[:50])
    with pytest.raises(IsharesHoldingsError, match="only 47 equity rows"):
        parse_holdings_csv(head)


def test_cp1252_bytes_decode():
    assert decode_bytes("Börse".encode("cp1252")) == "Börse"
    assert decode_bytes("\ufeffBörse".encode("utf-8")) == "Börse"
