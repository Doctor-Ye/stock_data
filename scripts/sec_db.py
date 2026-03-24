#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import html
import http.client
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from socket import timeout as socket_timeout
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.json"
FALLBACK_SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.example.json"
UNIVERSE_CHECKPOINT_VERSION = 4
ANNUAL_FORMS = {"10-K", "10-K/A", "10-KT", "20-F", "20-F/A", "40-F", "40-F/A"}
QUARTERLY_FORMS = {"10-Q", "10-Q/A"}
GUIDANCE_FORMS = ANNUAL_FORMS | QUARTERLY_FORMS | {"8-K", "8-K/A", "6-K", "6-K/A"}
ADR_NAME_RE = re.compile(r"\badrs?\b|\bads\b|american depositary|depositary receipt", re.I)
NEGATIVE_EQUITY_NAME_RE = re.compile(
    r"\betf\b|\betn\b|\bexchange traded\b|\bmutual fund\b|\bfund\b|\btrust\b|"
    r"\bpreferred\b|\bpfd\b|\bwarrants?\b|\brights?\b|\bnotes?\b|\bdebentures?\b|"
    r"\bbeneficial interest\b|\bunits?\b",
    re.I,
)
POSITIVE_EQUITY_NAME_RE = re.compile(
    r"\bcommon stock\b|\bcommon shares?\b|\bordinary shares?\b|\bcapital stock\b|"
    r"\bsubordinate voting shares?\b|\bvoting shares?\b|\bclass [a-z] common\b|"
    r"\bclass [a-z] ordinary\b|\bclass [a-z] shares?\b",
    re.I,
)
EXCHANGE_CODE_MAP = {
    "N": "NYSE",
    "A": "NYSE American",
    "P": "NYSE Arca",
    "Q": "Nasdaq",
    "Z": "Cboe",
    "V": "IEX",
}
GUIDANCE_KEYWORDS = (
    "guidance",
    "outlook",
    "expect",
    "expects",
    "forecast",
    "projects",
    "projected",
    "anticipate",
    "anticipates",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def load_settings() -> dict[str, Any]:
    path = DEFAULT_SETTINGS_PATH if DEFAULT_SETTINGS_PATH.exists() else FALLBACK_SETTINGS_PATH
    settings = json.loads(path.read_text(encoding="utf-8"))
    data_root = (PROJECT_ROOT / settings.get("dataRoot", "./data")).resolve()
    settings["projectRoot"] = str(PROJECT_ROOT)
    settings["dataRootResolved"] = str(data_root)
    settings.setdefault("additionalCompaniesPath", str((PROJECT_ROOT / "config" / "additional_companies.json").resolve()))
    settings.setdefault("profitForecastsPath", str((PROJECT_ROOT / "config" / "earnings_forecasts.json").resolve()))
    settings.setdefault("nasdaq100Source", "https://en.wikipedia.org/wiki/Nasdaq-100")
    settings.setdefault("sqlitePath", str((data_root / "db" / "stock_sec.db").resolve()))
    settings.setdefault("formsToTrack", sorted(ANNUAL_FORMS | QUARTERLY_FORMS))
    settings.setdefault("lookbackYears", 10)
    settings.setdefault("marketDataBaseUrl", "https://stooq.com/q/l/")
    settings.setdefault("marketDataSource", "stooq")
    settings.setdefault("yahooChartBaseUrl", "https://query1.finance.yahoo.com/v8/finance/chart")
    settings.setdefault("nasdaqListedUrl", "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt")
    settings.setdefault("otherListedUrl", "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt")
    settings.setdefault("universeMinMarketCapUsd", 10_000_000_000)
    return settings


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def append_log(settings: dict[str, Any], message: str) -> None:
    logs_root = Path(settings["dataRootResolved"]) / "logs"
    ensure_dir(logs_root)
    line = f"[{utc_now_iso()}] {message}\n"
    with (logs_root / "pipeline.log").open("a", encoding="utf-8") as handle:
        handle.write(line)
    print(line.rstrip())


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def get_or_fetch_text(url: str, path: Path, settings: dict[str, Any], force: bool = False) -> str:
    if path.exists() and not force:
        return path.read_text(encoding="utf-8")
    payload = fetch_text(url, settings)
    ensure_dir(path.parent)
    path.write_text(payload, encoding="utf-8")
    time.sleep(0.2)
    return payload


def load_companies_snapshot(settings: dict[str, Any]) -> list[dict[str, Any]]:
    snapshot_path = Path(settings["dataRootResolved"]) / "db" / "companies.json"
    data = read_json(snapshot_path)
    companies: list[dict[str, Any]] = []
    for row in list(data or []):
        companies.append(
            {
                "ticker": row["ticker"],
                "cik": row["cik"],
                "name": row["name"],
                "security": row["security"],
                "sector": row["sector"],
                "subIndustry": row.get("subIndustry", row.get("sub_industry")),
                "headquarters": row["headquarters"],
                "dateAdded": row.get("dateAdded", row.get("date_added")),
                "listingExchange": row.get("listingExchange", row.get("listing_exchange")),
                "isAdr": bool(row.get("isAdr", row.get("is_adr"))),
                "universeSource": row.get("universeSource", row.get("universe_source")),
            }
        )
    return companies


def load_market_quotes_snapshot(settings: dict[str, Any]) -> dict[str, dict[str, Any]]:
    snapshot_path = Path(settings["dataRootResolved"]) / "db" / "market_quotes.json"
    rows = read_json(snapshot_path) or []
    return {row["ticker"]: row for row in rows if row.get("ticker")}


def normalize_ticker(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip().upper().replace(".", "-")


def cik10(value: Any) -> str:
    text = str(value).strip()
    if not text.isdigit():
        raise ValueError(f"Invalid CIK: {value}")
    return text.zfill(10)


def fetch_text(url: str, settings: dict[str, Any]) -> str:
    headers = {
        "User-Agent": settings["userAgent"],
        "Accept": "application/json, text/html, */*",
    }
    request = urllib.request.Request(url, headers=headers)
    attempts = 5
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code in {400, 401, 403, 404}:
                raise RuntimeError(f"Failed to fetch {url}: HTTP {exc.code}") from exc
            last_error = exc
            wait_seconds = min(30, attempt * 2)
            append_log(settings, f"HTTP retry {attempt}/{attempts} for {url}: {exc}")
            time.sleep(wait_seconds)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, socket_timeout, http.client.RemoteDisconnected, ConnectionResetError) as exc:
            last_error = exc
            wait_seconds = min(30, attempt * 2)
            append_log(settings, f"HTTP retry {attempt}/{attempts} for {url}: {exc}")
            time.sleep(wait_seconds)

    raise RuntimeError(f"Failed to fetch {url}") from last_error


def fetch_json(url: str, settings: dict[str, Any]) -> Any:
    return json.loads(fetch_text(url, settings))


def parse_sp500_constituents(html: str) -> list[dict[str, Any]]:
    table_match = re.search(r'<table[^>]*class="[^"]*wikitable[^"]*"[^>]*>(.*?)</table>', html, re.S)
    if not table_match:
        raise RuntimeError("Unable to locate S&P 500 constituent table.")

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_match.group(1), re.S)
    items: list[dict[str, Any]] = []
    for row in rows:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)
        if len(cells) < 8:
            continue

        cleaned = [html_unescape(re.sub(r"<.*?>", "", cell).strip()) for cell in cells]
        symbol = normalize_ticker(cleaned[0])
        if not symbol or symbol == "SYMBOL":
            continue

        items.append(
            {
                "ticker": symbol,
                "security": cleaned[1],
                "sector": cleaned[2],
                "subIndustry": cleaned[3],
                "headquarters": cleaned[4],
                "dateAdded": cleaned[5],
                "cikFromSource": cleaned[6],
            }
        )

    if len(items) < 400:
        raise RuntimeError(f"Parsed constituent list is unexpectedly small: {len(items)}")
    return items


def parse_nasdaq100_constituents(html: str) -> list[dict[str, Any]]:
    tables = re.findall(r'<table[^>]*class="[^"]*wikitable[^"]*"[^>]*>(.*?)</table>', html, re.S)
    for table_html in tables:
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.S)
        if not rows:
            continue
        header_cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", rows[0], re.S)
        headers = [html_unescape(re.sub(r"<.*?>", "", cell).strip()).lower() for cell in header_cells]
        if not headers:
            continue
        ticker_idx = next((i for i, header in enumerate(headers) if "ticker" in header or "symbol" in header), None)
        company_idx = next((i for i, header in enumerate(headers) if "company" in header), None)
        if ticker_idx is None or company_idx is None:
            continue

        items: list[dict[str, Any]] = []
        for row in rows[1:]:
            cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)
            if len(cells) <= max(ticker_idx, company_idx):
                continue
            cleaned = [html_unescape(re.sub(r"<.*?>", "", cell).strip()) for cell in cells]
            symbol = normalize_ticker(cleaned[ticker_idx])
            if not symbol or symbol in {"SYMBOL", "TICKER"}:
                continue
            items.append(
                {
                    "ticker": symbol,
                    "security": cleaned[company_idx],
                }
            )
        if len(items) >= 80:
            return items
    raise RuntimeError("Unable to locate Nasdaq-100 constituent table.")


def html_unescape(value: str) -> str:
    return html.unescape(value)


def get_sp500_constituents(settings: dict[str, Any]) -> list[dict[str, Any]]:
    return parse_sp500_constituents(fetch_text(settings["sp500Source"], settings))


def get_nasdaq100_constituents(settings: dict[str, Any]) -> list[dict[str, Any]]:
    return parse_nasdaq100_constituents(fetch_text(settings["nasdaq100Source"], settings))


def get_sec_ticker_map(settings: dict[str, Any]) -> list[dict[str, Any]]:
    payload = fetch_json(settings["secTickerMapUrl"], settings)
    items: list[dict[str, Any]] = []
    for row in payload.values():
        items.append(
            {
                "ticker": normalize_ticker(row["ticker"]),
                "title": row["title"],
                "cik": cik10(row["cik_str"]),
            }
        )
    return items


def get_additional_companies(settings: dict[str, Any]) -> list[dict[str, Any]]:
    path = Path(settings["additionalCompaniesPath"])
    payload = read_json(path)
    if not payload:
        return []

    items: list[dict[str, Any]] = []
    for row in payload:
        ticker = normalize_ticker(row.get("ticker"))
        if not ticker:
            continue
        items.append(
            {
                "ticker": ticker,
                "security": row.get("security") or ticker,
                "sector": row.get("sector"),
                "subIndustry": row.get("subIndustry"),
                "headquarters": row.get("headquarters"),
                "dateAdded": row.get("dateAdded"),
                "cikFromSource": row.get("cik"),
                "listingExchange": row.get("listingExchange"),
                "isAdr": bool(row.get("isAdr")),
                "universeSource": "manual",
            }
        )
    return items


def parse_symbol_directory(text: str, ticker_key: str, market_name: str, exchange_key: str | None = None) -> list[dict[str, Any]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    lines = [line for line in lines if not line.startswith("File Creation Time")]
    if not lines:
        return []

    reader = csv.DictReader(lines, delimiter="|")
    items: list[dict[str, Any]] = []
    for row in reader:
        ticker = normalize_ticker(row.get(ticker_key))
        security = (row.get("Security Name") or "").strip()
        if not ticker or not security:
            continue
        if (row.get("ETF") or "N").upper() != "N":
            continue
        if (row.get("Test Issue") or "N").upper() != "N":
            continue
        if (row.get("NextShares") or "N").upper() != "N":
            continue
        exchange_code = (row.get(exchange_key) or "").strip().upper() if exchange_key else "Q"
        items.append(
            {
                "ticker": ticker,
                "security": security,
                "listingExchange": EXCHANGE_CODE_MAP.get(exchange_code, market_name),
            }
        )
    return items


def is_adr_security_name(security_name: str | None) -> bool:
    if not security_name:
        return False
    return bool(ADR_NAME_RE.search(security_name))


def is_equity_security_name(security_name: str | None) -> bool:
    if not security_name:
        return False
    if NEGATIVE_EQUITY_NAME_RE.search(security_name):
        return False
    if is_adr_security_name(security_name):
        return True
    if POSITIVE_EQUITY_NAME_RE.search(security_name):
        return True
    return True


def get_us_listed_equities(settings: dict[str, Any], force: bool = False) -> list[dict[str, Any]]:
    reference_root = Path(settings["dataRootResolved"]) / "raw" / "reference"
    nasdaq_text = get_or_fetch_text(
        settings["nasdaqListedUrl"],
        reference_root / "nasdaqlisted.txt",
        settings,
        force=force,
    )
    other_text = get_or_fetch_text(
        settings["otherListedUrl"],
        reference_root / "otherlisted.txt",
        settings,
        force=force,
    )

    merged: dict[str, dict[str, Any]] = {}
    for row in parse_symbol_directory(nasdaq_text, "Symbol", "Nasdaq"):
        merged[row["ticker"]] = row
    for row in parse_symbol_directory(other_text, "ACT Symbol", "Other", exchange_key="Exchange"):
        merged.setdefault(row["ticker"], row)

    return [
        {
            **row,
            "isAdr": is_adr_security_name(row["security"]),
        }
        for row in merged.values()
        if is_equity_security_name(row["security"])
    ]


def latest_shares_outstanding(facts: dict[str, Any]) -> float | None:
    candidates: list[dict[str, Any]] = []
    for taxonomy, concept, unit in CONCEPT_MAP["SharesOutstanding"]["concepts"]:
        for entry in fact_entries(facts, taxonomy, concept, unit):
            form = (entry.get("form") or "").upper()
            if form and form not in ANNUAL_FORMS and form not in QUARTERLY_FORMS:
                continue
            if entry.get("frame"):
                continue
            value = entry.get("val")
            if value in (None, "", 0):
                continue
            candidates.append(entry)

    if not candidates:
        return None

    latest = sorted(
        candidates,
        key=lambda item: (item.get("filed") or "", item.get("end") or ""),
        reverse=True,
    )[0]
    try:
        return float(latest["val"])
    except (TypeError, ValueError, KeyError):
        return None


def build_company_seed(
    ticker: str,
    cik: str,
    name: str,
    security: str | None,
    sector: str | None = None,
    sub_industry: str | None = None,
    headquarters: str | None = None,
    date_added: str | None = None,
    listing_exchange: str | None = None,
    is_adr: bool = False,
    universe_source: str | None = None,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "cik": cik,
        "name": name,
        "security": security or name or ticker,
        "sector": sector,
        "subIndustry": sub_industry,
        "headquarters": headquarters,
        "dateAdded": date_added,
        "listingExchange": listing_exchange,
        "isAdr": is_adr,
        "universeSource": universe_source,
    }


def merge_company_rows(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key in ("name", "security", "sector", "subIndustry", "headquarters", "dateAdded", "listingExchange"):
        if incoming.get(key):
            merged[key] = incoming[key]
    merged["isAdr"] = bool(existing.get("isAdr")) or bool(incoming.get("isAdr"))

    sources = {
        item.strip()
        for item in str(existing.get("universeSource") or "").split(",")
        if item.strip()
    }
    sources.update(
        item.strip()
        for item in str(incoming.get("universeSource") or "").split(",")
        if item.strip()
    )
    merged["universeSource"] = ",".join(sorted(sources)) if sources else None
    return merged


def estimate_market_cap(
    company: dict[str, Any],
    settings: dict[str, Any],
    cached_quotes: dict[str, dict[str, Any]],
) -> float | None:
    quote = cached_quotes.get(company["ticker"])
    if not quote or quote.get("close") in (None, 0):
        quote = fetch_market_quote(company["ticker"], settings)
        if quote:
            cached_quotes[company["ticker"]] = quote
        time.sleep(0.1)
    if not quote or quote.get("close") in (None, 0):
        return None

    facts = get_company_facts(company, settings, force=False)
    shares = latest_shares_outstanding(facts)
    if shares in (None, 0):
        return None
    return float(quote["close"]) * float(shares)


def universe_priority_bucket(market_cap: float | None, is_adr: bool) -> int:
    if market_cap is not None:
        if market_cap > 50_000_000_000:
            return 0
        if market_cap > 20_000_000_000:
            return 1
        if market_cap > 15_000_000_000:
            return 2
        if market_cap > 10_000_000_000:
            return 3
    if is_adr:
        return 4
    return 5


def universe_checkpoint_path(settings: dict[str, Any]) -> Path:
    return Path(settings["dataRootResolved"]) / "db" / "universe_screen_checkpoint.json"


def load_universe_checkpoint(settings: dict[str, Any], force: bool = False) -> dict[str, Any]:
    if force:
        return {"completed": False, "lastTicker": None, "total": 0, "accepted": []}
    payload = read_json(universe_checkpoint_path(settings)) or {}
    if int(payload.get("version") or 0) != UNIVERSE_CHECKPOINT_VERSION:
        return {"completed": False, "lastTicker": None, "total": 0, "accepted": []}
    return {
        "completed": bool(payload.get("completed")),
        "lastTicker": normalize_ticker(payload.get("lastTicker")),
        "total": int(payload.get("total") or 0),
        "accepted": list(payload.get("accepted") or []),
    }


def save_universe_checkpoint(settings: dict[str, Any], total: int, accepted: list[dict[str, Any]], last_ticker: str | None, completed: bool) -> None:
    write_json(
        universe_checkpoint_path(settings),
        {
            "version": UNIVERSE_CHECKPOINT_VERSION,
            "updatedAtUtc": utc_now_iso(),
            "total": total,
            "lastTicker": last_ticker,
            "completed": completed,
            "accepted": accepted,
        },
    )


def load_staged_universe_from_checkpoint(settings: dict[str, Any]) -> list[dict[str, Any]]:
    checkpoint = load_universe_checkpoint(settings, force=False)
    accepted = sorted(
        checkpoint["accepted"],
        key=lambda item: (
            universe_priority_bucket(item.get("screenedMarketCap"), bool(item.get("isAdr"))),
            -(item.get("screenedMarketCap") or 0),
            item["ticker"],
        ),
    )
    companies: list[dict[str, Any]] = []
    for row in accepted:
        companies.append(
            build_company_seed(
                ticker=row["ticker"],
                cik=row["cik"],
                name=row["name"],
                security=row.get("security"),
                sector=row.get("sector"),
                sub_industry=row.get("subIndustry"),
                headquarters=row.get("headquarters"),
                date_added=row.get("dateAdded"),
                listing_exchange=row.get("listingExchange"),
                is_adr=bool(row.get("isAdr")),
                universe_source=row.get("universeSource"),
            )
        )
    return companies


def persist_staged_universe_checkpoint(
    settings: dict[str, Any],
    companies: list[dict[str, Any]],
    *,
    build_web: bool = False,
) -> None:
    if not companies:
        return
    db = Database(Path(settings["sqlitePath"]))
    with db.connect() as conn:
        upsert_companies(conn, companies)
        set_state(conn, "last_universe_stage_utc", utc_now_iso())
        export_json_snapshots(conn, settings)
        conn.commit()
    if build_web:
        build_web_data(settings)


def get_expanded_universe_candidates(settings: dict[str, Any], force: bool = False) -> list[dict[str, Any]]:
    ticker_map = {item["ticker"]: item for item in get_sec_ticker_map(settings)}
    cached_quotes = load_market_quotes_snapshot(settings)
    min_market_cap = float(settings["universeMinMarketCapUsd"])
    listings = get_us_listed_equities(settings, force=force)
    total = len(listings)
    checkpoint = load_universe_checkpoint(settings, force=force)
    rows: list[dict[str, Any]] = list(checkpoint["accepted"])

    if checkpoint["completed"] and checkpoint["total"] == total:
        append_log(settings, f"Universe screen cache hit: {len(rows)} accepted candidates")
        return rows

    start_index = 0
    if checkpoint["lastTicker"]:
        for index, listing in enumerate(listings):
            if listing["ticker"] == checkpoint["lastTicker"]:
                start_index = index + 1
                break
        if start_index:
            append_log(settings, f"Universe screen resuming after {checkpoint['lastTicker']} [{start_index}/{total}]")

    for index in range(start_index, total):
        listing = listings[index]
        ticker = listing["ticker"]
        match = ticker_map.get(ticker)
        if not match and "-" in ticker:
            match = ticker_map.get(ticker.replace("-", "."))
        if not match:
            save_universe_checkpoint(settings, total, rows, ticker, False)
            continue

        company = build_company_seed(
            ticker=ticker,
            cik=match["cik"],
            name=match["title"],
            security=listing["security"],
            listing_exchange=listing.get("listingExchange"),
            is_adr=bool(listing.get("isAdr")),
            universe_source="market-cap",
        )

        if (index + 1) % 250 == 0:
            append_log(settings, f"Universe screen [{index + 1}/{total}] {ticker}")

        try:
            market_cap = estimate_market_cap(company, settings, cached_quotes)
        except Exception as exc:
            append_log(settings, f"Universe screen failed for {ticker}: {exc}")
            save_universe_checkpoint(settings, total, rows, ticker, False)
            continue

        if market_cap is not None and market_cap > min_market_cap:
            company["screenedMarketCap"] = market_cap
            rows.append(company)
        save_universe_checkpoint(settings, total, rows, ticker, False)

    rows.sort(
        key=lambda item: (
            universe_priority_bucket(item.get("screenedMarketCap"), bool(item.get("isAdr"))),
            -(item.get("screenedMarketCap") or 0),
            item["ticker"],
        )
    )
    save_universe_checkpoint(settings, total, rows, listings[-1]["ticker"] if listings else None, True)
    return rows


def resolve_companies(settings: dict[str, Any], force: bool = False) -> list[dict[str, Any]]:
    data_root = Path(settings["dataRootResolved"])
    db_root = data_root / "db"
    ticker_map = {item["ticker"]: item for item in get_sec_ticker_map(settings)}
    company_map: dict[str, dict[str, Any]] = {}

    for row in get_sp500_constituents(settings):
        match = ticker_map.get(row["ticker"])
        if not match and "-" in row["ticker"]:
            match = ticker_map.get(row["ticker"].replace("-", "."))
        if not match:
            continue
        seed = build_company_seed(
            ticker=row["ticker"],
            cik=match["cik"],
            name=match["title"],
            security=row["security"],
            sector=row["sector"],
            sub_industry=row["subIndustry"],
            headquarters=row["headquarters"],
            date_added=row["dateAdded"],
            universe_source="sp500",
        )
        company_map[row["ticker"]] = merge_company_rows(company_map.get(row["ticker"], seed), seed) if row["ticker"] in company_map else seed

    for row in get_nasdaq100_constituents(settings):
        match = ticker_map.get(row["ticker"])
        if not match and "-" in row["ticker"]:
            match = ticker_map.get(row["ticker"].replace("-", "."))
        if not match:
            continue
        seed = build_company_seed(
            ticker=row["ticker"],
            cik=match["cik"],
            name=match["title"],
            security=row["security"],
            universe_source="nasdaq100",
        )
        company_map[row["ticker"]] = merge_company_rows(company_map.get(row["ticker"], seed), seed) if row["ticker"] in company_map else seed

    for row in get_expanded_universe_candidates(settings, force=force):
        company_map[row["ticker"]] = merge_company_rows(company_map[row["ticker"]], row) if row["ticker"] in company_map else row

    for row in get_additional_companies(settings):
        match = ticker_map.get(row["ticker"])
        if not match and "-" in row["ticker"]:
            match = ticker_map.get(row["ticker"].replace("-", "."))

        resolved_cik = match["cik"] if match else (cik10(row["cikFromSource"]) if str(row["cikFromSource"]).isdigit() else None)
        if not resolved_cik:
            continue

        seed = build_company_seed(
            ticker=row["ticker"],
            cik=resolved_cik,
            name=match["title"] if match else (row["security"] or row["ticker"]),
            security=row["security"],
            sector=row["sector"],
            sub_industry=row["subIndustry"],
            headquarters=row["headquarters"],
            date_added=row["dateAdded"],
            listing_exchange=row.get("listingExchange"),
            is_adr=bool(row.get("isAdr")),
            universe_source=row.get("universeSource") or "manual",
        )
        company_map[row["ticker"]] = merge_company_rows(company_map[row["ticker"]], seed) if row["ticker"] in company_map else seed

    companies = sorted(company_map.values(), key=lambda item: item["ticker"])
    write_json(db_root / "companies.json", companies)
    write_json(db_root / "sp500_constituents.json", [row for row in companies if "sp500" in str(row.get("universeSource") or "").split(",")])
    write_json(db_root / "nasdaq100_constituents.json", [row for row in companies if "nasdaq100" in str(row.get("universeSource") or "").split(",")])
    return companies


def company_root(settings: dict[str, Any], cik: str) -> Path:
    return Path(settings["dataRootResolved"]) / "raw" / "sec" / "companies" / cik


def get_or_fetch_json(url: str, path: Path, settings: dict[str, Any], force: bool = False) -> Any:
    if path.exists() and not force:
        return read_json(path)
    payload = fetch_json(url, settings)
    write_json(path, payload)
    time.sleep(0.2)
    return payload


def get_or_fetch_filing_text(url: str, path: Path, settings: dict[str, Any], force: bool = False) -> str:
    if path.exists() and not force:
        return path.read_text(encoding="utf-8", errors="replace")
    payload = fetch_text(url, settings)
    ensure_dir(path.parent)
    path.write_text(payload, encoding="utf-8")
    time.sleep(0.2)
    return payload


def filing_document_root(settings: dict[str, Any], cik: str) -> Path:
    return company_root(settings, cik) / "filings"


def sec_archive_document_url(cik: str, accession_number: str, primary_document: str) -> str:
    cik_int = str(int(cik))
    accession_compact = accession_number.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_compact}/{primary_document}"


def filing_document_cache_path(settings: dict[str, Any], cik: str, accession_number: str, primary_document: str | None) -> Path:
    safe_name = primary_document or "primary_document.html"
    return filing_document_root(settings, cik) / accession_number / safe_name


def extract_guidance_exhibit_links(raw_html: str) -> list[str]:
    rows = re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", raw_html)
    links: list[str] = []
    for row in rows:
        row_text = html_to_text(row).lower()
        if "99.1" not in row_text and "99.2" not in row_text and "earnings release" not in row_text and "outlook" not in row_text:
            continue
        for href in re.findall(r'(?i)href="([^"]+)"', row):
            if href.startswith("#") or href.lower().startswith("javascript:"):
                continue
            links.append(href)
    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append(link)
    return deduped[:4]


def html_to_text(payload: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", payload)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"(?i)</div\s*>", "\n", text)
    text = re.sub(r"(?i)</tr\s*>", "\n", text)
    text = re.sub(r"(?is)<.*?>", " ", text)
    text = html_unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def normalize_sentence_text(text: str) -> str:
    cleaned = text.replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def normalize_fiscal_year_token(raw_year: str) -> int | None:
    try:
        year = int(raw_year)
    except ValueError:
        return None
    if year < 100:
        year += 2000
    return year if year >= 2000 else None


def sentence_contains_guidance(text: str) -> bool:
    lower = text.lower()
    return any(keyword in lower for keyword in GUIDANCE_KEYWORDS)


def split_sentences(text: str) -> list[str]:
    normalized = normalize_sentence_text(text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[\.\?\!;])\s+", normalized)
    return [part.strip() for part in parts if part.strip()]


def iter_guidance_table_windows(text: str) -> list[tuple[int, str]]:
    windows: list[tuple[int, str]] = []
    seen: set[tuple[int, int]] = set()
    for match in re.finditer(r"\bQ([1-4])\s+FY\s*(\d{2,4})\s+Outlook\b", text, re.I):
        fiscal_year = normalize_fiscal_year_token(match.group(2))
        if fiscal_year is None:
            continue
        key = (fiscal_year, match.start())
        if key in seen:
            continue
        seen.add(key)
        windows.append((fiscal_year, text[match.start() : min(match.start() + 4000, len(text))]))
    for match in re.finditer(r"\bFQ([1-4])[- ](\d{2,4})\b", text, re.I):
        fiscal_year = normalize_fiscal_year_token(match.group(2))
        if fiscal_year is None:
            continue
        outlook_probe = text[match.start() : min(match.start() + 160, len(text))]
        if "outlook" not in outlook_probe.lower():
            continue
        key = (fiscal_year, match.start())
        if key in seen:
            continue
        seen.add(key)
        windows.append((fiscal_year, text[match.start() : min(match.start() + 2500, len(text))]))
    return windows


def extract_revenue_outlook_value(text: str) -> float | None:
    patterns = [
        r"Revenue\s+\$?\s*([\d,]+(?:\.\d+)?)\s*(billion|million|thousand)(?:\s*(?:±|\+/-|plus or minus)\s*\$?\s*[\d,]+(?:\.\d+)?\s*(?:billion|million|thousand|%))?",
        r"Revenue\s+\$?\s*([\d,]+(?:\.\d+)?)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if not match:
            continue
        magnitude = match.group(2) if match.lastindex and match.lastindex >= 2 else None
        amount = parse_money_amount(match.group(1), magnitude)
        if amount is not None and amount > 0:
            if magnitude is None and amount < 1_000_000:
                amount *= 1_000_000
            return round(amount, 2)
    return None


def extract_table_guidance_forecasts(raw_html: str) -> list[tuple[str, int, float, str]]:
    text = normalize_sentence_text(html_to_text(raw_html))
    matches: list[tuple[str, int, float, str]] = []
    for fiscal_year, window in iter_guidance_table_windows(text):
        revenue_value = extract_revenue_outlook_value(window)
        if revenue_value is not None:
            matches.append(("revenue", fiscal_year, revenue_value, "outlook-table"))

        eps_match = re.search(
            r"(?:diluted\s+)?(?:earnings\s+per\s+share|eps)(?:\s*\(\d+\))?.{0,24}?\$\s*([\d,]+(?:\.\d+)?)",
            window,
            re.I,
        )
        if eps_match:
            eps_value = parse_eps_amount(eps_match.group(1))
            if eps_value is not None and eps_value > 0:
                matches.append(("eps", fiscal_year, eps_value, "outlook-table"))
    return matches


def merge_submission_arrays(submission: dict[str, Any]) -> list[dict[str, Any]]:
    recent = submission.get("filings", {}).get("recent")
    if recent is None:
        recent = submission
    accessions = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    acceptance_times = recent.get("acceptanceDateTime", [])
    forms = recent.get("form", [])
    primary_documents = recent.get("primaryDocument", [])
    descriptions = recent.get("primaryDocDescription", [])
    items: list[dict[str, Any]] = []
    for idx, accession in enumerate(accessions):
        items.append(
            {
                "accessionNumber": accession,
                "filingDate": filing_dates[idx] if idx < len(filing_dates) else None,
                "reportDate": report_dates[idx] if idx < len(report_dates) else None,
                "acceptanceDateTime": acceptance_times[idx] if idx < len(acceptance_times) else None,
                "form": forms[idx] if idx < len(forms) else None,
                "primaryDocument": primary_documents[idx] if idx < len(primary_documents) else None,
                "description": descriptions[idx] if idx < len(descriptions) else None,
            }
        )
    return items


def get_all_submissions(
    company: dict[str, Any],
    settings: dict[str, Any],
    force: bool = False,
    tracked_forms: set[str] | None = None,
) -> list[dict[str, Any]]:
    root = company_root(settings, company["cik"]) / "submissions"
    ensure_dir(root)
    primary_path = root / f"CIK{company['cik']}.json"
    primary_url = f"{settings['secSubmissionsBaseUrl']}/CIK{company['cik']}.json"
    primary = get_or_fetch_json(primary_url, primary_path, settings, force=force)

    rows = merge_submission_arrays(primary)
    for file_meta in primary.get("filings", {}).get("files", []):
        name = file_meta.get("name")
        if not name:
            continue
        history_path = root / name
        history_url = f"{settings['secSubmissionsBaseUrl']}/{name}"
        history = get_or_fetch_json(history_url, history_path, settings, force=force)
        rows.extend(merge_submission_arrays(history))

    seen: set[str] = set()
    filtered: list[dict[str, Any]] = []
    tracked_forms = tracked_forms or set(settings["formsToTrack"])
    for item in sorted(rows, key=lambda row: (row.get("filingDate") or "", row.get("accessionNumber") or "")):
        accession = item.get("accessionNumber")
        if not accession or accession in seen:
            continue
        seen.add(accession)
        if item.get("form") not in tracked_forms:
            continue
        filtered.append(
            {
                "cik": company["cik"],
                "ticker": company["ticker"],
                "companyName": company["name"],
                "accessionNumber": accession,
                "filingDate": item.get("filingDate"),
                "reportDate": item.get("reportDate"),
                "acceptanceDateTime": item.get("acceptanceDateTime"),
                "form": item.get("form"),
                "primaryDocument": item.get("primaryDocument"),
                "description": item.get("description"),
            }
        )
    return filtered


def get_company_facts(company: dict[str, Any], settings: dict[str, Any], force: bool = False) -> dict[str, Any]:
    root = company_root(settings, company["cik"])
    ensure_dir(root)
    path = root / "companyfacts.json"
    url = f"{settings['secCompanyFactsBaseUrl']}/CIK{company['cik']}.json"
    return get_or_fetch_json(url, path, settings, force=force)


def market_symbol(ticker: str) -> str:
    return f"{ticker.lower()}.us"


def fetch_yahoo_market_quote(ticker: str, settings: dict[str, Any]) -> dict[str, Any] | None:
    source_url = f"{settings['yahooChartBaseUrl']}/{ticker}?interval=1d&range=5d"
    request = urllib.request.Request(
        source_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))

    result = (((payload or {}).get("chart") or {}).get("result") or [None])[0]
    if not result:
        return None
    timestamps = result.get("timestamp") or []
    indicators = (((result.get("indicators") or {}).get("quote")) or [None])[0] or {}
    opens = indicators.get("open") or []
    highs = indicators.get("high") or []
    lows = indicators.get("low") or []
    closes = indicators.get("close") or []
    volumes = indicators.get("volume") or []

    last_index = None
    for index in range(len(timestamps) - 1, -1, -1):
        if index < len(closes) and closes[index] is not None:
            last_index = index
            break
    if last_index is None:
        return None

    observed = datetime.fromtimestamp(int(timestamps[last_index]), tz=timezone.utc)
    return {
        "ticker": ticker,
        "quoteDate": observed.strftime("%Y-%m-%d"),
        "quoteTime": observed.strftime("%H:%M:%S"),
        "open": round_or_none(opens[last_index] if last_index < len(opens) else None, 2),
        "high": round_or_none(highs[last_index] if last_index < len(highs) else None, 2),
        "low": round_or_none(lows[last_index] if last_index < len(lows) else None, 2),
        "close": round_or_none(closes[last_index], 2),
        "volume": round_or_none(volumes[last_index] if last_index < len(volumes) else None, 0),
        "source": "yahoo",
        "sourceUrl": source_url,
        "fetchedAtUtc": utc_now_iso(),
    }


def fetch_market_quote(ticker: str, settings: dict[str, Any]) -> dict[str, Any] | None:
    source_url = f"{settings['marketDataBaseUrl']}?s={market_symbol(ticker)}&i=d"
    request = urllib.request.Request(
        source_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read().decode("utf-8", errors="replace").strip()

    if raw.startswith("Exceeded the daily hits limit"):
        return fetch_yahoo_market_quote(ticker, settings)
    if not raw or raw.endswith("N/D,N/D,N/D,N/D,N/D,N/D,N/D,N/D"):
        return None

    parts = [item.strip() for item in raw.split(",")]
    if len(parts) < 8:
        return fetch_yahoo_market_quote(ticker, settings)

    def parse_float(value: str) -> float | None:
        return None if value in {"", "N/D"} else float(value)

    return {
        "ticker": ticker,
        "quoteDate": None if parts[1] in {"", "N/D"} else f"{parts[1][0:4]}-{parts[1][4:6]}-{parts[1][6:8]}",
        "quoteTime": None if parts[2] in {"", "N/D"} else f"{parts[2][0:2]}:{parts[2][2:4]}:{parts[2][4:6]}",
        "open": parse_float(parts[3]),
        "high": parse_float(parts[4]),
        "low": parse_float(parts[5]),
        "close": parse_float(parts[6]),
        "volume": parse_float(parts[7]),
        "source": settings["marketDataSource"],
        "sourceUrl": source_url,
        "fetchedAtUtc": utc_now_iso(),
    }


def sync_market_data(conn: sqlite3.Connection, settings: dict[str, Any], companies: list[dict[str, Any]], force: bool = False) -> int:
    existing_rows = {
        row["ticker"]: dict(row)
        for row in conn.execute("SELECT * FROM market_quotes")
    }
    quotes: list[dict[str, Any]] = []
    total = len(companies)
    for index, company in enumerate(companies, start=1):
        existing = existing_rows.get(company["ticker"])
        if existing and existing.get("quote_date") and not force:
            if existing["quote_date"] == datetime.now(timezone.utc).strftime("%Y-%m-%d"):
                continue
        try:
            append_log(settings, f"Market data [{index}/{total}] {company['ticker']}")
            quote = fetch_market_quote(company["ticker"], settings)
            if quote:
                quotes.append(quote)
            time.sleep(0.15)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, socket_timeout, http.client.RemoteDisconnected, ConnectionResetError) as exc:
            append_log(settings, f"Market data fetch failed for {company['ticker']}: {exc}")
        except Exception as exc:
            append_log(settings, f"Market data parse failed for {company['ticker']}: {exc}")
    upsert_market_quotes(conn, quotes)
    return len(quotes)


CONCEPT_MAP: dict[str, dict[str, Any]] = {
    "Revenue": {
        "periodType": "duration",
        "concepts": [
            ("us-gaap", "Revenues", "USD"),
            ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax", "USD"),
            ("us-gaap", "SalesRevenueNet", "USD"),
        ],
    },
    "NetIncome": {
        "periodType": "duration",
        "concepts": [("us-gaap", "NetIncomeLoss", "USD")],
    },
    "OperatingIncome": {
        "periodType": "duration",
        "concepts": [("us-gaap", "OperatingIncomeLoss", "USD")],
    },
    "TotalAssets": {
        "periodType": "instant",
        "concepts": [("us-gaap", "Assets", "USD")],
    },
    "TotalLiabilities": {
        "periodType": "instant",
        "concepts": [("us-gaap", "Liabilities", "USD")],
    },
    "ShareholdersEquity": {
        "periodType": "instant",
        "concepts": [
            ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "USD"),
            ("us-gaap", "StockholdersEquity", "USD"),
        ],
    },
    "CashAndEquivalents": {
        "periodType": "instant",
        "concepts": [("us-gaap", "CashAndCashEquivalentsAtCarryingValue", "USD")],
    },
    "OperatingCashFlow": {
        "periodType": "duration",
        "concepts": [("us-gaap", "NetCashProvidedByUsedInOperatingActivities", "USD")],
    },
    "CapitalExpenditure": {
        "periodType": "duration",
        "concepts": [("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment", "USD")],
    },
    "ShareBasedCompensationExpense": {
        "periodType": "duration",
        "concepts": [
            ("us-gaap", "ShareBasedCompensation", "USD"),
            ("us-gaap", "AllocatedShareBasedCompensationExpense", "USD"),
        ],
    },
    "SpecialItems": {
        "periodType": "duration",
        "concepts": [
            ("us-gaap", "RestructuringCharges", "USD"),
            ("us-gaap", "BusinessExitCosts", "USD"),
            ("us-gaap", "AssetImpairmentCharges", "USD"),
            ("us-gaap", "ImpairmentLoss", "USD"),
            ("us-gaap", "GoodwillImpairmentLoss", "USD"),
            ("us-gaap", "GainLossOnSaleOfBusiness", "USD"),
            ("us-gaap", "GainLossOnSaleOfOtherAssets", "USD"),
            ("us-gaap", "GainLossOnDispositionOfAssets", "USD"),
            ("us-gaap", "GainLossOnSaleOfInvestments", "USD"),
            ("us-gaap", "GainLossRelatedToLitigationSettlement", "USD"),
        ],
    },
    "DilutedEPS": {
        "periodType": "duration",
        "concepts": [("us-gaap", "EarningsPerShareDiluted", "USD/shares")],
    },
    "SharesOutstanding": {
        "periodType": "instant",
        "concepts": [
            ("dei", "EntityCommonStockSharesOutstanding", "shares"),
            ("us-gaap", "CommonStockSharesOutstanding", "shares"),
        ],
    },
}


def fact_entries(facts: dict[str, Any], taxonomy: str, concept: str, unit: str) -> list[dict[str, Any]]:
    taxonomy_node = facts.get("facts", {}).get(taxonomy, {})
    concept_node = taxonomy_node.get(concept, {})
    return list(concept_node.get("units", {}).get(unit, []) or [])


def sum_fact_entries(facts: dict[str, Any], concepts: list[tuple[str, str, str]], selector) -> dict[str, Any] | None:
    total = 0.0
    found = False
    filed: str | None = None
    form: str | None = None
    for taxonomy, concept, unit in concepts:
        selected = selector(fact_entries(facts, taxonomy, concept, unit))
        if not selected:
            continue
        value = selected.get("val")
        if value is None:
            continue
        total += float(value)
        filed = max(filter(None, [filed, selected.get("filed")])) if filed else selected.get("filed")
        form = form or selected.get("form")
        found = True
    if not found:
        return None
    return {"val": total, "filed": filed, "form": form}


def duration_days(entry: dict[str, Any]) -> int | None:
    start = entry.get("start")
    end = entry.get("end")
    if not start or not end:
        return None
    try:
        return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).days
    except ValueError:
        return None


def is_annual_duration(entry: dict[str, Any]) -> bool:
    days = duration_days(entry)
    return days is not None and 300 <= days <= 380


def is_quarterly_duration(entry: dict[str, Any]) -> bool:
    days = duration_days(entry)
    return days is not None and 70 <= days <= 110


def select_annual_fact(entries: Iterable[dict[str, Any]], fiscal_year: int, period_type: str) -> dict[str, Any] | None:
    entries = list(entries)
    candidates = []
    for entry in entries:
        if entry.get("fy") != fiscal_year or entry.get("form") not in ANNUAL_FORMS or entry.get("frame"):
            continue
        if period_type == "duration" and not is_annual_duration(entry):
            continue
        candidates.append(entry)
    if not candidates:
        for entry in entries:
            if entry.get("fy") != fiscal_year or entry.get("form") not in ANNUAL_FORMS:
                continue
            if period_type == "duration" and not is_annual_duration(entry):
                continue
            candidates.append(entry)
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: ((item.get("filed") or ""), (item.get("end") or "")), reverse=True)[0]


def select_annual_fact_sum(facts: dict[str, Any], concepts: list[tuple[str, str, str]], fiscal_year: int, period_type: str) -> dict[str, Any] | None:
    def selector(entries: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
        return select_annual_fact(entries, fiscal_year, period_type)

    return sum_fact_entries(facts, concepts, selector)


def select_quarterly_fact(entries: Iterable[dict[str, Any]], fiscal_year: int, period_type: str) -> dict[str, Any] | None:
    entries = list(entries)
    candidates = []
    for entry in entries:
        if entry.get("fy") != fiscal_year or entry.get("form") not in QUARTERLY_FORMS or entry.get("frame"):
            continue
        if period_type == "duration" and not is_quarterly_duration(entry):
            continue
        candidates.append(entry)
    if not candidates:
        for entry in entries:
            if entry.get("fy") != fiscal_year or entry.get("form") not in QUARTERLY_FORMS:
                continue
            if period_type == "duration" and not is_quarterly_duration(entry):
                continue
            candidates.append(entry)
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: (item.get("filed") or "", item.get("end") or ""), reverse=True)[0]


def select_quarterly_fact_sum(
    facts: dict[str, Any],
    concepts: list[tuple[str, str, str]],
    fiscal_year: int,
    fiscal_period: str,
    period_type: str,
) -> dict[str, Any] | None:
    def selector(entries: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
        entries = list(entries)
        candidates = []
        for entry in entries:
            if entry.get("fy") != fiscal_year or entry.get("fp") != fiscal_period or entry.get("form") not in QUARTERLY_FORMS:
                continue
            if entry.get("frame"):
                continue
            if period_type == "duration" and not is_quarterly_duration(entry):
                continue
            candidates.append(entry)
        if not candidates:
            return None
        return sorted(candidates, key=lambda item: (item.get("filed") or "", item.get("end") or ""), reverse=True)[0]

    return sum_fact_entries(facts, concepts, selector)


def build_annual_financials(company: dict[str, Any], facts: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    start_year = datetime.now(timezone.utc).year - int(settings["lookbackYears"]) + 1
    end_year = datetime.now(timezone.utc).year
    rows: list[dict[str, Any]] = []

    for fiscal_year in range(start_year, end_year + 1):
        row: dict[str, Any] = {
            "cik": company["cik"],
            "ticker": company["ticker"],
            "companyName": company["name"],
            "fiscalYear": fiscal_year,
            "Revenue": None,
            "NetIncome": None,
            "OperatingIncome": None,
            "TotalAssets": None,
            "TotalLiabilities": None,
            "ShareholdersEquity": None,
            "CashAndEquivalents": None,
            "OperatingCashFlow": None,
            "CapitalExpenditure": None,
            "ShareBasedCompensationExpense": None,
            "SpecialItems": None,
            "FreeCashFlow": None,
            "DilutedEPS": None,
            "SharesOutstanding": None,
            "sourceFiledDate": None,
            "sourceForm": None,
        }

        for metric_name, concepts in CONCEPT_MAP.items():
            selected = None
            if metric_name == "SpecialItems":
                selected = select_annual_fact_sum(facts, concepts["concepts"], fiscal_year, concepts["periodType"])
            else:
                for taxonomy, concept, unit in concepts["concepts"]:
                    selected = select_annual_fact(fact_entries(facts, taxonomy, concept, unit), fiscal_year, concepts["periodType"])
                    if selected:
                        break
            if selected:
                row[metric_name] = selected.get("val")
                row["sourceFiledDate"] = row["sourceFiledDate"] or selected.get("filed")
                row["sourceForm"] = row["sourceForm"] or selected.get("form")

        if row["OperatingCashFlow"] is not None and row["CapitalExpenditure"] is not None:
            row["FreeCashFlow"] = float(row["OperatingCashFlow"]) - abs(float(row["CapitalExpenditure"]))

        if row["sourceFiledDate"]:
            rows.append(row)

    return rows


def build_quarterly_financials(company: dict[str, Any], facts: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    if not settings.get("includeQuarterly", True):
        return []

    start_year = datetime.now(timezone.utc).year - int(settings["lookbackYears"]) + 1
    end_year = datetime.now(timezone.utc).year
    quarter_map: dict[tuple[int, str], dict[str, Any]] = {}

    for metric_name, concepts in CONCEPT_MAP.items():
        for taxonomy, concept, unit in concepts["concepts"]:
            entries = fact_entries(facts, taxonomy, concept, unit)
            if not entries:
                continue
            for entry in entries:
                fiscal_year = entry.get("fy")
                fiscal_period = entry.get("fp")
                form = entry.get("form")
                if not fiscal_year or not fiscal_period or form not in {"10-Q", "10-Q/A"}:
                    continue
                if fiscal_year < start_year or fiscal_year > end_year:
                    continue
                if fiscal_period not in {"Q1", "Q2", "Q3"}:
                    continue
                if concepts["periodType"] == "duration" and not is_quarterly_duration(entry):
                    continue

                key = (int(fiscal_year), str(fiscal_period))
                row = quarter_map.setdefault(
                    key,
                    {
                        "cik": company["cik"],
                        "ticker": company["ticker"],
                        "companyName": company["name"],
                        "fiscalYear": int(fiscal_year),
                        "fiscalPeriod": str(fiscal_period),
                        "periodEnd": entry.get("end"),
                        "Revenue": None,
                        "NetIncome": None,
                        "OperatingIncome": None,
                        "TotalAssets": None,
                        "TotalLiabilities": None,
                        "ShareholdersEquity": None,
                        "CashAndEquivalents": None,
                        "OperatingCashFlow": None,
                        "CapitalExpenditure": None,
                        "ShareBasedCompensationExpense": None,
                        "SpecialItems": None,
                        "FreeCashFlow": None,
                        "DilutedEPS": None,
                        "SharesOutstanding": None,
                        "sourceFiledDate": entry.get("filed"),
                        "sourceForm": form,
                    },
                )

                current_filed = row.get("sourceFiledDate") or ""
                new_filed = entry.get("filed") or ""
                if new_filed >= current_filed:
                    row[metric_name] = entry.get("val")
                    row["periodEnd"] = entry.get("end") or row.get("periodEnd")
                    row["sourceFiledDate"] = new_filed or row.get("sourceFiledDate")
                    row["sourceForm"] = form or row.get("sourceForm")
            if any(row.get(metric_name) is not None for row in quarter_map.values()):
                break

    for fiscal_year, fiscal_period in list(quarter_map.keys()):
        special_sum = select_quarterly_fact_sum(
            facts,
            CONCEPT_MAP["SpecialItems"]["concepts"],
            fiscal_year,
            fiscal_period,
            CONCEPT_MAP["SpecialItems"]["periodType"],
        )
        if special_sum:
            row = quarter_map[(fiscal_year, fiscal_period)]
            row["SpecialItems"] = special_sum.get("val")
            row["sourceFiledDate"] = row["sourceFiledDate"] or special_sum.get("filed")
            row["sourceForm"] = row["sourceForm"] or special_sum.get("form")

    rows = sorted(quarter_map.values(), key=lambda item: (item["fiscalYear"], item["fiscalPeriod"]))
    for row in rows:
        if row["OperatingCashFlow"] is not None and row["CapitalExpenditure"] is not None:
            row["FreeCashFlow"] = float(row["OperatingCashFlow"]) - abs(float(row["CapitalExpenditure"]))
    return rows


def sync_company(company: dict[str, Any], settings: dict[str, Any], force: bool = False) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    filings = get_all_submissions(company, settings, force=force)
    facts = get_company_facts(company, settings, force=force)
    annuals = build_annual_financials(company, facts, settings)
    quarterlies = build_quarterly_financials(company, facts, settings)
    return filings, annuals, quarterlies


SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    ticker TEXT PRIMARY KEY,
    cik TEXT NOT NULL,
    name TEXT,
    security TEXT,
    sector TEXT,
    sub_industry TEXT,
    headquarters TEXT,
    date_added TEXT,
    listing_exchange TEXT,
    is_adr INTEGER NOT NULL DEFAULT 0,
    universe_source TEXT
);
CREATE TABLE IF NOT EXISTS filings (
    accession_number TEXT PRIMARY KEY,
    cik TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT,
    filing_date TEXT,
    report_date TEXT,
    acceptance_datetime TEXT,
    form TEXT,
    primary_document TEXT,
    description TEXT
);
CREATE TABLE IF NOT EXISTS annual_financials (
    cik TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT,
    fiscal_year INTEGER NOT NULL,
    revenue REAL,
    net_income REAL,
    operating_income REAL,
    total_assets REAL,
    total_liabilities REAL,
    shareholders_equity REAL,
    cash_and_equivalents REAL,
    operating_cash_flow REAL,
    capital_expenditure REAL,
    share_based_compensation_expense REAL,
    special_items REAL,
    free_cash_flow REAL,
    diluted_eps REAL,
    shares_outstanding REAL,
    source_filed_date TEXT,
    source_form TEXT,
    PRIMARY KEY (cik, fiscal_year)
);
CREATE TABLE IF NOT EXISTS quarterly_financials (
    cik TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT,
    fiscal_year INTEGER NOT NULL,
    fiscal_period TEXT NOT NULL,
    period_end TEXT,
    revenue REAL,
    net_income REAL,
    operating_income REAL,
    total_assets REAL,
    total_liabilities REAL,
    shareholders_equity REAL,
    cash_and_equivalents REAL,
    operating_cash_flow REAL,
    capital_expenditure REAL,
    share_based_compensation_expense REAL,
    special_items REAL,
    free_cash_flow REAL,
    diluted_eps REAL,
    shares_outstanding REAL,
    source_filed_date TEXT,
    source_form TEXT,
    PRIMARY KEY (cik, fiscal_year, fiscal_period)
);
CREATE TABLE IF NOT EXISTS market_quotes (
    ticker TEXT PRIMARY KEY,
    quote_date TEXT,
    quote_time TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    source TEXT NOT NULL,
    source_url TEXT,
    fetched_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS announcements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL,
    updated_companies INTEGER NOT NULL,
    new_filings INTEGER NOT NULL,
    markdown_path TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS profit_forecasts (
    ticker TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    metric TEXT NOT NULL DEFAULT 'net_income',
    forecast_value REAL,
    source_type TEXT,
    source_name TEXT,
    source_url TEXT,
    notes TEXT,
    updated_at_utc TEXT,
    PRIMARY KEY (ticker, fiscal_year, metric)
);
CREATE INDEX IF NOT EXISTS idx_filings_ticker_date ON filings (ticker, filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_filings_form_date ON filings (form, filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_annual_financials_ticker_year ON annual_financials (ticker, fiscal_year DESC);
CREATE INDEX IF NOT EXISTS idx_quarterly_financials_ticker_period ON quarterly_financials (ticker, fiscal_year DESC, fiscal_period DESC);
CREATE INDEX IF NOT EXISTS idx_market_quotes_date ON market_quotes (quote_date DESC);
CREATE INDEX IF NOT EXISTS idx_profit_forecasts_ticker_year ON profit_forecasts (ticker, fiscal_year DESC);
CREATE VIEW IF NOT EXISTS latest_annual_financials AS
SELECT a.*
FROM annual_financials a
JOIN (
    SELECT cik, MAX(fiscal_year) AS max_fiscal_year
    FROM annual_financials
    GROUP BY cik
) latest
    ON latest.cik = a.cik
   AND latest.max_fiscal_year = a.fiscal_year;
"""


@dataclass
class Database:
    path: Path

    def connect(self) -> sqlite3.Connection:
        ensure_dir(self.path.parent)
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=MEMORY")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA synchronous=OFF")
        conn.executescript(SCHEMA)
        ensure_schema_migrations(conn)
        conn.row_factory = sqlite3.Row
        return conn


def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})").fetchall())


def ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    if not has_column(conn, table, column):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def ensure_schema_migrations(conn: sqlite3.Connection) -> None:
    ensure_column(conn, "annual_financials", "share_based_compensation_expense", "REAL")
    ensure_column(conn, "annual_financials", "special_items", "REAL")
    ensure_column(conn, "quarterly_financials", "share_based_compensation_expense", "REAL")
    ensure_column(conn, "quarterly_financials", "special_items", "REAL")
    ensure_column(conn, "companies", "listing_exchange", "TEXT")
    ensure_column(conn, "companies", "is_adr", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "companies", "universe_source", "TEXT")
    ensure_column(conn, "profit_forecasts", "metric", "TEXT NOT NULL DEFAULT 'net_income'")
    ensure_column(conn, "profit_forecasts", "forecast_value", "REAL")
    ensure_column(conn, "profit_forecasts", "source_type", "TEXT")
    ensure_column(conn, "profit_forecasts", "source_name", "TEXT")
    ensure_column(conn, "profit_forecasts", "source_url", "TEXT")
    ensure_column(conn, "profit_forecasts", "notes", "TEXT")
    ensure_column(conn, "profit_forecasts", "updated_at_utc", "TEXT")


def load_profit_forecasts(settings: dict[str, Any]) -> list[dict[str, Any]]:
    rows = read_json(Path(settings["profitForecastsPath"])) or []
    forecasts: list[dict[str, Any]] = []
    for row in rows:
        ticker = normalize_ticker(row.get("ticker"))
        fiscal_year = row.get("fiscalYear", row.get("fiscal_year"))
        forecast_value = row.get("forecastNetIncome", row.get("forecast_net_income"))
        if not ticker or fiscal_year is None:
            continue
        forecasts.append(
            {
                "ticker": ticker,
                "fiscal_year": int(fiscal_year),
                "metric": row.get("metric") or "net_income",
                "forecast_value": float(forecast_value) if forecast_value is not None else None,
                "source_type": row.get("sourceType", row.get("source_type")),
                "source_name": row.get("sourceName", row.get("source_name")),
                "source_url": row.get("sourceUrl", row.get("source_url")),
                "notes": row.get("notes"),
                "updated_at_utc": row.get("updatedAtUtc", row.get("updated_at_utc")) or utc_now_iso(),
            }
        )
    return forecasts


def upsert_profit_forecasts(conn: sqlite3.Connection, forecasts: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM profit_forecasts")
    if not forecasts:
        return
    conn.executemany(
        """
        INSERT INTO profit_forecasts (
            ticker, fiscal_year, metric, forecast_value, source_type, source_name, source_url, notes, updated_at_utc
        ) VALUES (
            :ticker, :fiscal_year, :metric, :forecast_value, :source_type, :source_name, :source_url, :notes, :updated_at_utc
        )
        ON CONFLICT(ticker, fiscal_year, metric) DO UPDATE SET
            forecast_value = excluded.forecast_value,
            source_type = excluded.source_type,
            source_name = excluded.source_name,
            source_url = excluded.source_url,
            notes = excluded.notes,
            updated_at_utc = excluded.updated_at_utc
        """,
        forecasts,
    )


def refresh_profit_forecasts(conn: sqlite3.Connection, settings: dict[str, Any]) -> list[dict[str, Any]]:
    manual_forecasts = load_profit_forecasts(settings)
    existing_forecasts = rows_to_dicts(conn.execute("SELECT * FROM profit_forecasts"))
    preserved_official_rows = [row for row in existing_forecasts if row.get("source_type") == "official-guidance"]
    merged_rows = merge_profit_forecasts(manual_forecasts, preserved_official_rows)
    upsert_profit_forecasts(conn, merged_rows)
    return merged_rows


def merge_profit_forecasts(manual_rows: list[dict[str, Any]], official_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in official_rows:
        merged[(row["ticker"], row["fiscal_year"], row["metric"])] = row
    for row in manual_rows:
        merged[(row["ticker"], row["fiscal_year"], row["metric"])] = row
    return sorted(merged.values(), key=lambda item: (item["ticker"], item["fiscal_year"], item["metric"]))


def upsert_companies(conn: sqlite3.Connection, companies: list[dict[str, Any]]) -> None:
    conn.executemany(
        """
        INSERT INTO companies (
            ticker, cik, name, security, sector, sub_industry, headquarters, date_added,
            listing_exchange, is_adr, universe_source
        )
        VALUES (
            :ticker, :cik, :name, :security, :sector, :subIndustry, :headquarters, :dateAdded,
            :listingExchange, :isAdr, :universeSource
        )
        ON CONFLICT(ticker) DO UPDATE SET
            cik = excluded.cik,
            name = excluded.name,
            security = excluded.security,
            sector = excluded.sector,
            sub_industry = excluded.sub_industry,
            headquarters = excluded.headquarters,
            date_added = excluded.date_added,
            listing_exchange = excluded.listing_exchange,
            is_adr = excluded.is_adr,
            universe_source = excluded.universe_source
        """,
        companies,
    )


def upsert_filings(conn: sqlite3.Connection, filings: list[dict[str, Any]]) -> None:
    conn.executemany(
        """
        INSERT INTO filings (
            accession_number, cik, ticker, company_name, filing_date, report_date,
            acceptance_datetime, form, primary_document, description
        ) VALUES (
            :accessionNumber, :cik, :ticker, :companyName, :filingDate, :reportDate,
            :acceptanceDateTime, :form, :primaryDocument, :description
        )
        ON CONFLICT(accession_number) DO UPDATE SET
            cik = excluded.cik,
            ticker = excluded.ticker,
            company_name = excluded.company_name,
            filing_date = excluded.filing_date,
            report_date = excluded.report_date,
            acceptance_datetime = excluded.acceptance_datetime,
            form = excluded.form,
            primary_document = excluded.primary_document,
            description = excluded.description
        """,
        filings,
    )


def replace_annuals_for_company(conn: sqlite3.Connection, company: dict[str, Any], annuals: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM annual_financials WHERE cik = ?", (company["cik"],))
    conn.executemany(
        """
        INSERT INTO annual_financials (
            cik, ticker, company_name, fiscal_year, revenue, net_income, operating_income,
            total_assets, total_liabilities, shareholders_equity, cash_and_equivalents,
            operating_cash_flow, capital_expenditure, share_based_compensation_expense, special_items, free_cash_flow, diluted_eps,
            shares_outstanding, source_filed_date, source_form
        ) VALUES (
            :cik, :ticker, :companyName, :fiscalYear, :Revenue, :NetIncome, :OperatingIncome,
            :TotalAssets, :TotalLiabilities, :ShareholdersEquity, :CashAndEquivalents,
            :OperatingCashFlow, :CapitalExpenditure, :ShareBasedCompensationExpense, :SpecialItems, :FreeCashFlow, :DilutedEPS,
            :SharesOutstanding, :sourceFiledDate, :sourceForm
        )
        """,
        annuals,
    )


def replace_quarterlies_for_company(conn: sqlite3.Connection, company: dict[str, Any], quarterlies: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM quarterly_financials WHERE cik = ?", (company["cik"],))
    conn.executemany(
        """
        INSERT INTO quarterly_financials (
            cik, ticker, company_name, fiscal_year, fiscal_period, period_end, revenue, net_income, operating_income,
            total_assets, total_liabilities, shareholders_equity, cash_and_equivalents, operating_cash_flow,
            capital_expenditure, share_based_compensation_expense, special_items, free_cash_flow, diluted_eps, shares_outstanding, source_filed_date, source_form
        ) VALUES (
            :cik, :ticker, :companyName, :fiscalYear, :fiscalPeriod, :periodEnd, :Revenue, :NetIncome, :OperatingIncome,
            :TotalAssets, :TotalLiabilities, :ShareholdersEquity, :CashAndEquivalents, :OperatingCashFlow,
            :CapitalExpenditure, :ShareBasedCompensationExpense, :SpecialItems, :FreeCashFlow, :DilutedEPS, :SharesOutstanding, :sourceFiledDate, :sourceForm
        )
        """,
        quarterlies,
    )


def upsert_market_quotes(conn: sqlite3.Connection, quotes: list[dict[str, Any]]) -> None:
    if not quotes:
        return
    conn.executemany(
        """
        INSERT INTO market_quotes (
            ticker, quote_date, quote_time, open, high, low, close, volume,
            source, source_url, fetched_at_utc
        ) VALUES (
            :ticker, :quoteDate, :quoteTime, :open, :high, :low, :close, :volume,
            :source, :sourceUrl, :fetchedAtUtc
        )
        ON CONFLICT(ticker) DO UPDATE SET
            quote_date = excluded.quote_date,
            quote_time = excluded.quote_time,
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            source = excluded.source,
            source_url = excluded.source_url,
            fetched_at_utc = excluded.fetched_at_utc
        """,
        quotes,
    )


def get_existing_accessions(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT accession_number FROM filings")}


def get_companies_from_db(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ticker, cik, name, security, sector, sub_industry, headquarters, date_added, listing_exchange, is_adr, universe_source
        FROM companies
        ORDER BY ticker
        """
    ).fetchall()
    return [
        {
            "ticker": row["ticker"],
            "cik": row["cik"],
            "name": row["name"],
            "security": row["security"],
            "sector": row["sector"],
            "subIndustry": row["sub_industry"],
            "headquarters": row["headquarters"],
            "dateAdded": row["date_added"],
            "listingExchange": row["listing_exchange"],
            "isAdr": bool(row["is_adr"]),
            "universeSource": row["universe_source"],
        }
        for row in rows
    ]


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO sync_state (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def get_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM sync_state WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def export_json_snapshots(conn: sqlite3.Connection, settings: dict[str, Any]) -> None:
    db_root = Path(settings["dataRootResolved"]) / "db"
    companies = [dict(row) for row in conn.execute("SELECT * FROM companies ORDER BY ticker")]
    filings = [dict(row) for row in conn.execute("SELECT * FROM filings ORDER BY filing_date, accession_number")]
    annuals = [dict(row) for row in conn.execute("SELECT * FROM annual_financials ORDER BY ticker, fiscal_year")]
    quarterlies = [dict(row) for row in conn.execute("SELECT * FROM quarterly_financials ORDER BY ticker, fiscal_year, fiscal_period")]
    market_quotes = [dict(row) for row in conn.execute("SELECT * FROM market_quotes ORDER BY ticker")]
    write_json(db_root / "companies.json", companies)
    write_json(db_root / "filings.json", filings)
    write_json(db_root / "financials_annual.json", annuals)
    write_json(db_root / "financials_quarterly.json", quarterlies)
    write_json(db_root / "market_quotes.json", market_quotes)


def publish_announcement(settings: dict[str, Any], conn: sqlite3.Connection, title: str, updated_companies: list[dict[str, Any]], new_filings: list[dict[str, Any]]) -> None:
    root = Path(settings["dataRootResolved"]) / "announcements"
    history_root = root / "history"
    ensure_dir(history_root)
    generated = utc_now_iso()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    lines = [
        f"# {title}",
        "",
        f"- GeneratedAtUtc: {generated}",
        f"- UpdatedCompanies: {len(updated_companies)}",
        f"- NewFilings: {len(new_filings)}",
        "",
    ]
    if new_filings:
        lines.extend(["## New Filings", ""])
        for filing in sorted(new_filings, key=lambda item: (item.get("filingDate") or "", item["ticker"]), reverse=True):
            lines.append(f"- {filing.get('filingDate')} | {filing['ticker']} | {filing.get('form')} | {filing['accessionNumber']}")
        lines.append("")
    if updated_companies:
        lines.extend(["## Updated Companies", ""])
        for company in sorted(updated_companies, key=lambda item: item["ticker"]):
            lines.append(f"- {company['ticker']} | {company['name']} | CIK {company['cik']}")

    markdown = "\n".join(lines)
    latest_md = root / "latest.md"
    latest_json = root / "latest.json"
    history_md = history_root / f"{stamp}.md"
    history_json = history_root / f"{stamp}.json"
    payload = {
        "title": title,
        "generatedAtUtc": generated,
        "updatedCompanies": updated_companies,
        "newFilings": new_filings,
    }
    latest_md.write_text(markdown, encoding="utf-8")
    history_md.write_text(markdown, encoding="utf-8")
    write_json(latest_json, payload)
    write_json(history_json, payload)
    conn.execute(
        """
        INSERT INTO announcements (title, generated_at_utc, updated_companies, new_filings, markdown_path, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (title, generated, len(updated_companies), len(new_filings), str(history_md), json.dumps(payload, ensure_ascii=False)),
    )


def rows_to_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def run_status(settings: dict[str, Any]) -> None:
    db = Database(Path(settings["sqlitePath"]))
    with db.connect() as conn:
        refresh_profit_forecasts(conn, settings)
        counts = {
            "companies": conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
            "filings": conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0],
            "annualFinancials": conn.execute("SELECT COUNT(*) FROM annual_financials").fetchone()[0],
            "quarterlyFinancials": conn.execute("SELECT COUNT(*) FROM quarterly_financials").fetchone()[0],
            "marketQuotes": conn.execute("SELECT COUNT(*) FROM market_quotes").fetchone()[0],
            "profitForecasts": conn.execute("SELECT COUNT(*) FROM profit_forecasts").fetchone()[0],
            "announcements": conn.execute("SELECT COUNT(*) FROM announcements").fetchone()[0],
        }
        sync_state = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM sync_state")}
        recent_filings = rows_to_dicts(
            conn.execute(
                """
                SELECT ticker, form, filing_date, accession_number
                FROM filings
                ORDER BY filing_date DESC, accession_number DESC
                LIMIT 10
                """
            )
        )
        print_json({"counts": counts, "syncState": sync_state, "recentFilings": recent_filings})


def run_query_sql(settings: dict[str, Any], sql: str) -> None:
    db = Database(Path(settings["sqlitePath"]))
    with db.connect() as conn:
        cursor = conn.execute(sql)
        if cursor.description is None:
            conn.commit()
            print_json({"rowsAffected": cursor.rowcount})
            return
        print_json(rows_to_dicts(cursor))


def run_sync_progress(settings: dict[str, Any]) -> None:
    db = Database(Path(settings["sqlitePath"]))
    with db.connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        last_ticker = normalize_ticker(get_state(conn, "last_processed_ticker"))
        completed = 0
        if last_ticker:
            row = conn.execute(
                """
                WITH ordered AS (
                    SELECT ticker, ROW_NUMBER() OVER (ORDER BY ticker) AS rn
                    FROM companies
                )
                SELECT rn FROM ordered WHERE ticker = ?
                """,
                (last_ticker,),
            ).fetchone()
            completed = row[0] if row else 0
        print_json(
            {
                "totalCompanies": total,
                "lastProcessedTicker": last_ticker,
                "completedCompanies": completed,
                "remainingCompanies": max(total - completed, 0),
            }
        )


def run_export_csv(settings: dict[str, Any], table: str, output: str, where: str | None, order_by: str | None, limit: int | None) -> None:
    allowed_tables = {"companies", "filings", "annual_financials", "quarterly_financials", "market_quotes", "announcements", "latest_annual_financials"}
    if table not in allowed_tables:
        raise ValueError(f"Unsupported table or view: {table}")

    sql = f"SELECT * FROM {table}"
    if where:
        sql += f" WHERE {where}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit:
        sql += f" LIMIT {limit}"

    db = Database(Path(settings["sqlitePath"]))
    output_path = Path(output)
    if not output_path.is_absolute():
        output_path = (PROJECT_ROOT / output_path).resolve()
    ensure_dir(output_path.parent)

    with db.connect() as conn:
        cursor = conn.execute(sql)
        if cursor.description is None:
            raise RuntimeError("CSV export requires a SELECT query.")
        columns = [item[0] for item in cursor.description]
        rows = cursor.fetchall()

    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)

    print_json({"output": str(output_path), "rows": len(rows), "table": table})


def format_billions(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / 1_000_000_000, 2)


def growth_pct(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round(((float(current) - float(previous)) / abs(float(previous))) * 100, 2)


def round_or_none(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def ratio_or_none(numerator: float | None, denominator: float | None, digits: int = 2) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return round(float(numerator) / float(denominator), digits)


def adjusted_net_income_after_fee(row: dict[str, Any] | None) -> float | None:
    if not row or row.get("net_income") is None:
        return None
    base = float(row["net_income"])
    sbc = float(row["share_based_compensation_expense"]) if row.get("share_based_compensation_expense") is not None else 0.0
    return round(base - sbc, 2)


def normalized_net_income_proxy(row: dict[str, Any] | None) -> float | None:
    fee_adjusted = adjusted_net_income_after_fee(row)
    if fee_adjusted is None:
        return None
    special_items = abs(float(row["special_items"])) if row.get("special_items") is not None else 0.0
    return round(fee_adjusted - special_items, 2)


def parse_fiscal_year_hint(text: str) -> int | None:
    match = re.search(r"\b(?:fiscal year|fy)\s*(20\d{2})\b", text, re.I)
    if match:
        return int(match.group(1))
    match = re.search(r"\b(?:first|second|third|fourth)\s+quarter(?:\s+of)?\s+fiscal\s+(\d{2,4})\b", text, re.I)
    if match:
        return normalize_fiscal_year_token(match.group(1))
    match = re.search(r"\b(?:fiscal\s+q[1-4]|fq[1-4]|q[1-4]\s+fy)\s*[- ]?(\d{2,4})\b", text, re.I)
    if match:
        return normalize_fiscal_year_token(match.group(1))
    match = re.search(r"\bfor\s+(20\d{2})\b", text, re.I)
    if match:
        return int(match.group(1))
    return None


def parse_money_amount(raw_value: str, magnitude: str | None) -> float | None:
    try:
        value = float(raw_value.replace(",", ""))
    except ValueError:
        return None
    scale = (magnitude or "").lower()
    if scale.startswith("billion"):
        value *= 1_000_000_000
    elif scale.startswith("million"):
        value *= 1_000_000
    elif scale.startswith("thousand"):
        value *= 1_000
    return value


def parse_eps_amount(raw_value: str) -> float | None:
    try:
        return float(raw_value.replace(",", ""))
    except ValueError:
        return None


def extract_guidance_value_from_text(metric: str, text: str, shares_outstanding: float | None) -> tuple[float | None, str | None]:
    if metric == "revenue":
        revenue_patterns = [
            r"(?:revenue|sales)\s+(?:is expected to be|expected to be|is projected to be|projected to be|will be|to be|of)\s+\$?\s*([\d,]+(?:\.\d+)?)\s*(billion|million|thousand)?",
            r"(?:expect|expects|forecast|forecasts|project|projects|anticipate|anticipates).{0,80}?(?:revenue|sales).{0,40}?\$?\s*([\d,]+(?:\.\d+)?)\s*(billion|million|thousand)?",
        ]
        for pattern in revenue_patterns:
            match = re.search(pattern, text, re.I)
            if match:
                amount = parse_money_amount(match.group(1), match.group(2))
                if amount is not None and amount > 0:
                    return round(amount, 2), "revenue"
        return None, None

    money_patterns = [
        r"(?:net income|profit attributable to shareholders|profit attributable to owners|earnings)\s+(?:is expected to be|expected to be|is projected to be|projected to be|will be|to be|of)\s+\$?\s*([\d,]+(?:\.\d+)?)\s*(billion|million|thousand)?",
        r"(?:expect|expects|forecast|forecasts|project|projects|anticipate|anticipates).{0,80}?(?:net income|earnings).{0,40}?\$?\s*([\d,]+(?:\.\d+)?)\s*(billion|million|thousand)?",
    ]
    for pattern in money_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            amount = parse_money_amount(match.group(1), match.group(2))
            if amount is not None and amount > 0:
                return round(amount, 2), "net-income"

    if shares_outstanding not in (None, 0):
        eps_patterns = [
            r"(?:diluted\s+)?eps\s+(?:is expected to be|expected to be|is projected to be|projected to be|will be|to be|of)\s+\$?\s*([\d,]+(?:\.\d+)?)",
            r"(?:expect|expects|forecast|forecasts|project|projects|anticipate|anticipates).{0,80}?(?:diluted\s+)?eps.{0,20}?\$?\s*([\d,]+(?:\.\d+)?)",
        ]
        for pattern in eps_patterns:
            match = re.search(pattern, text, re.I)
            if match:
                eps = parse_eps_amount(match.group(1))
                if eps is not None and eps > 0:
                    return round(float(shares_outstanding) * eps, 2), "eps-derived"

    return None, None


def filing_candidates_for_guidance(submission_rows: list[dict[str, Any]], lookback_days: int = 400) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc).timestamp() - lookback_days * 86400
    candidates: list[dict[str, Any]] = []
    for row in submission_rows:
        form = row.get("form")
        filing_date = row.get("filingDate")
        primary_document = row.get("primaryDocument")
        accession = row.get("accessionNumber")
        if form not in GUIDANCE_FORMS or not filing_date or not primary_document or not accession:
            continue
        try:
            filed_ts = datetime.fromisoformat(f"{filing_date}T00:00:00+00:00").timestamp()
        except ValueError:
            continue
        if filed_ts < cutoff:
            continue
        candidates.append(row)
    candidates.sort(key=lambda item: (item.get("filingDate") or "", item.get("acceptanceDateTime") or ""), reverse=True)
    return candidates[:8]


def build_forecast_row(
    company: dict[str, Any],
    filing: dict[str, Any],
    accession_number: str,
    document_name: str,
    document_url: str,
    metric: str,
    fiscal_year: int,
    forecast_value: float,
    extraction_kind: str,
    excerpt: str,
) -> dict[str, Any]:
    return {
        "ticker": company["ticker"],
        "fiscal_year": fiscal_year,
        "metric": metric,
        "forecast_value": forecast_value,
        "source_type": "official-guidance",
        "source_name": f"{filing.get('form')} {filing.get('filingDate')}",
        "source_url": document_url,
        "notes": f"Extracted from SEC official filing; accession {accession_number}; document={document_name}; method={extraction_kind}; excerpt={excerpt[:400]}",
        "updated_at_utc": utc_now_iso(),
    }


def extract_official_guidance_forecasts(
    company: dict[str, Any],
    settings: dict[str, Any],
    shares_outstanding: float | None,
    force: bool = False,
) -> list[dict[str, Any]]:
    extracted: dict[tuple[str, int], dict[str, Any]] = {}
    submissions = get_all_submissions(company, settings, force=force, tracked_forms=GUIDANCE_FORMS)
    for filing in filing_candidates_for_guidance(submissions):
        accession_number = filing["accessionNumber"]
        primary_document = filing.get("primaryDocument")
        if not primary_document:
            continue
        source_url = sec_archive_document_url(company["cik"], accession_number, primary_document)
        cache_path = filing_document_cache_path(settings, company["cik"], accession_number, primary_document)
        try:
            raw_text = get_or_fetch_filing_text(source_url, cache_path, settings, force=force)
        except Exception as exc:
            append_log(settings, f"Guidance document fetch failed for {company['ticker']} {accession_number}: {exc}")
            continue

        related_documents = [primary_document] + extract_guidance_exhibit_links(raw_text)
        seen_docs: set[str] = set()
        for document_name in related_documents:
            if document_name in seen_docs:
                continue
            seen_docs.add(document_name)
            document_url = sec_archive_document_url(company["cik"], accession_number, document_name)
            document_cache_path = filing_document_cache_path(settings, company["cik"], accession_number, document_name)
            try:
                document_raw = raw_text if document_name == primary_document else get_or_fetch_filing_text(document_url, document_cache_path, settings, force=force)
            except Exception as exc:
                append_log(settings, f"Guidance exhibit fetch failed for {company['ticker']} {accession_number} {document_name}: {exc}")
                continue
            for metric, fiscal_year, raw_value, extraction_kind in extract_table_guidance_forecasts(document_raw):
                forecast_value = raw_value
                if metric == "eps":
                    if shares_outstanding in (None, 0):
                        continue
                    forecast_value = round(float(shares_outstanding) * raw_value, 2)
                    metric = "net_income"
                key = (metric, fiscal_year)
                if key in extracted:
                    continue
                extracted[key] = build_forecast_row(
                    company=company,
                    filing=filing,
                    accession_number=accession_number,
                    document_name=document_name,
                    document_url=document_url,
                    metric=metric,
                    fiscal_year=fiscal_year,
                    forecast_value=forecast_value,
                    extraction_kind=extraction_kind,
                    excerpt="Qx FY outlook table",
                )
            plain_text = html_to_text(document_raw)
            sentences = split_sentences(plain_text)
            if not sentences:
                continue
            for index, sentence in enumerate(sentences):
                if not sentence_contains_guidance(sentence):
                    continue
                window = " ".join(sentences[index : min(index + 3, len(sentences))])
                lower_window = window.lower()
                if "net income" not in lower_window and "earnings" not in lower_window and "eps" not in lower_window and "revenue" not in lower_window and "sales" not in lower_window:
                    continue
                fiscal_year = parse_fiscal_year_hint(window) or parse_fiscal_year_hint(plain_text[:5000])
                if fiscal_year is None:
                    continue
                for metric in ("net_income", "revenue"):
                    forecast_value, extraction_kind = extract_guidance_value_from_text(metric, window, shares_outstanding)
                    if forecast_value is None:
                        continue
                    key = (metric, fiscal_year)
                    if key in extracted:
                        continue
                    extracted[key] = build_forecast_row(
                        company=company,
                        filing=filing,
                        accession_number=accession_number,
                        document_name=document_name,
                        document_url=document_url,
                        metric=metric,
                        fiscal_year=fiscal_year,
                        forecast_value=forecast_value,
                        extraction_kind=extraction_kind,
                        excerpt=window,
                    )
    return sorted(extracted.values(), key=lambda item: (item["metric"], item["fiscal_year"]))


def valuation_multiple(market_cap: float | None, metric: float | None, digits: int = 2) -> float | None:
    if market_cap is None or metric in (None, 0):
        return None
    if float(metric) <= 0:
        return None
    return round(float(market_cap) / float(metric), digits)


def geometric_average_growth(values: list[float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    if len(valid) < 3:
        return None
    start = valid[0]
    end = valid[-1]
    periods = len(valid) - 1
    if start <= 0 or end <= 0 or periods <= 0:
        return None
    return round((((end / start) ** (1 / periods)) - 1) * 100, 2)


def projected_five_year_normalized_net_income(
    latest_normalized_net_income: float | None,
    geometric_growth_pct: float | None,
) -> float | None:
    if latest_normalized_net_income is None or geometric_growth_pct is None:
        return None
    if latest_normalized_net_income <= 0:
        return None
    growth_rate = geometric_growth_pct / 100
    total = 0.0
    for year in range(1, 6):
        total += float(latest_normalized_net_income) * ((1 + growth_rate) ** year)
    return round(total, 2)


def market_cap_payback_ratio(
    market_cap: float | None,
    projected_total: float | None,
) -> float | None:
    if market_cap is None or projected_total is None or market_cap <= 0:
        return None
    return round((float(projected_total) / float(market_cap)) * 100, 2)


def build_three_year_analysis(company: dict[str, Any], annuals: list[dict[str, Any]], market_data: dict[str, Any] | None) -> dict[str, Any]:
    recent_years = annuals[-3:]
    latest = recent_years[-1] if recent_years else None
    previous = recent_years[-2] if len(recent_years) > 1 else None
    market_cap = market_data.get("marketCap") if market_data else None

    latest_ps = valuation_multiple(market_cap, latest.get("revenue") if latest else None)
    latest_operating_profit = latest.get("operating_income") if latest else None
    latest_fee_adjusted = adjusted_net_income_after_fee(latest)
    latest_normalized = normalized_net_income_proxy(latest)
    previous_normalized = normalized_net_income_proxy(previous)
    normalized_growth = growth_pct(latest_normalized, previous_normalized)
    normalized_pe = valuation_multiple(market_cap, latest_normalized)
    normalized_values = [normalized_net_income_proxy(row) for row in recent_years]
    normalized_growth_geomean = geometric_average_growth(normalized_values)
    revenue_values = [row.get("revenue") for row in recent_years]
    revenue_growth_geomean = geometric_average_growth(revenue_values)
    normalized_growth_for_projection = revenue_growth_geomean
    projected_five_year_normalized = projected_five_year_normalized_net_income(latest_normalized, normalized_growth_for_projection)
    five_year_market_cap_payback = market_cap_payback_ratio(market_cap, projected_five_year_normalized)
    operating_margin = ratio_or_none(latest_operating_profit, latest.get("revenue") if latest else None)
    if operating_margin is not None:
        operating_margin = round(operating_margin * 100, 2)

    lines: list[str] = []
    if latest:
        lines.append(
            f"FY {latest['fiscal_year']}: PS {latest_ps if latest_ps is not None else '--'}x, "
            f"operating profit {format_billions(latest_operating_profit) if latest_operating_profit is not None else '--'}B, "
            f"normalized NI proxy {format_billions(latest_normalized) if latest_normalized is not None else '--'}B."
        )
    if normalized_growth is not None:
        trend = "improving" if normalized_growth >= 0 else "declining"
        lines.append(f"Normalized net income proxy growth is {normalized_growth}% YoY, trend {trend}.")
    if revenue_growth_geomean is not None:
        lines.append(f"3Y geometric revenue growth is {revenue_growth_geomean}%.")
    if normalized_pe is not None:
        lines.append(f"Normalized P/E proxy is {normalized_pe}x based on current market cap.")
    if five_year_market_cap_payback is not None:
        lines.append(
            f"5Y market-cap payback ratio is {five_year_market_cap_payback}% using 3Y geometric revenue growth."
        )

    return {
        "years": [
            {
                **row,
                "psRatio": valuation_multiple(market_cap, row.get("revenue")),
                "operatingMarginPct": round_or_none(ratio_or_none(row.get("operating_income"), row.get("revenue"), 4) * 100 if ratio_or_none(row.get("operating_income"), row.get("revenue"), 4) is not None else None, 2),
                "feeAdjustedNetIncome": adjusted_net_income_after_fee(row),
                "normalizedNetIncomeProxy": normalized_net_income_proxy(row),
            }
            for row in recent_years
        ],
        "latestPsRatio": latest_ps,
        "latestOperatingProfit": latest_operating_profit,
        "latestOperatingMarginPct": operating_margin,
        "latestFeeAdjustedNetIncome": latest_fee_adjusted,
        "latestNormalizedNetIncomeProxy": latest_normalized,
        "normalizedNetIncomeGrowthPct": normalized_growth,
        "normalizedNetIncomeGrowthGeomeanPct": normalized_growth_geomean,
        "revenueGrowthGeomeanPct": revenue_growth_geomean,
        "normalizedNetIncomeProjectionGrowthPct": normalized_growth_for_projection,
        "normalizedPeProxy": normalized_pe,
        "projectedFiveYearNormalizedNetIncome": projected_five_year_normalized,
        "fiveYearMarketCapPaybackPct": five_year_market_cap_payback,
        "commentary": lines,
        "methodology": [
            "Fee-adjusted net income proxy = net income - stock-based compensation expense.",
            "Normalized net income proxy = fee-adjusted net income proxy - absolute special items.",
            "5Y market-cap payback ratio = projected next 5 years normalized NI proxy sum / current market cap.",
            "Projection growth uses 3Y geometric revenue growth.",
        ],
    }


def select_latest_profit_forecast(forecasts: list[dict[str, Any]]) -> dict[str, Any] | None:
    return select_latest_metric_forecast(forecasts, "net_income")


def select_latest_metric_forecast(forecasts: list[dict[str, Any]], metric: str) -> dict[str, Any] | None:
    metric_rows = [row for row in forecasts if row.get("metric") == metric]
    if not metric_rows:
        return None
    return sorted(
        metric_rows,
        key=lambda item: (item.get("fiscal_year") or 0, item.get("updated_at_utc") or ""),
        reverse=True,
    )[0]


def select_latest_balance_sheet_row(
    quarterlies: list[dict[str, Any]],
    annuals: list[dict[str, Any]],
) -> dict[str, Any] | None:
    for row in reversed(quarterlies):
        if row.get("shares_outstanding") is not None or row.get("shareholders_equity") is not None:
            return row
    for row in reversed(annuals):
        if row.get("shares_outstanding") is not None or row.get("shareholders_equity") is not None:
            return row
    return None


def build_market_snapshot(
    quote: dict[str, Any] | None,
    annuals: list[dict[str, Any]],
    quarterlies: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not quote:
        return None

    latest_annual = annuals[-1] if annuals else None
    balance_sheet_row = select_latest_balance_sheet_row(quarterlies, annuals)
    price = quote.get("close")
    shares_outstanding = balance_sheet_row.get("shares_outstanding") if balance_sheet_row else None
    shareholders_equity = balance_sheet_row.get("shareholders_equity") if balance_sheet_row else None
    diluted_eps = latest_annual.get("diluted_eps") if latest_annual else None

    market_cap = None
    if price is not None and shares_outstanding not in (None, 0):
        market_cap = float(price) * float(shares_outstanding)

    pe_ratio = None
    if price is not None and diluted_eps not in (None, 0) and float(diluted_eps) > 0:
        pe_ratio = float(price) / float(diluted_eps)

    pb_ratio = None
    if market_cap is not None and shareholders_equity not in (None, 0) and float(shareholders_equity) > 0:
        pb_ratio = float(market_cap) / float(shareholders_equity)

    return {
        "price": round_or_none(price, 2),
        "previousClose": round_or_none(price, 2),
        "priceDate": quote.get("quote_date"),
        "priceTime": quote.get("quote_time"),
        "open": round_or_none(quote.get("open"), 2),
        "high": round_or_none(quote.get("high"), 2),
        "low": round_or_none(quote.get("low"), 2),
        "volume": round_or_none(quote.get("volume"), 0),
        "marketCap": round_or_none(market_cap, 2),
        "peRatio": round_or_none(pe_ratio, 2),
        "pbRatio": round_or_none(pb_ratio, 2),
        "sharesOutstanding": round_or_none(shares_outstanding, 0),
        "shareholdersEquity": round_or_none(shareholders_equity, 2),
        "dilutedEps": round_or_none(diluted_eps, 2),
        "equitySourceFiledDate": balance_sheet_row.get("source_filed_date") if balance_sheet_row else None,
        "equitySourceForm": balance_sheet_row.get("source_form") if balance_sheet_row else None,
        "earningsFiscalYear": latest_annual.get("fiscal_year") if latest_annual else None,
        "source": quote.get("source"),
        "fetchedAtUtc": quote.get("fetched_at_utc"),
    }


def build_web_data(settings: dict[str, Any]) -> None:
    db = Database(Path(settings["sqlitePath"]))
    web_root = PROJECT_ROOT / "web" / "data"
    companies_root = web_root / "companies"
    ensure_dir(companies_root)

    with db.connect() as conn:
        refresh_profit_forecasts(conn, settings)
        companies = rows_to_dicts(
            conn.execute(
                """
                SELECT ticker, cik, name, security, sector, sub_industry, headquarters, date_added, listing_exchange, is_adr, universe_source
                FROM companies
                ORDER BY ticker
                """
            )
        )
        annuals = rows_to_dicts(conn.execute("SELECT * FROM annual_financials ORDER BY ticker, fiscal_year"))
        quarterlies = rows_to_dicts(conn.execute("SELECT * FROM quarterly_financials ORDER BY ticker, fiscal_year, fiscal_period"))
        filings = rows_to_dicts(conn.execute("SELECT * FROM filings ORDER BY filing_date DESC, accession_number DESC"))
        market_quotes = rows_to_dicts(conn.execute("SELECT * FROM market_quotes ORDER BY ticker"))
        profit_forecasts = rows_to_dicts(conn.execute("SELECT * FROM profit_forecasts ORDER BY ticker, fiscal_year DESC"))

    annuals_by_ticker: dict[str, list[dict[str, Any]]] = {}
    quarterlies_by_ticker: dict[str, list[dict[str, Any]]] = {}
    filings_by_ticker: dict[str, list[dict[str, Any]]] = {}
    market_quotes_by_ticker = {row["ticker"]: row for row in market_quotes}
    forecasts_by_ticker: dict[str, list[dict[str, Any]]] = {}

    for row in annuals:
        annuals_by_ticker.setdefault(row["ticker"], []).append(row)
    for row in quarterlies:
        quarterlies_by_ticker.setdefault(row["ticker"], []).append(row)
    for row in filings:
        filings_by_ticker.setdefault(row["ticker"], []).append(row)
    for row in profit_forecasts:
        forecasts_by_ticker.setdefault(row["ticker"], []).append(row)

    companies_by_cik: dict[str, list[dict[str, Any]]] = {}
    for company in companies:
        companies_by_cik.setdefault(company["cik"], []).append(company)

    summaries: list[dict[str, Any]] = []
    for cik, grouped_companies in companies_by_cik.items():
        primary_company = grouped_companies[0]
        primary_ticker = primary_company["ticker"]
        aliases = [row["ticker"] for row in grouped_companies]

        company_annuals = annuals_by_ticker.get(primary_ticker, [])
        if not company_annuals:
            for alias in aliases[1:]:
                if annuals_by_ticker.get(alias):
                    primary_ticker = alias
                    company_annuals = annuals_by_ticker.get(alias, [])
                    break

        company_quarterlies = quarterlies_by_ticker.get(primary_ticker, [])
        company_filings = filings_by_ticker.get(primary_ticker, [])[:20]
        latest_annual = company_annuals[-1] if company_annuals else None
        previous_annual = company_annuals[-2] if len(company_annuals) > 1 else None
        latest_quarter = company_quarterlies[-1] if company_quarterlies else None
        latest_filing = company_filings[0] if company_filings else None
        market_data = build_market_snapshot(market_quotes_by_ticker.get(primary_ticker), company_annuals, company_quarterlies)
        analysis = build_three_year_analysis(primary_company, company_annuals, market_data)
        ticker_forecasts = forecasts_by_ticker.get(primary_ticker, [])
        forecast = select_latest_metric_forecast(ticker_forecasts, "net_income")
        revenue_forecast = select_latest_metric_forecast(ticker_forecasts, "revenue")
        forward_pe = valuation_multiple(market_data.get("marketCap") if market_data else None, forecast.get("forecast_value") if forecast else None)

        summary = {
            "ticker": primary_ticker,
            "aliases": aliases,
            "cik": cik,
            "name": primary_company["name"],
            "security": primary_company["security"],
            "sector": primary_company["sector"],
            "subIndustry": primary_company["sub_industry"],
            "headquarters": primary_company["headquarters"],
            "dateAdded": primary_company["date_added"],
            "listingExchange": primary_company["listing_exchange"],
            "isAdr": bool(primary_company["is_adr"]),
            "universeSource": primary_company["universe_source"],
            "latestAnnual": latest_annual,
            "latestQuarter": latest_quarter,
            "latestFiling": latest_filing,
            "marketData": market_data,
            "analysis": analysis,
            "revenueBillions": format_billions(latest_annual["revenue"]) if latest_annual else None,
            "netIncomeBillions": format_billions(latest_annual["net_income"]) if latest_annual else None,
            "freeCashFlowBillions": format_billions(latest_annual["free_cash_flow"]) if latest_annual else None,
            "revenueGrowthPct": growth_pct(
                latest_annual["revenue"] if latest_annual else None,
                previous_annual["revenue"] if previous_annual else None,
            ),
            "netIncomeGrowthPct": growth_pct(
                latest_annual["net_income"] if latest_annual else None,
                previous_annual["net_income"] if previous_annual else None,
            ),
            "marketCapBillions": format_billions(market_data["marketCap"]) if market_data and market_data.get("marketCap") is not None else None,
            "psRatio": analysis["latestPsRatio"],
            "operatingProfit": analysis["latestOperatingProfit"],
            "feeAdjustedNetIncome": analysis["latestFeeAdjustedNetIncome"],
            "normalizedNetIncomeGrowthPct": analysis["normalizedNetIncomeGrowthPct"],
            "normalizedNetIncomeGrowthGeomeanPct": analysis["normalizedNetIncomeGrowthGeomeanPct"],
            "revenueGrowthGeomeanPct": analysis["revenueGrowthGeomeanPct"],
            "normalizedNetIncomeProjectionGrowthPct": analysis["normalizedNetIncomeProjectionGrowthPct"],
            "normalizedPeProxy": analysis["normalizedPeProxy"],
            "projectedFiveYearNormalizedNetIncome": analysis["projectedFiveYearNormalizedNetIncome"],
            "fiveYearMarketCapPaybackPct": analysis["fiveYearMarketCapPaybackPct"],
            "forecastNetIncome": forecast.get("forecast_value") if forecast else None,
            "forecastNetIncomeFiscalYear": forecast.get("fiscal_year") if forecast else None,
            "forecastSourceType": forecast.get("source_type") if forecast else None,
            "forecastSourceName": forecast.get("source_name") if forecast else None,
            "forecastRevenue": revenue_forecast.get("forecast_value") if revenue_forecast else None,
            "forecastRevenueFiscalYear": revenue_forecast.get("fiscal_year") if revenue_forecast else None,
            "forecastRevenueSourceType": revenue_forecast.get("source_type") if revenue_forecast else None,
            "forecastRevenueSourceName": revenue_forecast.get("source_name") if revenue_forecast else None,
            "forwardPeRatio": forward_pe,
        }
        summaries.append(summary)

        detail_payload = {
            "company": summary,
            "annuals": company_annuals,
            "quarterlies": company_quarterlies,
            "filings": company_filings,
        }
        write_json(companies_root / f"{primary_ticker}.json", detail_payload)
        for alias in aliases:
            if alias != primary_ticker:
                write_json(companies_root / f"{alias}.json", detail_payload)

    sector_map: dict[str, dict[str, Any]] = {}
    for company in summaries:
        sector = company["sector"] or "Unknown"
        bucket = sector_map.setdefault(
            sector,
            {
                "sector": sector,
                "companyCount": 0,
                "totalRevenue": 0.0,
                "totalNetIncome": 0.0,
            },
        )
        bucket["companyCount"] += 1
        bucket["totalRevenue"] += float(company["latestAnnual"]["revenue"]) if company.get("latestAnnual") and company["latestAnnual"].get("revenue") is not None else 0.0
        bucket["totalNetIncome"] += float(company["latestAnnual"]["net_income"]) if company.get("latestAnnual") and company["latestAnnual"].get("net_income") is not None else 0.0

    sectors = sorted(
        [
            {
                **row,
                "totalRevenueBillions": format_billions(row["totalRevenue"]),
                "totalNetIncomeBillions": format_billions(row["totalNetIncome"]),
            }
            for row in sector_map.values()
        ],
        key=lambda item: item["totalRevenue"],
        reverse=True,
    )

    highlight_limit = 100

    top_revenue = sorted(summaries, key=lambda item: item["latestAnnual"]["revenue"] if item.get("latestAnnual") and item["latestAnnual"].get("revenue") is not None else 0, reverse=True)[:highlight_limit]
    top_profit = sorted(
        [item for item in summaries if item.get("latestAnnual") and item["latestAnnual"].get("net_income") is not None],
        key=lambda item: item["latestAnnual"]["net_income"],
        reverse=True,
    )[:highlight_limit]
    top_ps = sorted(
        [item for item in summaries if item.get("psRatio") is not None],
        key=lambda item: item["psRatio"],
        reverse=True,
    )[:highlight_limit]
    top_normalized_growth = sorted(
        [item for item in summaries if item.get("normalizedNetIncomeGrowthPct") is not None],
        key=lambda item: item["normalizedNetIncomeGrowthPct"],
        reverse=True,
    )[:highlight_limit]
    top_market_cap_payback = sorted(
        [item for item in summaries if item.get("fiveYearMarketCapPaybackPct") is not None],
        key=lambda item: item["fiveYearMarketCapPaybackPct"],
        reverse=True,
    )[:highlight_limit]
    top_market_cap = sorted(
        [item for item in summaries if item.get("marketData") and item["marketData"].get("marketCap") is not None],
        key=lambda item: item["marketData"]["marketCap"],
        reverse=True,
    )[:highlight_limit]
    top_forward_pe = sorted(
        [item for item in summaries if item.get("forwardPeRatio") is not None],
        key=lambda item: item["forwardPeRatio"],
    )[:highlight_limit]
    latest_filings = sorted(
        [item for item in summaries if item.get("latestFiling") and item["latestFiling"].get("filing_date")],
        key=lambda item: item["latestFiling"]["filing_date"],
        reverse=True,
    )[:highlight_limit]

    payload = {
        "generatedAtUtc": utc_now_iso(),
        "companyCount": len(summaries),
        "companies": summaries,
        "sectors": sectors,
        "highlights": {
            "topRevenue": top_revenue,
            "topProfit": top_profit,
            "topPs": top_ps,
            "topNormalizedGrowth": top_normalized_growth,
            "topMarketCapPayback": top_market_cap_payback,
            "topMarketCap": top_market_cap,
            "topForwardPe": top_forward_pe,
            "latestFilings": latest_filings,
        },
    }
    write_json(web_root / "summary.json", payload)
    print_json({"output": str(web_root), "companyCount": len(summaries)})


def refresh_companies(settings: dict[str, Any]) -> None:
    checkpoint = load_universe_checkpoint(settings, force=False)
    staged_companies = load_staged_universe_from_checkpoint(settings)
    if staged_companies and not checkpoint["completed"]:
        append_log(
            settings,
            f"Publishing staged universe progress before resume: {len(staged_companies)} companies accepted through {checkpoint['lastTicker'] or '--'}",
        )
        persist_staged_universe_checkpoint(settings, staged_companies, build_web=True)

    db = Database(Path(settings["sqlitePath"]))
    companies = resolve_companies(settings)
    with db.connect() as conn:
        upsert_companies(conn, companies)
        set_state(conn, "last_universe_refresh_utc", utc_now_iso())
        set_state(conn, "last_processed_ticker", "")
        set_state(conn, "last_processed_cik", "")
        set_state(conn, "last_processed_step", "")
        export_json_snapshots(conn, settings)
        conn.commit()
    build_web_data(settings)
    print_json({"companiesRefreshed": len(companies), "mode": "universe-only"})


def stage_universe_checkpoint(settings: dict[str, Any], limit: int | None = None) -> None:
    companies = load_staged_universe_from_checkpoint(settings)
    if limit:
        companies = companies[:limit]
    persist_staged_universe_checkpoint(settings, companies, build_web=True)
    print_json({"companiesStaged": len(companies), "mode": "checkpoint"})


def recompute_financials(settings: dict[str, Any], ticker: str | None = None, limit: int | None = None) -> None:
    db = Database(Path(settings["sqlitePath"]))
    recomputed_at = utc_now_iso()
    with db.connect() as conn:
        companies = filter_companies(get_companies_from_db(conn), ticker, limit)
        total = len(companies)
        for index, company in enumerate(companies, start=1):
            append_log(settings, f"Recompute financials [{index}/{total}] {company['ticker']} {company['cik']}")
            facts = get_company_facts(company, settings, force=False)
            annuals = build_annual_financials(company, facts, settings)
            quarterlies = build_quarterly_financials(company, facts, settings)
            replace_annuals_for_company(conn, company, annuals)
            replace_quarterlies_for_company(conn, company, quarterlies)
            conn.commit()
        sync_market_data(conn, settings, companies, force=False)
        export_json_snapshots(conn, settings)
        set_state(conn, "last_recompute_utc", recomputed_at)
        conn.commit()
    build_web_data(settings)
    print_json({"recomputedCompanies": total})


def run_market_data_sync(settings: dict[str, Any], ticker: str | None = None, limit: int | None = None, force: bool = False) -> None:
    db = Database(Path(settings["sqlitePath"]))
    market_sync_at = utc_now_iso()
    with db.connect() as conn:
        companies = get_companies_from_db(conn)
        if not companies:
            companies = resolve_companies(settings)
            upsert_companies(conn, companies)
        companies = filter_companies(companies, ticker, limit)
        refreshed = sync_market_data(conn, settings, companies, force=force)
        export_json_snapshots(conn, settings)
        set_state(conn, "last_market_sync_utc", market_sync_at)
        conn.commit()
    build_web_data(settings)
    print_json({"marketQuotesRefreshed": refreshed, "companies": len(companies)})


def rebuild_db_from_cache(settings: dict[str, Any], output_path: str) -> None:
    target_path = Path(output_path)
    if not target_path.is_absolute():
        target_path = (PROJECT_ROOT / target_path).resolve()

    if target_path.exists():
        raise RuntimeError(f"Target database already exists: {target_path}")

    companies = load_companies_snapshot(settings)
    target_db = Database(target_path)
    rebuild_at = utc_now_iso()
    with target_db.connect() as conn:
        upsert_companies(conn, companies)
        total = len(companies)
        for index, company in enumerate(companies, start=1):
            append_log(settings, f"Rebuild DB [{index}/{total}] {company['ticker']} {company['cik']}")
            filings = get_all_submissions(company, settings, force=False)
            facts = get_company_facts(company, settings, force=False)
            annuals = build_annual_financials(company, facts, settings)
            quarterlies = build_quarterly_financials(company, facts, settings)
            upsert_filings(conn, filings)
            replace_annuals_for_company(conn, company, annuals)
            replace_quarterlies_for_company(conn, company, quarterlies)
            conn.commit()
        sync_market_data(conn, settings, companies, force=True)
        export_json_snapshots(conn, {**settings, "sqlitePath": str(target_path)})
        set_state(conn, "last_rebuild_utc", rebuild_at)
        conn.commit()
    build_web_data({**settings, "sqlitePath": str(target_path)})
    print_json({"rebuiltDatabase": str(target_path), "companies": len(companies)})


def filter_companies(companies: list[dict[str, Any]], ticker: str | None, limit: int | None) -> list[dict[str, Any]]:
    if ticker:
        requested = {normalize_ticker(item) for item in ticker.split(",")}
        requested.discard(None)
        companies = [company for company in companies if company["ticker"] in requested]
    if limit:
        companies = companies[:limit]
    return companies


def filter_companies_by_universe(companies: list[dict[str, Any]], universe: str | None) -> list[dict[str, Any]]:
    if not universe:
        return companies
    requested = {item.strip().lower() for item in universe.split(",") if item.strip()}
    if not requested:
        return companies
    filtered: list[dict[str, Any]] = []
    for company in companies:
        sources = {
            item.strip().lower()
            for item in str(company.get("universeSource") or "").split(",")
            if item.strip()
        }
        if sources & requested:
            filtered.append(company)
    return filtered


def apply_resume_checkpoint(conn: sqlite3.Connection, companies: list[dict[str, Any]], resume: bool, ticker: str | None) -> tuple[list[dict[str, Any]], str | None]:
    if not resume or ticker:
        return companies, None

    last_ticker = normalize_ticker(get_state(conn, "last_processed_ticker"))
    if not last_ticker:
        return companies, None

    for index, company in enumerate(companies):
        if company["ticker"] == last_ticker:
            return companies[index + 1 :], last_ticker
    return companies, None


def run_full_sync(
    settings: dict[str, Any],
    ticker: str | None = None,
    limit: int | None = None,
    force: bool = False,
    resume: bool = False,
    refresh_universe: bool = False,
) -> None:
    db = Database(Path(settings["sqlitePath"]))
    full_sync_at = utc_now_iso()
    with db.connect() as conn:
        companies = get_companies_from_db(conn)
        if refresh_universe or not companies:
            companies = resolve_companies(settings, force=force)
            upsert_companies(conn, companies)
            set_state(conn, "last_universe_refresh_utc", utc_now_iso())
            if refresh_universe:
                set_state(conn, "last_processed_ticker", "")
                set_state(conn, "last_processed_cik", "")
                set_state(conn, "last_processed_step", "")
            conn.commit()
        companies = filter_companies(companies, ticker, None)
        companies, resumed_from = apply_resume_checkpoint(conn, companies, resume, ticker)
        companies = filter_companies(companies, None, limit)
        all_filings: list[dict[str, Any]] = []
        all_updated_companies: list[dict[str, Any]] = []
        failed_companies: list[dict[str, Any]] = []
        total = len(companies)
        if resumed_from:
            append_log(settings, f"Full sync resumed after {resumed_from}; remaining {total} companies")
        elif resume:
            append_log(settings, "Full sync resume requested but no checkpoint was found; starting from the beginning")
        append_log(settings, f"Full sync started for {total} companies")
        for index, company in enumerate(companies, start=1):
            try:
                append_log(settings, f"Full sync [{index}/{total}] {company['ticker']} {company['cik']}")
                filings, annuals, quarterlies = sync_company(company, settings, force=force)
                upsert_filings(conn, filings)
                replace_annuals_for_company(conn, company, annuals)
                replace_quarterlies_for_company(conn, company, quarterlies)
                set_state(conn, "last_processed_ticker", company["ticker"])
                set_state(conn, "last_processed_cik", company["cik"])
                set_state(conn, "last_processed_step", f"{index}/{total}")
                conn.commit()
                all_filings.extend(filings)
                all_updated_companies.append(company)
            except Exception as exc:
                conn.rollback()
                append_log(settings, f"Full sync failed for {company['ticker']} {company['cik']}: {exc}")
                failed_companies.append({"ticker": company["ticker"], "cik": company["cik"], "error": str(exc)})
                continue
        export_json_snapshots(conn, settings)
        set_state(conn, "last_full_sync_utc", utc_now_iso())
        set_state(conn, "last_full_sync_failed_count", str(len(failed_companies)))
        failed_path = Path(settings["dataRootResolved"]) / "logs" / f"full-sync-failed-{utc_stamp()}.json"
        if failed_companies:
            write_json(failed_path, failed_companies)
            append_log(settings, f"Full sync completed with {len(failed_companies)} failed companies. Details: {failed_path}")
        else:
            append_log(settings, "Full sync completed without company-level failures")
        sync_market_data(conn, settings, companies, force=True)
        publish_announcement(
            settings,
            conn,
            "Full Sync Batch Completed" if (limit or resume or ticker) else "Full Sync Completed",
            all_updated_companies,
            all_filings,
        )
        set_state(conn, "last_full_sync_utc", full_sync_at)
        conn.commit()
    build_web_data(settings)


def run_daily_update(
    settings: dict[str, Any],
    ticker: str | None = None,
    limit: int | None = None,
    force: bool = False,
    resume: bool = False,
) -> None:
    db = Database(Path(settings["sqlitePath"]))
    daily_run_at = utc_now_iso()
    with db.connect() as conn:
        companies = get_companies_from_db(conn)
        if not companies:
            companies = resolve_companies(settings)
            upsert_companies(conn, companies)
        companies = filter_companies(companies, ticker, None)
        companies, resumed_from = apply_resume_checkpoint(conn, companies, resume, ticker)
        companies = filter_companies(companies, None, limit)

        existing_accessions = get_existing_accessions(conn)
        updated_companies: list[dict[str, Any]] = []
        new_filings: list[dict[str, Any]] = []
        failed_companies: list[dict[str, Any]] = []

        total = len(companies)
        if resumed_from:
            append_log(settings, f"Daily update resumed after {resumed_from}; remaining {total} companies")
        elif resume:
            append_log(settings, "Daily update resume requested but no checkpoint was found; starting from the beginning")
        append_log(settings, f"Daily update started for {total} companies")
        for index, company in enumerate(companies, start=1):
            try:
                append_log(settings, f"Daily update [{index}/{total}] {company['ticker']} {company['cik']}")
                filings = get_all_submissions(company, settings, force=force)
                company_new_filings = [row for row in filings if row["accessionNumber"] not in existing_accessions]
                if not company_new_filings:
                    set_state(conn, "last_processed_ticker", company["ticker"])
                    set_state(conn, "last_processed_cik", company["cik"])
                    set_state(conn, "last_processed_step", f"{index}/{total}")
                    conn.commit()
                    continue
                for filing in company_new_filings:
                    existing_accessions.add(filing["accessionNumber"])
                upsert_filings(conn, company_new_filings)
                facts = get_company_facts(company, settings, force=True)
                annuals = build_annual_financials(company, facts, settings)
                quarterlies = build_quarterly_financials(company, facts, settings)
                replace_annuals_for_company(conn, company, annuals)
                replace_quarterlies_for_company(conn, company, quarterlies)
                set_state(conn, "last_processed_ticker", company["ticker"])
                set_state(conn, "last_processed_cik", company["cik"])
                set_state(conn, "last_processed_step", f"{index}/{total}")
                conn.commit()
                updated_companies.append(company)
                new_filings.extend(company_new_filings)
            except Exception as exc:
                conn.rollback()
                append_log(settings, f"Daily update failed for {company['ticker']} {company['cik']}: {exc}")
                failed_companies.append({"ticker": company["ticker"], "cik": company["cik"], "error": str(exc)})
                continue

        export_json_snapshots(conn, settings)
        set_state(conn, "last_daily_run_utc", daily_run_at)
        set_state(conn, "last_daily_failed_count", str(len(failed_companies)))
        if failed_companies:
            failed_path = Path(settings["dataRootResolved"]) / "logs" / f"daily-update-failed-{utc_stamp()}.json"
            write_json(failed_path, failed_companies)
            append_log(settings, f"Daily update completed with {len(failed_companies)} failed companies. Details: {failed_path}")
        sync_market_data(conn, settings, companies, force=True)
        publish_announcement(
            settings,
            conn,
            ("Daily SEC Update Batch" if (limit or resume or ticker) else "Daily SEC Update") if new_filings else ("Daily SEC Update Batch - No Changes" if (limit or resume or ticker) else "Daily SEC Update - No Changes"),
            updated_companies,
            new_filings,
        )
        conn.commit()
    build_web_data(settings)


def refresh_official_guidance(
    settings: dict[str, Any],
    ticker: str | None = None,
    universe: str | None = None,
    limit: int | None = None,
    force: bool = False,
) -> None:
    db = Database(Path(settings["sqlitePath"]))
    with db.connect() as conn:
        companies = get_companies_from_db(conn)
        companies = filter_companies_by_universe(companies, universe)
        companies = filter_companies(companies, ticker, limit)
        if not companies:
            print_json({"guidanceForecastsRefreshed": 0, "companies": 0})
            return

        annuals_by_ticker: dict[str, list[dict[str, Any]]] = {}
        quarterlies_by_ticker: dict[str, list[dict[str, Any]]] = {}
        for row in rows_to_dicts(conn.execute("SELECT * FROM annual_financials ORDER BY ticker, fiscal_year")):
            annuals_by_ticker.setdefault(row["ticker"], []).append(row)
        for row in rows_to_dicts(conn.execute("SELECT * FROM quarterly_financials ORDER BY ticker, fiscal_year, fiscal_period")):
            quarterlies_by_ticker.setdefault(row["ticker"], []).append(row)

        manual_forecasts = load_profit_forecasts(settings)
        existing_forecasts = rows_to_dicts(conn.execute("SELECT * FROM profit_forecasts"))
        target_tickers = {company["ticker"] for company in companies}
        preserved_official_rows = [
            row
            for row in existing_forecasts
            if row.get("source_type") == "official-guidance" and row.get("ticker") not in target_tickers
        ]
        official_rows: list[dict[str, Any]] = []

        total = len(companies)
        append_log(settings, f"Official guidance refresh started for {total} companies")
        for index, company in enumerate(companies, start=1):
            append_log(settings, f"Official guidance [{index}/{total}] {company['ticker']} {company['cik']}")
            annuals = annuals_by_ticker.get(company["ticker"], [])
            quarterlies = quarterlies_by_ticker.get(company["ticker"], [])
            balance_sheet_row = select_latest_balance_sheet_row(quarterlies, annuals)
            shares_outstanding = balance_sheet_row.get("shares_outstanding") if balance_sheet_row else None
            forecasts = extract_official_guidance_forecasts(company, settings, shares_outstanding, force=force)
            official_rows.extend(forecasts)

        merged_rows = merge_profit_forecasts(manual_forecasts, preserved_official_rows + official_rows)
        upsert_profit_forecasts(conn, merged_rows)
        conn.commit()
        metric_counts: dict[str, int] = {}
        for row in official_rows:
            metric_counts[row["metric"]] = metric_counts.get(row["metric"], 0) + 1
        append_log(settings, f"Official guidance refresh completed with {len(official_rows)} extracted forecasts")
        print_json({"guidanceForecastsRefreshed": len(official_rows), "metrics": metric_counts, "companies": total, "storedForecasts": len(merged_rows)})


def register_task(python_exe: str, script_path: str, daily_time: str) -> None:
    task_name = "StockSecDailyUpdatePython"
    command = f'"{python_exe}" "{script_path}" daily-update'
    subprocess.run(
        [
            "schtasks",
            "/Create",
            "/SC",
            "DAILY",
            "/TN",
            task_name,
            "/TR",
            command,
            "/ST",
            daily_time,
            "/F",
        ],
        check=True,
    )


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="S&P 500 SEC database pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    full_sync = subparsers.add_parser("full-sync")
    full_sync.add_argument("--ticker")
    full_sync.add_argument("--limit", type=int)
    full_sync.add_argument("--force", action="store_true")
    full_sync.add_argument("--resume", action="store_true")
    full_sync.add_argument("--refresh-universe", action="store_true")

    daily_update = subparsers.add_parser("daily-update")
    daily_update.add_argument("--ticker")
    daily_update.add_argument("--limit", type=int)
    daily_update.add_argument("--force", action="store_true")
    daily_update.add_argument("--resume", action="store_true")

    subparsers.add_parser("status")

    query_sql = subparsers.add_parser("query-sql")
    query_sql.add_argument("sql")
    subparsers.add_parser("sync-progress")

    export_csv = subparsers.add_parser("export-csv")
    export_csv.add_argument("--table", required=True)
    export_csv.add_argument("--output", required=True)
    export_csv.add_argument("--where")
    export_csv.add_argument("--order-by")
    export_csv.add_argument("--limit", type=int)

    subparsers.add_parser("build-web-data")
    subparsers.add_parser("refresh-companies")
    refresh_guidance = subparsers.add_parser("refresh-official-guidance")
    refresh_guidance.add_argument("--ticker")
    refresh_guidance.add_argument("--universe")
    refresh_guidance.add_argument("--limit", type=int)
    refresh_guidance.add_argument("--force", action="store_true")
    stage_checkpoint = subparsers.add_parser("stage-universe-checkpoint")
    stage_checkpoint.add_argument("--limit", type=int)
    market_sync = subparsers.add_parser("sync-market-data")
    market_sync.add_argument("--ticker")
    market_sync.add_argument("--limit", type=int)
    market_sync.add_argument("--force", action="store_true")
    recompute = subparsers.add_parser("recompute-financials")
    recompute.add_argument("--ticker")
    recompute.add_argument("--limit", type=int)

    rebuild = subparsers.add_parser("rebuild-db-from-cache")
    rebuild.add_argument("--output", required=True)

    register = subparsers.add_parser("register-task")
    register.add_argument("--daily-time", default="08:00")
    register.add_argument("--python-exe", default=sys.executable)

    args = parser.parse_args(argv)
    if args.command == "full-sync":
        run_full_sync(
            settings,
            ticker=args.ticker,
            limit=args.limit,
            force=args.force,
            resume=args.resume,
            refresh_universe=args.refresh_universe,
        )
        return 0
    if args.command == "daily-update":
        run_daily_update(settings, ticker=args.ticker, limit=args.limit, force=args.force, resume=args.resume)
        return 0
    if args.command == "status":
        run_status(settings)
        return 0
    if args.command == "query-sql":
        run_query_sql(settings, args.sql)
        return 0
    if args.command == "sync-progress":
        run_sync_progress(settings)
        return 0
    if args.command == "export-csv":
        run_export_csv(settings, args.table, args.output, args.where, args.order_by, args.limit)
        return 0
    if args.command == "build-web-data":
        build_web_data(settings)
        return 0
    if args.command == "refresh-companies":
        refresh_companies(settings)
        return 0
    if args.command == "refresh-official-guidance":
        refresh_official_guidance(settings, ticker=args.ticker, universe=args.universe, limit=args.limit, force=args.force)
        return 0
    if args.command == "stage-universe-checkpoint":
        stage_universe_checkpoint(settings, limit=args.limit)
        return 0
    if args.command == "sync-market-data":
        run_market_data_sync(settings, ticker=args.ticker, limit=args.limit, force=args.force)
        return 0
    if args.command == "recompute-financials":
        recompute_financials(settings, ticker=args.ticker, limit=args.limit)
        return 0
    if args.command == "rebuild-db-from-cache":
        rebuild_db_from_cache(settings, args.output)
        return 0
    if args.command == "register-task":
        register_task(args.python_exe, str(Path(__file__).resolve()), args.daily_time)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
