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
    settings.setdefault("sqlitePath", str((data_root / "db" / "stock_sec.db").resolve()))
    settings.setdefault("formsToTrack", ["10-K", "10-K/A", "10-KT", "10-Q", "10-Q/A"])
    settings.setdefault("lookbackYears", 10)
    settings.setdefault("marketDataBaseUrl", "https://stooq.com/q/l/")
    settings.setdefault("marketDataSource", "stooq")
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
            }
        )
    return companies


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


def html_unescape(value: str) -> str:
    return html.unescape(value)


def get_sp500_constituents(settings: dict[str, Any]) -> list[dict[str, Any]]:
    return parse_sp500_constituents(fetch_text(settings["sp500Source"], settings))


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
            }
        )
    return items


def resolve_companies(settings: dict[str, Any]) -> list[dict[str, Any]]:
    data_root = Path(settings["dataRootResolved"])
    db_root = data_root / "db"
    ticker_map = {item["ticker"]: item for item in get_sec_ticker_map(settings)}
    companies: list[dict[str, Any]] = []
    seen_tickers: set[str] = set()

    constituents = get_sp500_constituents(settings)
    constituents.extend(get_additional_companies(settings))

    for row in constituents:
        if row["ticker"] in seen_tickers:
            continue
        match = ticker_map.get(row["ticker"])
        if not match and "-" in row["ticker"]:
            match = ticker_map.get(row["ticker"].replace("-", "."))

        resolved_cik = match["cik"] if match else (cik10(row["cikFromSource"]) if str(row["cikFromSource"]).isdigit() else None)
        if not resolved_cik:
            continue

        companies.append(
            {
                "ticker": row["ticker"],
                "cik": resolved_cik,
                "name": match["title"] if match else row["security"],
                "security": row["security"],
                "sector": row["sector"],
                "subIndustry": row["subIndustry"],
                "headquarters": row["headquarters"],
                "dateAdded": row["dateAdded"],
            }
        )
        seen_tickers.add(row["ticker"])

    write_json(db_root / "companies.json", companies)
    write_json(db_root / "sp500_constituents.json", companies)
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


def get_all_submissions(company: dict[str, Any], settings: dict[str, Any], force: bool = False) -> list[dict[str, Any]]:
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
    tracked_forms = set(settings["formsToTrack"])
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

    if not raw or raw.endswith("N/D,N/D,N/D,N/D,N/D,N/D,N/D,N/D"):
        return None

    parts = [item.strip() for item in raw.split(",")]
    if len(parts) < 8:
        raise RuntimeError(f"Unexpected market quote payload for {ticker}: {raw}")

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
    annual_forms = {"10-K", "10-K/A", "10-KT"}
    entries = list(entries)
    candidates = []
    for entry in entries:
        if entry.get("fy") != fiscal_year or entry.get("form") not in annual_forms or entry.get("frame"):
            continue
        if period_type == "duration" and not is_annual_duration(entry):
            continue
        candidates.append(entry)
    if not candidates:
        for entry in entries:
            if entry.get("fy") != fiscal_year or entry.get("form") not in annual_forms:
                continue
            if period_type == "duration" and not is_annual_duration(entry):
                continue
            candidates.append(entry)
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: ((item.get("filed") or ""), (item.get("end") or "")), reverse=True)[0]


def select_quarterly_fact(entries: Iterable[dict[str, Any]], fiscal_year: int, period_type: str) -> dict[str, Any] | None:
    quarterly_forms = {"10-Q", "10-Q/A"}
    entries = list(entries)
    candidates = []
    for entry in entries:
        if entry.get("fy") != fiscal_year or entry.get("form") not in quarterly_forms or entry.get("frame"):
            continue
        if period_type == "duration" and not is_quarterly_duration(entry):
            continue
        candidates.append(entry)
    if not candidates:
        for entry in entries:
            if entry.get("fy") != fiscal_year or entry.get("form") not in quarterly_forms:
                continue
            if period_type == "duration" and not is_quarterly_duration(entry):
                continue
            candidates.append(entry)
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: (item.get("filed") or "", item.get("end") or ""), reverse=True)[0]


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
            "FreeCashFlow": None,
            "DilutedEPS": None,
            "SharesOutstanding": None,
            "sourceFiledDate": None,
            "sourceForm": None,
        }

        for metric_name, concepts in CONCEPT_MAP.items():
            selected = None
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
    date_added TEXT
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
CREATE INDEX IF NOT EXISTS idx_filings_ticker_date ON filings (ticker, filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_filings_form_date ON filings (form, filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_annual_financials_ticker_year ON annual_financials (ticker, fiscal_year DESC);
CREATE INDEX IF NOT EXISTS idx_quarterly_financials_ticker_period ON quarterly_financials (ticker, fiscal_year DESC, fiscal_period DESC);
CREATE INDEX IF NOT EXISTS idx_market_quotes_date ON market_quotes (quote_date DESC);
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
        conn.row_factory = sqlite3.Row
        return conn


def upsert_companies(conn: sqlite3.Connection, companies: list[dict[str, Any]]) -> None:
    conn.executemany(
        """
        INSERT INTO companies (ticker, cik, name, security, sector, sub_industry, headquarters, date_added)
        VALUES (:ticker, :cik, :name, :security, :sector, :subIndustry, :headquarters, :dateAdded)
        ON CONFLICT(ticker) DO UPDATE SET
            cik = excluded.cik,
            name = excluded.name,
            security = excluded.security,
            sector = excluded.sector,
            sub_industry = excluded.sub_industry,
            headquarters = excluded.headquarters,
            date_added = excluded.date_added
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
            operating_cash_flow, capital_expenditure, free_cash_flow, diluted_eps,
            shares_outstanding, source_filed_date, source_form
        ) VALUES (
            :cik, :ticker, :companyName, :fiscalYear, :Revenue, :NetIncome, :OperatingIncome,
            :TotalAssets, :TotalLiabilities, :ShareholdersEquity, :CashAndEquivalents,
            :OperatingCashFlow, :CapitalExpenditure, :FreeCashFlow, :DilutedEPS,
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
            capital_expenditure, free_cash_flow, diluted_eps, shares_outstanding, source_filed_date, source_form
        ) VALUES (
            :cik, :ticker, :companyName, :fiscalYear, :fiscalPeriod, :periodEnd, :Revenue, :NetIncome, :OperatingIncome,
            :TotalAssets, :TotalLiabilities, :ShareholdersEquity, :CashAndEquivalents, :OperatingCashFlow,
            :CapitalExpenditure, :FreeCashFlow, :DilutedEPS, :SharesOutstanding, :sourceFiledDate, :sourceForm
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
        SELECT ticker, cik, name, security, sector, sub_industry, headquarters, date_added
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
        counts = {
            "companies": conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
            "filings": conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0],
            "annualFinancials": conn.execute("SELECT COUNT(*) FROM annual_financials").fetchone()[0],
            "quarterlyFinancials": conn.execute("SELECT COUNT(*) FROM quarterly_financials").fetchone()[0],
            "marketQuotes": conn.execute("SELECT COUNT(*) FROM market_quotes").fetchone()[0],
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
        companies = rows_to_dicts(
            conn.execute(
                """
                SELECT ticker, cik, name, security, sector, sub_industry, headquarters, date_added
                FROM companies
                ORDER BY ticker
                """
            )
        )
        annuals = rows_to_dicts(conn.execute("SELECT * FROM annual_financials ORDER BY ticker, fiscal_year"))
        quarterlies = rows_to_dicts(conn.execute("SELECT * FROM quarterly_financials ORDER BY ticker, fiscal_year, fiscal_period"))
        filings = rows_to_dicts(conn.execute("SELECT * FROM filings ORDER BY filing_date DESC, accession_number DESC"))
        market_quotes = rows_to_dicts(conn.execute("SELECT * FROM market_quotes ORDER BY ticker"))

    annuals_by_ticker: dict[str, list[dict[str, Any]]] = {}
    quarterlies_by_ticker: dict[str, list[dict[str, Any]]] = {}
    filings_by_ticker: dict[str, list[dict[str, Any]]] = {}
    market_quotes_by_ticker = {row["ticker"]: row for row in market_quotes}

    for row in annuals:
        annuals_by_ticker.setdefault(row["ticker"], []).append(row)
    for row in quarterlies:
        quarterlies_by_ticker.setdefault(row["ticker"], []).append(row)
    for row in filings:
        filings_by_ticker.setdefault(row["ticker"], []).append(row)

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
            "latestAnnual": latest_annual,
            "latestQuarter": latest_quarter,
            "latestFiling": latest_filing,
            "marketData": market_data,
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

    top_revenue = sorted(summaries, key=lambda item: item["latestAnnual"]["revenue"] if item.get("latestAnnual") and item["latestAnnual"].get("revenue") is not None else 0, reverse=True)[:10]
    top_profit = sorted(
        [item for item in summaries if item.get("latestAnnual") and item["latestAnnual"].get("net_income") is not None],
        key=lambda item: item["latestAnnual"]["net_income"],
        reverse=True,
    )[:10]
    top_market_cap = sorted(
        [item for item in summaries if item.get("marketData") and item["marketData"].get("marketCap") is not None],
        key=lambda item: item["marketData"]["marketCap"],
        reverse=True,
    )[:10]
    latest_filings = sorted(
        [item for item in summaries if item.get("latestFiling") and item["latestFiling"].get("filing_date")],
        key=lambda item: item["latestFiling"]["filing_date"],
        reverse=True,
    )[:12]

    payload = {
        "generatedAtUtc": utc_now_iso(),
        "companyCount": len(summaries),
        "companies": summaries,
        "sectors": sectors,
        "highlights": {
            "topRevenue": top_revenue,
            "topProfit": top_profit,
            "topMarketCap": top_market_cap,
            "latestFilings": latest_filings,
        },
    }
    write_json(web_root / "summary.json", payload)
    print_json({"output": str(web_root), "companyCount": len(summaries)})


def refresh_companies(settings: dict[str, Any]) -> None:
    db = Database(Path(settings["sqlitePath"]))
    companies = resolve_companies(settings)
    with db.connect() as conn:
        upsert_companies(conn, companies)
        sync_market_data(conn, settings, companies, force=True)
        export_json_snapshots(conn, settings)
        conn.commit()
    build_web_data(settings)
    print_json({"companiesRefreshed": len(companies)})


def recompute_financials(settings: dict[str, Any], ticker: str | None = None, limit: int | None = None) -> None:
    db = Database(Path(settings["sqlitePath"]))
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
        build_web_data(settings)
        set_state(conn, "last_recompute_utc", utc_now_iso())
        conn.commit()
    print_json({"recomputedCompanies": total})


def run_market_data_sync(settings: dict[str, Any], ticker: str | None = None, limit: int | None = None, force: bool = False) -> None:
    db = Database(Path(settings["sqlitePath"]))
    with db.connect() as conn:
        companies = get_companies_from_db(conn)
        if not companies:
            companies = resolve_companies(settings)
            upsert_companies(conn, companies)
        companies = filter_companies(companies, ticker, limit)
        refreshed = sync_market_data(conn, settings, companies, force=force)
        export_json_snapshots(conn, settings)
        build_web_data(settings)
        set_state(conn, "last_market_sync_utc", utc_now_iso())
        conn.commit()
    print_json({"marketQuotesRefreshed": refreshed, "companies": len(companies)})


def rebuild_db_from_cache(settings: dict[str, Any], output_path: str) -> None:
    target_path = Path(output_path)
    if not target_path.is_absolute():
        target_path = (PROJECT_ROOT / target_path).resolve()

    if target_path.exists():
        raise RuntimeError(f"Target database already exists: {target_path}")

    companies = load_companies_snapshot(settings)
    target_db = Database(target_path)
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
        build_web_data({**settings, "sqlitePath": str(target_path)})
        set_state(conn, "last_rebuild_utc", utc_now_iso())
        conn.commit()
    print_json({"rebuiltDatabase": str(target_path), "companies": len(companies)})


def filter_companies(companies: list[dict[str, Any]], ticker: str | None, limit: int | None) -> list[dict[str, Any]]:
    if ticker:
        requested = {normalize_ticker(item) for item in ticker.split(",")}
        requested.discard(None)
        companies = [company for company in companies if company["ticker"] in requested]
    if limit:
        companies = companies[:limit]
    return companies


def run_full_sync(settings: dict[str, Any], ticker: str | None = None, limit: int | None = None, force: bool = False) -> None:
    db = Database(Path(settings["sqlitePath"]))
    with db.connect() as conn:
        companies = filter_companies(resolve_companies(settings), ticker, limit)
        upsert_companies(conn, companies)
        all_filings: list[dict[str, Any]] = []
        all_updated_companies: list[dict[str, Any]] = []
        failed_companies: list[dict[str, Any]] = []
        total = len(companies)
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
        build_web_data(settings)
        publish_announcement(settings, conn, "Full Sync Completed", all_updated_companies, all_filings)
        conn.commit()


def run_daily_update(settings: dict[str, Any], ticker: str | None = None, limit: int | None = None, force: bool = False) -> None:
    db = Database(Path(settings["sqlitePath"]))
    with db.connect() as conn:
        companies = get_companies_from_db(conn)
        if not companies:
            companies = resolve_companies(settings)
            upsert_companies(conn, companies)
        companies = filter_companies(companies, ticker, limit)

        existing_accessions = get_existing_accessions(conn)
        updated_companies: list[dict[str, Any]] = []
        new_filings: list[dict[str, Any]] = []
        failed_companies: list[dict[str, Any]] = []

        total = len(companies)
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
        set_state(conn, "last_daily_run_utc", utc_now_iso())
        set_state(conn, "last_daily_failed_count", str(len(failed_companies)))
        if failed_companies:
            failed_path = Path(settings["dataRootResolved"]) / "logs" / f"daily-update-failed-{utc_stamp()}.json"
            write_json(failed_path, failed_companies)
            append_log(settings, f"Daily update completed with {len(failed_companies)} failed companies. Details: {failed_path}")
        sync_market_data(conn, settings, companies, force=True)
        build_web_data(settings)
        publish_announcement(
            settings,
            conn,
            "Daily SEC Update" if new_filings else "Daily SEC Update - No Changes",
            updated_companies,
            new_filings,
        )
        conn.commit()


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

    daily_update = subparsers.add_parser("daily-update")
    daily_update.add_argument("--ticker")
    daily_update.add_argument("--limit", type=int)
    daily_update.add_argument("--force", action="store_true")

    subparsers.add_parser("status")

    query_sql = subparsers.add_parser("query-sql")
    query_sql.add_argument("sql")

    export_csv = subparsers.add_parser("export-csv")
    export_csv.add_argument("--table", required=True)
    export_csv.add_argument("--output", required=True)
    export_csv.add_argument("--where")
    export_csv.add_argument("--order-by")
    export_csv.add_argument("--limit", type=int)

    subparsers.add_parser("build-web-data")
    subparsers.add_parser("refresh-companies")
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
        run_full_sync(settings, ticker=args.ticker, limit=args.limit, force=args.force)
        return 0
    if args.command == "daily-update":
        run_daily_update(settings, ticker=args.ticker, limit=args.limit, force=args.force)
        return 0
    if args.command == "status":
        run_status(settings)
        return 0
    if args.command == "query-sql":
        run_query_sql(settings, args.sql)
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
