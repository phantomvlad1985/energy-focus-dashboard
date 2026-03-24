"""
Energy Focus — FastAPI Backend (Standalone for Render.com)
Serves live data endpoints for the European Gas Dashboard.
All data fetching is self-contained — no local module imports.

Run:  uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests as http_requests
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logging.basicConfig(level="INFO", format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")
logger = logging.getLogger("api")

# ─── Config ────────────────────────────────────────────────────────────────
GIE_API_KEY = os.getenv("GIE_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

app = FastAPI(
    title="Energy Focus API",
    description="Live data API for energy-focus.org European Gas Dashboard",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─── In-Memory Cache ────────────────────────────────────────────────────────
_cache: dict = {}
CACHE_TTL = {
    "flows": timedelta(hours=6),
    "storage": timedelta(hours=12),
    "ttf": timedelta(hours=6),
    "lng": timedelta(hours=12),
    "oil": timedelta(hours=6),
    "brief": timedelta(hours=24),
}


def _get_cached(key: str) -> Optional[dict]:
    if key in _cache:
        ts, data = _cache[key]
        ttl = CACHE_TTL.get(key, timedelta(hours=1))
        if datetime.now() - ts < ttl:
            return data
    return None


def _set_cache(key: str, data):
    _cache[key] = (datetime.now(), data)


# ─── FLOWS ENDPOINT (ENTSOG Transparency Platform) ─────────────────────────
@app.get("/api/flows")
async def get_flows(
    date: str = Query(default="latest", description="Date YYYY-MM-DD or 'latest'"),
):
    """Cross-border physical gas flow data from ENTSOG."""
    cached = _get_cached("flows")
    if cached:
        return cached

    try:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

        url = (
            "https://transparency.entsog.eu/api/v1/operationaldata.json"
            f"?from={start}&to={end}"
            "&indicator=Physical+Flow"
            "&periodType=day"
            "&limit=5000"
        )
        resp = http_requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        operators = data.get("operationaldata", [])
        if not operators:
            return {"date": end, "source": "entsog", "pair_flows": [], "record_count": 0}

        records = []
        for op in operators:
            val = op.get("value")
            if val is not None:
                records.append({
                    "operator_key": op.get("operatorKey", ""),
                    "operator_label": op.get("operatorLabel", ""),
                    "direction": op.get("directionKey", ""),
                    "point_key": op.get("pointKey", ""),
                    "point_label": op.get("pointLabel", ""),
                    "flow_value": float(val),
                    "unit": op.get("unit", "kWh/d"),
                    "period_from": op.get("periodFrom", ""),
                })

        # Aggregate by operator
        df = pd.DataFrame(records)
        if not df.empty and "flow_value" in df.columns:
            agg = (
                df.groupby(["operator_key", "direction"])["flow_value"]
                .sum()
                .reset_index()
            )
            agg["flow_gwh_d"] = agg["flow_value"] / 1e6
            pair_flows = agg[["operator_key", "direction", "flow_gwh_d"]].to_dict(orient="records")
        else:
            pair_flows = []

        result = {
            "date": end,
            "source": "entsog",
            "pair_flows": pair_flows,
            "record_count": len(records),
            "fetched_at": datetime.now().isoformat(),
        }
        _set_cache("flows", result)
        return result

    except Exception as e:
        logger.error(f"Flows fetch failed: {e}")
        raise HTTPException(status_code=502, detail=f"ENTSOG unavailable: {str(e)}")


# ─── STORAGE ENDPOINT (GIE AGSI+) ──────────────────────────────────────────
@app.get("/api/storage")
async def get_storage(
    country: Optional[str] = Query(default=None, description="Country code (e.g. DE)"),
    days: int = Query(default=30, description="Number of days of history"),
):
    """EU gas storage levels from GIE AGSI+."""
    cache_key = f"storage_{country or 'all'}_{days}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    try:
        headers = {"x-key": GIE_API_KEY}
        base_url = "https://agsi.gie.eu/api"

        if country:
            url = f"{base_url}?country={country}&size=30"
        else:
            url = f"{base_url}?size=30"

        resp = http_requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        entries = data.get("data", data) if isinstance(data, dict) else data
        if not entries:
            return {"date": datetime.now().strftime("%Y-%m-%d"), "source": "agsi", "data": []}

        records = []
        for e in entries:
            try:
                records.append({
                    "date": e.get("gasDayStart", ""),
                    "country": e.get("code", e.get("name", "")),
                    "country_name": e.get("name", ""),
                    "fill_pct": float(e.get("full", 0) or 0),
                    "gas_in_storage_twh": float(e.get("gasInStorage", 0) or 0),
                    "working_volume_twh": float(e.get("workingGasVolume", 0) or 0),
                    "injection_twh": float(e.get("injection", 0) or 0),
                    "withdrawal_twh": float(e.get("withdrawal", 0) or 0),
                })
            except (ValueError, TypeError):
                continue

        summary = records[:20]

        result = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "source": "agsi",
            "summary": summary,
            "timeseries": records,
            "fetched_at": datetime.now().isoformat(),
        }
        _set_cache(cache_key, result)
        return result

    except Exception as e:
        logger.error(f"Storage fetch failed: {e}")
        raise HTTPException(status_code=502, detail=f"AGSI+ unavailable: {str(e)}")


# ─── TTF PRICES ENDPOINT ────────────────────────────────────────────────────
@app.get("/api/prices/ttf")
async def get_ttf_prices(
    days: int = Query(default=90, description="Number of days of history"),
):
    """TTF natural gas futures prices (via Yahoo Finance)."""
    cached = _get_cached("ttf")
    if cached:
        return cached

    try:
        import yfinance as yf
        ticker = yf.Ticker("TTF=F")
        hist = ticker.history(period=f"{days}d")

        if hist.empty:
            return {"source": "yfinance", "data": [], "note": "No TTF data returned"}

        hist = hist.reset_index()
        records = []
        for _, row in hist.iterrows():
            records.append({
                "date": row["Date"].strftime("%Y-%m-%d"),
                "open": round(float(row["Open"]), 2),
             "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row.get("Volume", 0)),
            })

        latest = records[-1] if records else {}
        result = {
            "source": "yfinance",
            "ticker": "TTF=F",
            "unit": "EUR/MWh",
            "latest": latest,
            "timeseries": records,
            "fetched_at": datetime.now().isoformat(),
        }
        _set_cache("ttf", result)
        return result

    except Exception as e:
        logger.error(f"TTF fetch failed: {e}")
        raise HTTPException(status_code=502, detail=f"TTF data unavailable: {str(e)}")


# ─── OIL PRICES ENDPOINT ────────────────────────────────────────────────────
@app.get("/api/prices/oil")
async def get_oil_prices(
    days: int = Query(default=90, description="Number of days of history"),
):
    """Crude oil (Brent, WTI) prices via Yahoo Finance."""
    cached = _get_cached("oil")
    if cached:
        return cached

    try:
        import yfinance as yf
        tickers = {"BZ=F": "Brent Crude", "CL=F": "WTI Crude"}
        all_records = []

        for symbol, name in tickers.items():
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period=f"{days}d")
                if not hist.empty:
                    hist = hist.reset_index()
                    for _, row in hist.iterrows():
                        all_records.append({
                            "date": row["Date"].strftime("%Y-%m-%d"),
                            "product": name,
                            "open": round(float(row["Open"]), 2),
                            "high": round(float(row["High"]), 2),
                            "low": round(float(row["Low"]), 2),
                            "close": round(float(row["Close"]), 2),
                            "volume": int(row.get("Volume", 0)),
                        })
            except Exception as e:
                logger.warning(f"Oil ticker {symbol} failed: {e}")

        result = {
            "source": "yfinance",
            "unit": "USD/bbl",
            "timeseries": all_records,
            "fetched_at": datetime.now().isoformat(),
        }
        _set_cache("oil", result)
        return result

    except Exception as e:
        logger.error(f"Oil fetch failed: {e}")
        raise HTTPException(status_code=502, detail=f"Oil data unavailable: {str(e)}")


# ─── LNG TERMINALS ENDPOINT (GIE ALSI) ─────────────────────────────────────
@app.get("/api/lng")
async def get_lng(
    days: int = Query(default=30, description="Number of days of history"),
):
    """EU LNG terminal data from GIE ALSI."""
    cached = _get_cached("lng")
    if cached:
        return cached

    try:
        headers = {"x-key": GIE_API_KEY}
        url = "https://alsi.gie.eu/api?size=30"
        resp = http_requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        entries = data.get("data", data) if isinstance(data, dict) else data
        records = []
        for e in entries:
            try:
                records.append({
                    "date": e.get("gasDayStart", ""),
                    "country": e.get("code", ""),
                    "name": e.get("name", ""),
                    "dtrs": float(e.get("dtrs", 0) or 0),
                    "send_out": float(e.get("sendOut", 0) or 0),
                    "inventory": float(e.get("inventory", 0) or 0),
                })
            except (ValueError, TypeError):
                continue

        result = {
            "source": "alsi",
            "timeseries": records,
            "fetched_at": datetime.now().isoformat(),
        }
        _set_cache("lng", result)
        return result

    except Exception as e:
        logger.error(f"LNG fetch failed: {e}")
        raise HTTPException(status_code=502, detail=f"ALSI unavailable: {str(e)}")


# ─── DAILY BRIEF ENDPOINT ───────────────────────────────────────────────────
@app.get("/api/brief")
async def get_daily_brief():
    """AI-generated daily market brief (cached 24h)."""
    cached = _get_cached("brief")
    if cached:
        return cached

    try:
        from api.daily_brief import generate_daily_brief
        brief = await generate_daily_brief()
        _set_cache("brief", brief)
        return brief
    except Exception as e:
        logger.error(f"Daily brief generation failed: {e}")
        raise HTTPException(status_code=502, detail=f"Brief unavailable: {str(e)}")


# ─── HEALTH CHECK ───────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "cache_keys": list(_cache.keys()),
        "uptime_check": True,
    }
