"""
Daily Brief Generator — Energy Focus (Standalone for Render.com)
Fetches latest energy market data, scrapes headlines from public RSS feeds,
then uses the Anthropic Claude API to generate a 200-300 word market summary.
"""

import os
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import requests

logger = logging.getLogger("daily_brief")

# ─── Config from environment ───────────────────────────────────────────────
GIE_API_KEY = os.getenv("GIE_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# ─── RSS Feed Sources ──────────────────────────────────────────────────────
RSS_FEEDS = [
    {"name": "Reuters Energy", "url": "https://www.reutersagency.com/feed/?best-topics=energy&post_type=best"},
    {"name": "Platts/S&P Global", "url": "https://www.spglobal.com/commodityinsights/en/rss-feed/natural-gas"},
    {"name": "ICIS Heren", "url": "https://www.icis.com/explore/resources/news/energy/feed/"},
    {"name": "Montel News", "url": "https://www.montelnews.com/rss"},
    {"name": "Energy Intelligence", "url": "https://www.energyintel.com/rss"},
    {"name": "Natural Gas World", "url": "https://www.naturalgasworld.com/rss"},
]

ENTSOG_URGENT_MARKET_MSG = (
    "https://transparency.entsog.eu/api/v1/urgentmarketmessages.json"
    "?from={from_date}&to={to_date}&limit=20"
)


def _fetch_rss_headlines(max_per_feed: int = 5) -> list:
    """Fetch latest headlines from RSS feeds."""
    headlines = []
    for feed in RSS_FEEDS:
        try:
            resp = requests.get(feed["url"], timeout=15, headers={
                "User-Agent": "EnergyFocus/1.0 (market-brief-bot)"
            })
            if resp.status_code != 200:
                continue

            root = ET.fromstring(resp.text)
            items = root.findall(".//item") or root.findall(
                ".//{http://www.w3.org/2005/Atom}entry"
            )
            for item in items[:max_per_feed]:
                title = (
                    item.findtext("title")
                    or item.findtext("{http://www.w3.org/2005/Atom}title")
                    or ""
                )
                link = (
                    item.findtext("link")
                    or (item.find("{http://www.w3.org/2005/Atom}link") or {}).get("href", "")
                    or ""
                )
                pub = (
                    item.findtext("pubDate")
                    or item.findtext("{http://www.w3.org/2005/Atom}published")
                    or ""
                )
                if title.strip():
                    headlines.append({
                        "source": feed["name"],
                        "title": title.strip(),
                        "published": pub.strip(),
                        "link": link.strip(),
                    })
        except Exception as e:
            logger.warning(f"RSS {feed['name']} failed: {e}")
            continue

    logger.info(f"Fetched {len(headlines)} headlines from {len(RSS_FEEDS)} feeds")
    return headlines


def _fetch_entsog_urgent_messages() -> list:
    """Fetch ENTSOG Urgent Market Messages (last 3 days)."""
    try:
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        url = ENTSOG_URGENT_MARKET_MSG.format(from_date=from_date, to_date=to_date)
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            messages = data if isinstance(data, list) else data.get("urgentMarketMessages", [])
            return [
                f"[{m.get('messageType', 'UMM')}] {m.get('messageText', '')[:200]}"
                for m in messages[:10]
                if m.get("messageText")
            ]
    except Exception as e:
        logger.warning(f"ENTSOG UMM fetch failed: {e}")
    return []


def _fetch_market_snapshot() -> dict:
    """Gather latest market data points for context."""
    snapshot = {}

    # TTF price via yfinance
    try:
        import yfinance as yf
        ticker = yf.Ticker("TTF=F")
        hist = ticker.history(period="7d")
        if not hist.empty:
            latest = hist.iloc[-1]
            prev = hist.iloc[-2] if len(hist) > 1 else latest
            snapshot["ttf"] = {
                "price": round(float(latest["Close"]), 2),
                "change": round(float(latest["Close"] - prev["Close"]), 2),
                "unit": "EUR/MWh",
            }
    except Exception as e:
        logger.warning(f"TTF snapshot failed: {e}")

    # Oil prices via yfinance
    try:
        import yfinance as yf
        for symbol, name in {"BZ=F": "Brent Crude", "CL=F": "WTI Crude"}.items():
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="7d")
                if not hist.empty:
                    latest = hist.iloc[-1]
                    prev = hist.iloc[-2] if len(hist) > 1 else latest
                    snapshot[name.lower().replace(" ", "_")] = {
                        "price": round(float(latest["Close"]), 2),
                        "change": round(float(latest["Close"] - prev["Close"]), 2),
                        "unit": "USD/bbl",
                    }
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Oil snapshot failed: {e}")

    return snapshot


def _call_claude_api(prompt: str, max_tokens: int = 500) -> str:
    """Call Anthropic Messages API to generate the brief."""
    if not ANTHROPIC_API_KEY:
        return "[Daily brief unavailable: ANTHROPIC_API_KEY not configured]"

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "system": (
            "You are an expert European energy market analyst writing a daily "
            "market brief for energy-focus.org. Write in a professional but "
            "accessible tone. Focus on natural gas (TTF, storage, flows), oil "
            "(Brent), and key geopolitical/infrastructure developments. "
            "Always cite specific numbers when available. Keep the brief "
            "between 200 and 300 words. Use no markdown headers — just flowing "
            "prose with clear paragraph breaks."
        ),
    }

    try:
        resp = requests.post(ANTHROPIC_API_URL, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        content = data.get("content", [])
        text = " ".join(
            block.get("text", "") for block in content if block.get("type") == "text"
        )
        return text.strip()
    except Exception as e:
        logger.error(f"Claude API call failed: {e}")
        return f"[Brief generation failed: {str(e)}]"


async def generate_daily_brief() -> dict:
    """Main entry point: gather data, build prompt, call Claude, return structured brief."""
    logger.info("Generating daily brief...")

    headlines = _fetch_rss_headlines(max_per_feed=5)
    umm = _fetch_entsog_urgent_messages()
    snapshot = _fetch_market_snapshot()

    today = datetime.now().strftime("%A, %B %d, %Y")
    headline_text = "\n".join(
        f"- [{h['source']}] {h['title']}" for h in headlines[:25]
    )
    umm_text = "\n".join(f"- {m}" for m in umm) if umm else "None reported."
    snapshot_text = json.dumps(snapshot, indent=2) if snapshot else "No live data available."

    prompt = f"""Write the Energy Focus daily market brief for {today}.

Here are the latest headlines from industry sources:
{headline_text}

ENTSOG Urgent Market Messages (last 3 days):
{umm_text}

Current market snapshot:
{snapshot_text}

Write a cohesive 200-300 word brief covering the most important developments.
Prioritize: gas prices/storage/flows, then oil, then infrastructure/policy.
Start with today's date as the dateline."""

    brief_text = _call_claude_api(prompt)
    word_count = len(brief_text.split())

    return {
        "date": today,
        "brief": brief_text,
        "word_count": word_count,
        "sources_used": len(headlines),
        "umm_count": len(umm),
        "market_snapshot": snapshot,
        "generated_at": datetime.now().isoformat(),
        "model": CLAUDE_MODEL,
    }


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level="INFO", format="%(asctime)s | %(name)s | %(message)s")
    result = asyncio.run(generate_daily_brief())
    print(json.dumps(result, indent=2))
