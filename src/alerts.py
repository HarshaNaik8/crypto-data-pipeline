# src/alerts.py
"""
Data Observability & Alerting module for Crypto Pipeline.
Dispatches formatted notifications to Discord (or Slack-compatible) webhooks.

Design Principles:
1. Non-blocking & Resilient: Alert failures must NEVER crash the data pipeline.
2. Rich Context: Alerts contain execution duration, row counts, and market highlights.
3. Graceful Fallback: Operates silently if webhook URL is not configured.
"""

import os
import logging
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("alerts")

# Embed color constants (hex decimal values used by Discord)
COLOR_SUCCESS = 0x2ECC71  # Emerald Green
COLOR_FAILURE = 0xE74C3C  # Crimson Red
COLOR_WARNING = 0xF1C40F  # Amber Yellow
COLOR_INFO    = 0x3498DB  # Sky Blue


def get_webhook_url() -> str | None:
    """Retrieve webhook URL from environment, or return None if unconfigured."""
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    return url if url else None


def send_discord_webhook(embed: dict, webhook_url: str | None = None) -> bool:
    """
    Deliver a Discord Embed payload via HTTP POST.
    
    Guarantees:
    - Never raises an unhandled exception to caller.
    - Times out after 5 seconds to prevent hanging the pipeline.
    """
    target_url = webhook_url or get_webhook_url()
    if not target_url:
        logger.info("Alerting skipped: DISCORD_WEBHOOK_URL not configured.")
        return False

    payload = {
        "username": "Crypto Pipeline Sentinel",
        "avatar_url": "https://cdn-icons-png.flaticon.com/512/2586/2586057.png",
        "embeds": [embed],
    }

    try:
        response = requests.post(target_url, json=payload, timeout=5)
        if response.status_code in (200, 204):
            logger.info("Webhook alert delivered successfully.")
            return True
        else:
            logger.warning("Webhook returned unexpected status code %d: %s", response.status_code, response.text)
            return False
    except requests.exceptions.RequestException as err:
        logger.warning("Failed to send webhook alert (network error): %s", err)
        return False


def notify_success(
    duration_seconds: float,
    extracted_count: int,
    loaded_count: int,
    asset_summaries: list[dict] | None = None,
    webhook_url: str | None = None,
) -> bool:
    """
    Format and send a rich green success heartbeat.
    Includes execution time, row delta, and asset price snapshots.
    """
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Format asset highlights (e.g. BTC: $84,738 (+0.80%))
    market_highlights = []
    if asset_summaries:
        for asset in asset_summaries:
            symbol = asset.get("symbol", "N/A")
            price = asset.get("price", 0.0)
            chg = asset.get("change_24h", 0.0)
            chg_sign = "+" if chg >= 0 else ""
            market_highlights.append(f"• **{symbol}**: `${price:,.2f}` ({chg_sign}{chg:.2f}%)")
        highlights_str = "\n".join(market_highlights)
    else:
        highlights_str = "No asset summary details available."

    embed = {
        "title": "🚀 Pipeline Execution Succeeded",
        "description": "Daily cryptocurrency ingestion, feature engineering, and warehousing completed without errors.",
        "color": COLOR_SUCCESS,
        "fields": [
            {
                "name": "⏱️ Execution Duration",
                "value": f"`{duration_seconds:.2f} seconds`",
                "inline": True,
            },
            {
                "name": "📊 Records (Extracted / Loaded)",
                "value": f"`{extracted_count} / {loaded_count}`",
                "inline": True,
            },
            {
                "name": "📈 Market Snapshot",
                "value": highlights_str,
                "inline": False,
            },
        ],
        "footer": {
            "text": f"Production Star-Schema Warehouse • {now_utc}",
        },
    }

    return send_discord_webhook(embed, webhook_url=webhook_url)


def notify_failure(
    phase: str,
    error_message: str,
    duration_seconds: float = 0.0,
    webhook_url: str | None = None,
) -> bool:
    """
    Format and send a high-priority red incident alert.
    Identifies the failed phase (Extract, Transform, or Load) and error diagnostic.
    """
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Truncate error message if exceptionally long for Discord embed limits (max 1024 chars per field)
    clean_error = error_message[:900] + ("..." if len(error_message) > 900 else "")

    embed = {
        "title": "🚨 Pipeline Execution Failed",
        "description": f"The automated data pipeline encountered an unrecoverable failure during the **{phase.upper()}** stage.",
        "color": COLOR_FAILURE,
        "fields": [
            {
                "name": "🛑 Failed Stage",
                "value": f"`Phase: {phase.upper()}`",
                "inline": True,
            },
            {
                "name": "⏱️ Time Elapsed Before Failure",
                "value": f"`{duration_seconds:.2f}s`",
                "inline": True,
            },
            {
                "name": "🔍 Error Diagnostics",
                "value": f"```python\n{clean_error}\n```",
                "inline": False,
            },
            {
                "name": "🛠️ Action Required",
                "value": "Check the local execution log file in `logs/` or run `pytest` to inspect pipeline health.",
                "inline": False,
            },
        ],
        "footer": {
            "text": f"Incident Alert • {now_utc}",
        },
    }

    return send_discord_webhook(embed, webhook_url=webhook_url)
