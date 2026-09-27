# scripts/test_alert.py
"""
Utility script to verify your Discord / Slack Webhook integration.
Sends a test heartbeat alert using the configured DISCORD_WEBHOOK_URL.

Usage:
    python scripts/test_alert.py
"""

import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.alerts import notify_success, get_webhook_url


def main():
    url = get_webhook_url()
    if not url:
        print("\n❌ Error: DISCORD_WEBHOOK_URL is not set in your .env file!")
        print("Please edit .env and add:")
        print("DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...")
        sys.exit(1)

    print(f"\n📡 Dispatching test alert to webhook URL: {url[:35]}...")

    dummy_assets = [
        {"symbol": "BITCOIN", "price": 84738.00, "change_24h": 0.80},
        {"symbol": "ETHEREUM", "price": 2716.52, "change_24h": 0.99},
        {"symbol": "SOLANA", "price": 124.19, "change_24h": 2.92},
    ]

    success = notify_success(
        duration_seconds=2.45,
        extracted_count=3,
        loaded_count=3,
        asset_summaries=dummy_assets,
    )

    if success:
        print("✅ SUCCESS! Test alert delivered to Discord. Check your channel!")
    else:
        print("❌ FAILED to send alert. Check webhook URL and internet connectivity.")


if __name__ == "__main__":
    main()
