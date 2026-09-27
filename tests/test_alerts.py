# tests/test_alerts.py
"""
Unit tests for Data Observability & Alerting module (src/alerts.py).
All external HTTP network calls to Discord are mocked to ensure offline reliability.
"""

from unittest.mock import patch, MagicMock
import requests
import pytest

from src.alerts import (
    send_discord_webhook,
    notify_success,
    notify_failure,
    COLOR_SUCCESS,
    COLOR_FAILURE,
)


class TestDiscordAlerts:
    @patch("src.alerts.get_webhook_url")
    def test_webhook_skipped_when_url_missing(self, mock_get_url):
        """When webhook URL is None or empty, sending should return False silently."""
        mock_get_url.return_value = None
        result = send_discord_webhook(embed={"title": "Test"}, webhook_url="")
        assert result is False

    @patch("src.alerts.requests.post")
    def test_webhook_successful_delivery(self, mock_post):
        """When Discord returns 204 No Content, send_discord_webhook returns True."""
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response

        dummy_url = "https://discord.com/api/webhooks/test/123"
        result = send_discord_webhook(embed={"title": "Test Title"}, webhook_url=dummy_url)

        assert result is True
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == dummy_url
        assert "embeds" in kwargs["json"]
        assert kwargs["json"]["embeds"][0]["title"] == "Test Title"

    @patch("src.alerts.requests.post")
    def test_webhook_handles_network_exception_gracefully(self, mock_post):
        """Network drop or timeout should be caught and return False without throwing."""
        mock_post.side_effect = requests.exceptions.Timeout("Connection timed out")

        dummy_url = "https://discord.com/api/webhooks/test/123"
        result = send_discord_webhook(embed={"title": "Test"}, webhook_url=dummy_url)

        assert result is False

    @patch("src.alerts.send_discord_webhook")
    def test_notify_success_embed_structure(self, mock_send):
        """Validate that notify_success builds the correct green embed and summary."""
        mock_send.return_value = True

        asset_samples = [
            {"symbol": "BITCOIN", "price": 84500.50, "change_24h": 1.25},
            {"symbol": "ETHEREUM", "price": 2700.10, "change_24h": -0.45},
        ]

        result = notify_success(
            duration_seconds=3.45,
            extracted_count=3,
            loaded_count=3,
            asset_summaries=asset_samples,
            webhook_url="https://discord.com/dummy",
        )

        assert result is True
        mock_send.assert_called_once()
        embed = mock_send.call_args[0][0]

        assert embed["color"] == COLOR_SUCCESS
        assert "Succeeded" in embed["title"]
        field_names = [f["name"] for f in embed["fields"]]
        assert "⏱️ Execution Duration" in field_names
        assert "📊 Records (Extracted / Loaded)" in field_names
        assert "📈 Market Snapshot" in field_names

    @patch("src.alerts.send_discord_webhook")
    def test_notify_failure_embed_structure(self, mock_send):
        """Validate that notify_failure formats the crimson error embed with phase diagnostics."""
        mock_send.return_value = True

        result = notify_failure(
            phase="EXTRACT",
            error_message="HTTP 429 Too Many Requests exceeded retries",
            duration_seconds=12.5,
            webhook_url="https://discord.com/dummy",
        )

        assert result is True
        mock_send.assert_called_once()
        embed = mock_send.call_args[0][0]

        assert embed["color"] == COLOR_FAILURE
        assert "Failed" in embed["title"]
        assert "EXTRACT" in embed["fields"][0]["value"]
        assert "HTTP 429" in embed["fields"][2]["value"]
