"""Notification utilities for alerting on ingestion failures."""

from __future__ import annotations

import json
from typing import Optional

import requests

from bronze_framework.utils.logging import get_logger

logger = get_logger(__name__)

# Configure these via environment variables or Databricks secrets
_SLACK_WEBHOOK_URL: Optional[str] = None
_TEAMS_WEBHOOK_URL: Optional[str] = None


def configure_slack(webhook_url: str) -> None:
    """Set the Slack webhook URL for notifications."""
    global _SLACK_WEBHOOK_URL
    _SLACK_WEBHOOK_URL = webhook_url


def configure_teams(webhook_url: str) -> None:
    """Set the Teams webhook URL for notifications."""
    global _TEAMS_WEBHOOK_URL
    _TEAMS_WEBHOOK_URL = webhook_url


def send_failure_alert(
    source_name: str,
    error_message: str,
    environment: str = "dev",
) -> None:
    """Send failure alerts to configured channels."""
    if _SLACK_WEBHOOK_URL:
        send_slack_alert(source_name, error_message, environment)
    if _TEAMS_WEBHOOK_URL:
        send_teams_alert(source_name, error_message, environment)


def send_slack_alert(
    source_name: str,
    error_message: str,
    environment: str = "dev",
) -> None:
    """Send a failure notification to Slack via webhook."""
    if not _SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL not configured; skipping alert")
        return

    payload = {
        "text": f":red_circle: *Bronze Ingestion Failure*",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Bronze Ingestion Failure",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Source:*\n{source_name}"},
                    {"type": "mrkdwn", "text": f"*Environment:*\n{environment}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{error_message[:1500]}```",
                },
            },
        ],
    }

    try:
        response = requests.post(
            _SLACK_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        logger.info(f"Slack alert sent for {source_name}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Slack alert: {e}")


def send_teams_alert(
    source_name: str,
    error_message: str,
    environment: str = "dev",
) -> None:
    """Send a failure notification to Microsoft Teams via webhook."""
    if not _TEAMS_WEBHOOK_URL:
        logger.warning("Teams webhook URL not configured; skipping alert")
        return

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "FF0000",
        "summary": f"Bronze Ingestion Failure: {source_name}",
        "sections": [
            {
                "activityTitle": "Bronze Ingestion Failure",
                "facts": [
                    {"name": "Source", "value": source_name},
                    {"name": "Environment", "value": environment},
                    {"name": "Error", "value": error_message[:1500]},
                ],
                "markdown": True,
            }
        ],
    }

    try:
        response = requests.post(
            _TEAMS_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        logger.info(f"Teams alert sent for {source_name}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Teams alert: {e}")
