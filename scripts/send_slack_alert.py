"""
Sends a Slack alert with details about which pipeline check failed.

Usage: python3 scripts/send_slack_alert.py "some failure message"
"""

import sys
import os
import requests
from dotenv import load_dotenv

load_dotenv()

webhook_url = os.getenv("SLACK_WEBHOOK_URL")

if not webhook_url:
    raise ValueError("SLACK_WEBHOOK_URL not found - check your .env file")

failure_message = sys.argv[1] if len(sys.argv) > 1 else "Test alert - no message provided"

payload = {
    "text": f":rotating_light: *Data pipeline alert*\n{failure_message}"
}

response = requests.post(webhook_url, json=payload)

if response.status_code == 200:
    print("Slack alert sent successfully")
else:
    print(f"Failed to send Slack alert: {response.status_code} - {response.text}")