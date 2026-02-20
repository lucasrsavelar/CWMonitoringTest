import requests

# Replace with your own Incoming Webhook URL from the Slack app configuration
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

class SlackNotifier:

    def __init__(self, webhook_url = SLACK_WEBHOOK_URL):
        self._webhook_url = webhook_url

    # Builds and posts a Block Kit message to the configured Slack channel
    def send_anomaly_alert(self, date_hour, main_feature, details, ensemble_score):
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "⚠️ Anomaly Detected",
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Timestamp:*\n{date_hour}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Ensemble Score:*\n`{ensemble_score}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Main Trigger:*\n{main_feature}"
                        },
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Details:*\n{details}"
                    }
                },
                {"type": "divider"}
            ]
        }

        try:
            response = requests.post(self._webhook_url, json=payload, timeout=5)
            # Silently log failures
            # Notification issues should never crash the API
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Slack notification failed: {e}")
            return False