"""
alert_schema.py

Builds the fire-alert payload and the per-protocol SNS message bodies.

The alert payload is intentionally restricted to exactly five fields --
drone_id, latitude, longitude, confidence, timestamp -- per the
detection-lambda contract. No other field is added here; anything
else (object key, bucket, etc.) belongs in structured logs, not in the
published alert.
"""

from __future__ import annotations

import json
from typing import Any, Dict

#: The exact, closed set of keys an alert payload must contain -- in
#: order. Anything producing an alert dict must match this precisely.
ALERT_FIELDS = ("drone_id", "latitude", "longitude", "confidence", "timestamp")


def build_alert(
    drone_id: str,
    latitude: float,
    longitude: float,
    confidence: float,
    timestamp: str,
) -> Dict[str, Any]:
    """Build the alert payload dict.

    Returns a dict with exactly the keys drone_id, latitude, longitude,
    confidence, timestamp -- nothing more, nothing less.
    """
    return {
        "drone_id": drone_id,
        "latitude": latitude,
        "longitude": longitude,
        "confidence": confidence,
        "timestamp": timestamp,
    }


def _format_default(alert: Dict[str, Any]) -> str:
    return (
        f"Fire detected by drone {alert['drone_id']} at "
        f"({alert['latitude']}, {alert['longitude']}) "
        f"with confidence {alert['confidence']} at {alert['timestamp']}."
    )


def _format_sms(alert: Dict[str, Any]) -> str:
    # Short-form: SMS carriers commonly truncate messages around 140-160
    # chars, so keep this terse while still carrying every field.
    return (
        f"FIRE ALERT drone={alert['drone_id']} "
        f"lat={alert['latitude']} lon={alert['longitude']} "
        f"conf={alert['confidence']} t={alert['timestamp']}"
    )


def _format_email(alert: Dict[str, Any]) -> str:
    # Long-form: full sentence, restates every field for a human reader.
    return (
        "A potential fire was detected by the drone surveillance system.\n\n"
        f"Drone ID: {alert['drone_id']}\n"
        f"Location: latitude {alert['latitude']}, longitude {alert['longitude']}\n"
        f"Confidence score: {alert['confidence']}\n"
        f"Detected at: {alert['timestamp']}\n\n"
        "Please investigate and dispatch a response if appropriate."
    )


def build_sns_message(alert: Dict[str, Any]) -> Dict[str, str]:
    """Build the three per-protocol message bodies (default/sms/email)
    describing the same alert event. The sms body is intentionally the
    shortest of the three.
    """
    return {
        "default": _format_default(alert),
        "sms": _format_sms(alert),
        "email": _format_email(alert),
    }


def build_sns_publish_kwargs(alert: Dict[str, Any], *, topic_arn: str) -> Dict[str, Any]:
    """Build the full kwargs dict for an sns.publish(...) call: json
    MessageStructure with distinct default/sms/email bodies.
    """
    message = build_sns_message(alert)
    return {
        "TopicArn": topic_arn,
        "Message": json.dumps(message),
        "Subject": "Fire Alert",
        "MessageStructure": "json",
    }
