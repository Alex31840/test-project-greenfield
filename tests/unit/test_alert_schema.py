from __future__ import annotations

import json

from lambda_src.detection_handler import alert_schema


def test_build_alert_has_exactly_five_fields():
    alert = alert_schema.build_alert(
        drone_id="drone-42",
        latitude=37.7749,
        longitude=-122.4194,
        confidence=0.91,
        timestamp="2024-01-01T00:00:00Z",
    )
    assert set(alert.keys()) == {
        "drone_id",
        "latitude",
        "longitude",
        "confidence",
        "timestamp",
    }
    assert alert["drone_id"] == "drone-42"
    assert alert["latitude"] == 37.7749
    assert alert["longitude"] == -122.4194
    assert alert["confidence"] == 0.91
    assert alert["timestamp"] == "2024-01-01T00:00:00Z"


def test_build_sns_message_has_distinct_bodies_sms_shorter_than_email():
    alert = alert_schema.build_alert(
        drone_id="drone-7",
        latitude=1.234,
        longitude=5.678,
        confidence=0.88,
        timestamp="2024-05-05T12:00:00Z",
    )
    message = alert_schema.build_sns_message(alert)

    assert set(message.keys()) == {"default", "sms", "email"}
    assert message["default"] != message["sms"]
    assert message["default"] != message["email"]
    assert message["sms"] != message["email"]
    assert len(message["sms"]) < len(message["email"])

    for body in message.values():
        assert "drone-7" in body
        assert "1.234" in body
        assert "5.678" in body
        assert "0.88" in body
        assert "2024-05-05T12:00:00Z" in body


def test_build_sns_publish_kwargs_uses_json_message_structure():
    alert = alert_schema.build_alert(
        drone_id="drone-1",
        latitude=0.0,
        longitude=0.0,
        confidence=0.75,
        timestamp="2024-01-01T00:00:00Z",
    )
    kwargs = alert_schema.build_sns_publish_kwargs(alert, topic_arn="arn:aws:sns:us-east-1:123:fire-alerts")

    assert kwargs["MessageStructure"] == "json"
    assert kwargs["TopicArn"] == "arn:aws:sns:us-east-1:123:fire-alerts"
    body = json.loads(kwargs["Message"])
    assert set(body.keys()) == {"default", "sms", "email"}
