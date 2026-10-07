from __future__ import annotations

import io
import json
import struct

import pytest

from lambda_src.detection_handler import config as config_module
from lambda_src.detection_handler import handler as handler_module


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_jpeg(width: int = 100, height: int = 100) -> bytes:
    soi = b"\xff\xd8"
    sof_payload = struct.pack(">BHHB", 8, height, width, 1) + b"\x01\x11\x00"
    sof_len = len(sof_payload) + 2
    sof = b"\xff\xc0" + struct.pack(">H", sof_len) + sof_payload
    eoi = b"\xff\xd9"
    return soi + sof + eoi


def _s3_event(bucket: str = "fire-images", key: str = "stream/drone-1/frame-0001.jpg"):
    return {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                },
            }
        ]
    }


class FakeStreamingBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class FakeS3Client:
    def __init__(self, image_bytes: bytes, metadata=None):
        self.image_bytes = image_bytes
        self.metadata = metadata or {}

    def head_object(self, Bucket, Key):
        return {"Metadata": self.metadata, "LastModified": None}

    def get_object(self, Bucket, Key):
        return {"Body": FakeStreamingBody(self.image_bytes)}


class FakeSageMakerClient:
    def __init__(self, confidence=None, raise_times=0, payload=None):
        self.confidence = confidence
        self.raise_times = raise_times
        self.calls = 0
        self.payload = payload

    def invoke_endpoint(self, EndpointName, ContentType, Body):
        self.calls += 1
        if self.calls <= self.raise_times:
            raise RuntimeError("endpoint unavailable")
        payload = self.payload if self.payload is not None else {"confidence": self.confidence}
        return {"Body": FakeStreamingBody(json.dumps(payload).encode("utf-8"))}


class FakeSNSClient:
    def __init__(self):
        self.publish_calls = []

    def publish(self, **kwargs):
        self.publish_calls.append(kwargs)
        return {"MessageId": "fake-message-id"}


@pytest.fixture(autouse=True)
def _reset_handler_state(monkeypatch):
    handler_module._alerted_keys.clear()
    config_module.reset_cache()
    monkeypatch.setenv("SAGEMAKER_ENDPOINT_PARAM", "/fire-detection/test/sagemaker-endpoint-name")
    monkeypatch.setenv("CONFIDENCE_THRESHOLD_PARAM", "/fire-detection/test/confidence-threshold")
    monkeypatch.setenv("SNS_TOPIC_ARN_PARAM", "/fire-detection/test/sns-topic-arn")
    yield
    handler_module._alerted_keys.clear()
    config_module.reset_cache()


@pytest.fixture
def fake_ssm(monkeypatch, request):
    threshold = getattr(request, "param", "0.75")

    class FakeSSMClient:
        def get_parameters(self, Names, WithDecryption=True):
            values = {
                "/fire-detection/test/sagemaker-endpoint-name": "fire-detection-endpoint",
                "/fire-detection/test/confidence-threshold": threshold,
                "/fire-detection/test/sns-topic-arn": "arn:aws:sns:us-east-1:123456789012:fire-alerts",
            }
            return {
                "Parameters": [
                    {"Name": name, "Value": values[name]} for name in Names
                ],
                "InvalidParameters": [],
            }

    monkeypatch.setattr(config_module, "_ssm_client", FakeSSMClient())
    return FakeSSMClient()


# ---------------------------------------------------------------------------
# Acceptance criteria tests
# ---------------------------------------------------------------------------

def test_confidence_at_threshold_publishes_exactly_one_sns_message(monkeypatch, fake_ssm):
    image_bytes = _make_jpeg()
    fake_s3 = FakeS3Client(image_bytes, metadata={"drone-id": "drone-9", "latitude": "1.1", "longitude": "2.2"})
    fake_sagemaker = FakeSageMakerClient(confidence=0.75)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    result = handler_module.handler(_s3_event(), None)

    assert result["statusCode"] == 200
    assert len(fake_sns.publish_calls) == 1
    call = fake_sns.publish_calls[0]
    assert call["TopicArn"] == "arn:aws:sns:us-east-1:123456789012:fire-alerts"
    assert call["MessageStructure"] == "json"


def test_confidence_below_threshold_makes_zero_sns_publish_calls_and_logs_discard(monkeypatch, fake_ssm, caplog):
    image_bytes = _make_jpeg()
    fake_s3 = FakeS3Client(image_bytes)
    fake_sagemaker = FakeSageMakerClient(confidence=0.74)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    with caplog.at_level("INFO"):
        handler_module.handler(_s3_event(), None)

    assert len(fake_sns.publish_calls) == 0
    assert any('"outcome": "discarded"' in r.message for r in caplog.records)


def test_sns_message_has_distinct_default_sms_email_with_all_fields(monkeypatch, fake_ssm):
    image_bytes = _make_jpeg()
    fake_s3 = FakeS3Client(
        image_bytes,
        metadata={
            "drone-id": "drone-42",
            "latitude": "37.7749",
            "longitude": "-122.4194",
        },
    )
    fake_sagemaker = FakeSageMakerClient(confidence=0.9)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    handler_module.handler(_s3_event(), None)

    assert len(fake_sns.publish_calls) == 1
    call = fake_sns.publish_calls[0]
    assert call["MessageStructure"] == "json"
    body = json.loads(call["Message"])
    assert set(body.keys()) == {"default", "sms", "email"}
    assert len(body["sms"]) < len(body["email"])

    for key_fragment in ("drone-42", "37.7749", "-122.4194", "0.9"):
        assert key_fragment in body["sms"]
        assert key_fragment in body["email"]


def test_oversized_image_rejected_before_sagemaker_call_and_handler_does_not_raise(monkeypatch, fake_ssm, caplog):
    oversized = _make_jpeg() + b"\x00" * (5 * 1024 * 1024 + 10)
    fake_s3 = FakeS3Client(oversized)
    fake_sagemaker = FakeSageMakerClient(confidence=0.99)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    with caplog.at_level("INFO"):
        result = handler_module.handler(_s3_event(), None)

    assert result["statusCode"] == 200
    assert fake_sagemaker.calls == 0
    assert len(fake_sns.publish_calls) == 0
    assert any('"outcome": "invalid_image"' in r.message for r in caplog.records)


def test_non_png_jpeg_format_rejected(monkeypatch, fake_ssm, caplog):
    fake_s3 = FakeS3Client(b"GIF89a" + b"\x00" * 50)
    fake_sagemaker = FakeSageMakerClient(confidence=0.99)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    with caplog.at_level("INFO"):
        result = handler_module.handler(_s3_event(), None)

    assert result["statusCode"] == 200
    assert fake_sagemaker.calls == 0
    assert len(fake_sns.publish_calls) == 0


def test_oversized_dimensions_rejected(monkeypatch, fake_ssm):
    huge = _make_jpeg(width=20000, height=100)
    fake_s3 = FakeS3Client(huge)
    fake_sagemaker = FakeSageMakerClient(confidence=0.99)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    handler_module.handler(_s3_event(), None)

    assert fake_sagemaker.calls == 0
    assert len(fake_sns.publish_calls) == 0


def test_sagemaker_failure_retried_once_then_dropped_distinctly_logged(monkeypatch, fake_ssm, caplog):
    image_bytes = _make_jpeg()
    fake_s3 = FakeS3Client(image_bytes)
    # Fails on both attempts (raise_times=2 covers both calls)
    fake_sagemaker = FakeSageMakerClient(confidence=0.9, raise_times=2)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    with caplog.at_level("INFO"):
        result = handler_module.handler(_s3_event(), None)

    assert result["statusCode"] == 200
    assert fake_sagemaker.calls == 2  # initial attempt + exactly one retry
    assert len(fake_sns.publish_calls) == 0

    outcomes = [json.loads(r.message)["outcome"] for r in caplog.records if r.message.startswith("{")]
    assert "inference_failed" in outcomes
    assert "discarded" not in outcomes
    assert "inference_failed" != "discarded"  # distinct outcome strings, sanity


def test_sagemaker_succeeds_on_retry_after_one_failure(monkeypatch, fake_ssm):
    image_bytes = _make_jpeg()
    fake_s3 = FakeS3Client(image_bytes)
    fake_sagemaker = FakeSageMakerClient(confidence=0.9, raise_times=1)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    handler_module.handler(_s3_event(), None)

    assert fake_sagemaker.calls == 2
    assert len(fake_sns.publish_calls) == 1


def test_duplicate_s3_event_for_same_key_does_not_double_publish(monkeypatch, fake_ssm):
    image_bytes = _make_jpeg()
    fake_s3 = FakeS3Client(image_bytes)
    fake_sagemaker = FakeSageMakerClient(confidence=0.9)
    fake_sns = FakeSNSClient()

    monkeypatch.setattr(handler_module, "_s3_client", fake_s3)
    monkeypatch.setattr(handler_module, "_sagemaker_client", fake_sagemaker)
    monkeypatch.setattr(handler_module, "_sns_client", fake_sns)

    event = _s3_event(key="stream/drone-1/frame-0001.jpg")

    handler_module.handler(event, None)
    handler_module.handler(event, None)

    assert len(fake_sns.publish_calls) == 1
