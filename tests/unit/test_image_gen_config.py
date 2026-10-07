"""
Unit tests for reference_producer/image_gen_config.py.

Covers the acceptance criteria for this objective:

  1. build_image_generation_payload(), given a stream name, bucket URI
     and region, produces a JSON payload matching the documented
     update-image-generation-input.json structure field-for-field.
  2. SamplingInterval < 200 raises a validation error before any AWS
     API call is attempted.
  3. SamplingInterval == 200 (or omitted, defaulting to 200) succeeds
     in constructing and submitting the configuration via
     kinesisvideo.update_image_generation_configuration.
  4. The mandatory AWS_KINESISVIDEO_IMAGE_GENERATION fragment tag and
     the >= 60s wait requirement are documented (constants + comments
     a test can assert against, not just prose nobody checks).
"""
from __future__ import annotations

import pathlib

import pytest

from reference_producer.image_gen_config import (
    DEFAULT_FORMAT,
    DEFAULT_HEIGHT_PIXELS,
    DEFAULT_IMAGE_SELECTOR_TYPE,
    DEFAULT_JPEG_QUALITY,
    DEFAULT_SAMPLING_INTERVAL_MS,
    DEFAULT_WIDTH_PIXELS,
    IMAGE_GENERATION_FRAGMENT_TAG,
    MIN_SAMPLING_INTERVAL_MS,
    MIN_WAIT_SECONDS_BEFORE_PUTMEDIA,
    InvalidImageGenerationConfigError,
    apply_image_generation_configuration,
    build_image_generation_payload,
)


# ---------------------------------------------------------------------------
# 1. Payload shape matches the documented schema field-for-field.
# ---------------------------------------------------------------------------
class TestPayloadShape:
    def test_matches_documented_schema_field_for_field(self):
        payload = build_image_generation_payload(
            "my-drone-stream", "s3://my-bucket-name", "us-east-1"
        )
        assert payload == {
            "StreamName": "my-drone-stream",
            "ImageGenerationConfiguration": {
                "Status": "ENABLED",
                "DestinationConfig": {
                    "DestinationRegion": "us-east-1",
                    "Uri": "s3://my-bucket-name",
                },
                "SamplingInterval": 200,
                "ImageSelectorType": "PRODUCER_TIMESTAMP",
                "Format": "JPEG",
                "FormatConfig": {"JPEGQuality": "80"},
                "WidthPixels": 320,
                "HeightPixels": 240,
            },
        }

    def test_defaults_match_documented_values(self):
        assert DEFAULT_SAMPLING_INTERVAL_MS == 200
        assert DEFAULT_IMAGE_SELECTOR_TYPE == "PRODUCER_TIMESTAMP"
        assert DEFAULT_FORMAT == "JPEG"
        assert DEFAULT_JPEG_QUALITY == "80"
        assert DEFAULT_WIDTH_PIXELS == 320
        assert DEFAULT_HEIGHT_PIXELS == 240

    def test_custom_stream_bucket_and_region_are_threaded_through(self):
        payload = build_image_generation_payload(
            "other-stream", "s3://other-bucket", "ap-southeast-2"
        )
        assert payload["StreamName"] == "other-stream"
        cfg = payload["ImageGenerationConfiguration"]
        assert cfg["DestinationConfig"]["Uri"] == "s3://other-bucket"
        assert cfg["DestinationConfig"]["DestinationRegion"] == "ap-southeast-2"

    def test_rejects_non_s3_uri(self):
        with pytest.raises(InvalidImageGenerationConfigError):
            build_image_generation_payload(
                "stream", "https://not-an-s3-uri", "us-east-1"
            )

    def test_rejects_missing_stream_name(self):
        with pytest.raises(InvalidImageGenerationConfigError):
            build_image_generation_payload("", "s3://bucket", "us-east-1")

    def test_rejects_missing_region(self):
        with pytest.raises(InvalidImageGenerationConfigError):
            build_image_generation_payload("stream", "s3://bucket", "")


# ---------------------------------------------------------------------------
# 2. 200ms SamplingInterval floor enforced before any AWS call.
# ---------------------------------------------------------------------------
class TestSamplingIntervalFloor:
    def test_sampling_interval_below_200_rejected_without_calling_aws(self):
        class ExplodingClient:
            def update_image_generation_configuration(self, **kwargs):
                raise AssertionError(
                    "AWS API must not be called when validation should have failed"
                )

        with pytest.raises(InvalidImageGenerationConfigError):
            apply_image_generation_configuration(
                "stream",
                "s3://bucket",
                "us-east-1",
                sampling_interval_ms=199,
                client=ExplodingClient(),
            )

    def test_build_payload_also_rejects_below_floor(self):
        with pytest.raises(InvalidImageGenerationConfigError) as excinfo:
            build_image_generation_payload(
                "stream", "s3://bucket", "us-east-1", sampling_interval_ms=1
            )
        assert "200" in str(excinfo.value)

    def test_floor_constant_is_200(self):
        assert MIN_SAMPLING_INTERVAL_MS == 200


# ---------------------------------------------------------------------------
# 3. SamplingInterval == 200 (or default) succeeds and submits via the
#    kinesisvideo API.
# ---------------------------------------------------------------------------
class FakeKinesisVideoClient:
    def __init__(self):
        self.calls = []

    def update_image_generation_configuration(self, **kwargs):
        self.calls.append(kwargs)
        return {}  # the real API returns an empty response on success (p.246)


class TestSuccessfulSubmission:
    def test_sampling_interval_200_submits_via_api(self):
        client = FakeKinesisVideoClient()
        response = apply_image_generation_configuration(
            "demo-stream",
            "s3://my-bucket-name",
            "us-east-1",
            sampling_interval_ms=200,
            client=client,
        )
        assert response == {}
        assert len(client.calls) == 1
        call = client.calls[0]
        assert call["StreamName"] == "demo-stream"
        assert call["ImageGenerationConfiguration"]["SamplingInterval"] == 200
        assert call["ImageGenerationConfiguration"]["Status"] == "ENABLED"

    def test_sampling_interval_omitted_defaults_to_200_and_submits(self):
        client = FakeKinesisVideoClient()
        apply_image_generation_configuration(
            "demo-stream", "s3://my-bucket-name", "us-east-1", client=client
        )
        assert client.calls[0]["ImageGenerationConfiguration"]["SamplingInterval"] == 200

    def test_sampling_interval_above_floor_also_succeeds(self):
        client = FakeKinesisVideoClient()
        apply_image_generation_configuration(
            "demo-stream",
            "s3://my-bucket-name",
            "us-east-1",
            sampling_interval_ms=500,
            client=client,
        )
        assert client.calls[0]["ImageGenerationConfiguration"]["SamplingInterval"] == 500


# ---------------------------------------------------------------------------
# 4. Mandatory fragment tag + 60s wait requirement are documented.
# ---------------------------------------------------------------------------
class TestDocumentedRequirements:
    def test_fragment_tag_constant_matches_kvs_requirement(self):
        assert IMAGE_GENERATION_FRAGMENT_TAG == "AWS_KINESISVIDEO_IMAGE_GENERATION"

    def test_minimum_wait_is_60_seconds(self):
        assert MIN_WAIT_SECONDS_BEFORE_PUTMEDIA == 60

    def test_module_docstring_documents_fragment_tag_and_wait_requirement(self):
        source = pathlib.Path("reference_producer/image_gen_config.py").read_text()
        assert "AWS_KINESISVIDEO_IMAGE_GENERATION" in source
        assert "60" in source
        assert "putKinesisVideoEventMetadata" in source


# ---------------------------------------------------------------------------
# CLI smoke test (dry-run path, no AWS calls).
# ---------------------------------------------------------------------------
class TestCli:
    def test_dry_run_prints_payload_and_exits_zero(self, capsys):
        from reference_producer.image_gen_config import _main

        rc = _main(
            [
                "--stream-name",
                "demo-stream",
                "--bucket-uri",
                "s3://my-bucket-name",
                "--region",
                "us-east-1",
                "--dry-run",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert '"SamplingInterval": 200' in out
        assert '"StreamName": "demo-stream"' in out

    def test_cli_rejects_low_sampling_interval_without_aws_call(self, capsys):
        from reference_producer.image_gen_config import _main

        rc = _main(
            [
                "--stream-name",
                "demo-stream",
                "--bucket-uri",
                "s3://my-bucket-name",
                "--region",
                "us-east-1",
                "--sampling-interval-ms",
                "50",
                "--dry-run",
            ]
        )
        assert rc == 2
        err = capsys.readouterr().err
        assert "200" in err
