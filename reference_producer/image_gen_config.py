"""
image_gen_config.py

Builds and applies the KVS "real-time image generation" configuration
for a stream, matching the documented schema exactly (Kinesis Video
Streams Developer Guide, "Automated real-time image generation",
update-image-generation-input.json, p.245-246):

    {
      "StreamName": "demo-stream",
      "ImageGenerationConfiguration": {
        "Status": "ENABLED",
        "DestinationConfig": {
          "DestinationRegion": "us-east-1",
          "Uri": "s3://my-bucket-name"
        },
        "SamplingInterval": 200,
        "ImageSelectorType": "PRODUCER_TIMESTAMP",
        "Format": "JPEG",
        "FormatConfig": { "JPEGQuality": "80" },
        "WidthPixels": 320,
        "HeightPixels": 240
      }
    }

This is the same payload shape the AWS CLI sends via

    aws kinesisvideo update-image-generation-configuration \\
        --cli-input-json file://./update-image-generation-input.json

(p.246) and that boto3's ``kinesisvideo.update_image_generation_configuration``
API call expects, so the function below can be used both to render the
JSON file for the CLI and to submit the configuration directly via
boto3 -- there is no CloudFormation resource type for this setting as
of this writing (the KVS Developer Guide only documents the CLI/SDK
path, not an IaC resource), so it is applied out-of-band from the CDK
stack by whichever of these two paths the deployer chooses: run this
module as a script/CLI after `cdk deploy`, or import `apply_image_generation_configuration`
from a CDK custom-resource Lambda.

--------------------------------------------------------------------
MANDATORY: the AWS_KINESISVIDEO_IMAGE_GENERATION fragment tag
--------------------------------------------------------------------
Applying this configuration to the stream is necessary but NOT
sufficient to get images delivered to S3. Kinesis Video Streams only
generates and delivers images for fragments that carry the special
MKV "simple tag" named exactly ``AWS_KINESISVIDEO_IMAGE_GENERATION``
(no value required) -- this is an undocumented-feeling but *mandatory*
requirement buried in the KVS Developer Guide's "Adding image
generation tags to fragments" section (p.247): any fragment lacking
this tag is silently skipped by the image-generation pipeline, with
no error raised anywhere. On the producer side (Java Producer SDK)
this tag is attached per-fragment with
``putKinesisVideoEventMetadata`` before/at the start of each
keyframe-bearing fragment (p.247-248):

    kinesisVideoProducerStream.putFragmentMetadata(
        "AWS_KINESISVIDEO_IMAGE_GENERATION", "");

``reference_producer/producer.py`` (the CLI test sender, built under a
separate objective) is responsible for actually calling this on every
fragment it pushes, proving the tagging mechanism works end-to-end
during the build rather than being left as a guess from the docs.

--------------------------------------------------------------------
MANDATORY: wait >= 60 seconds before pushing video
--------------------------------------------------------------------
Per the KVS Developer Guide (p.246):

    "It takes at least 1 minute to initiate the image generation
    workflow after updating the image generation configuration. Wait
    at least 1 minute before uploading video to your stream."

Any caller of ``apply_image_generation_configuration`` (this module's
CLI entry point included) MUST wait >= 60 seconds after the
``update_image_generation_configuration`` call returns before the
reference producer (or any other PutMedia/Producer-SDK client) begins
pushing frames to the stream. This module does not sleep for the
caller automatically (a deployment script applying config for several
streams should not be forced to block sequentially) -- the CLI entry
point below prints this requirement, and ``reference_producer/producer.py``
is expected to honour it before its first ``putFrame``/metadata call.

--------------------------------------------------------------------
SamplingInterval floor
--------------------------------------------------------------------
200 ms is enforced here as a hard floor: a value below it is rejected
by this module's own validation *before* any AWS API call is made
(``IMAGE_GENERATION_MIN_SAMPLING_INTERVAL_MS``), independent of
whatever the service itself would do with it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

# The 200ms floor is enforced by this module, independent of the
# service's own limits -- see "SamplingInterval floor" above.
MIN_SAMPLING_INTERVAL_MS = 200

DEFAULT_SAMPLING_INTERVAL_MS = 200
DEFAULT_IMAGE_SELECTOR_TYPE = "PRODUCER_TIMESTAMP"
DEFAULT_FORMAT = "JPEG"
DEFAULT_JPEG_QUALITY = "80"
DEFAULT_WIDTH_PIXELS = 320
DEFAULT_HEIGHT_PIXELS = 240

# The mandatory per-fragment MKV tag without which KVS silently skips
# image generation for that fragment (p.247). Exposed as a constant so
# the reference producer can reference the exact same literal rather
# than re-typing it.
IMAGE_GENERATION_FRAGMENT_TAG = "AWS_KINESISVIDEO_IMAGE_GENERATION"

# Minimum time to wait, in seconds, after applying the configuration
# before pushing any video to the stream (p.246: "at least 1 minute").
MIN_WAIT_SECONDS_BEFORE_PUTMEDIA = 60


class InvalidImageGenerationConfigError(ValueError):
    """Raised when requested image-generation parameters violate a
    floor/constraint enforced by this module, before any AWS API call
    is attempted."""


@dataclass(frozen=True)
class ImageGenerationParams:
    """Inputs needed to build an ImageGenerationConfiguration payload."""

    stream_name: str
    bucket_uri: str
    destination_region: str
    sampling_interval_ms: int = DEFAULT_SAMPLING_INTERVAL_MS
    image_selector_type: str = DEFAULT_IMAGE_SELECTOR_TYPE
    image_format: str = DEFAULT_FORMAT
    jpeg_quality: str = DEFAULT_JPEG_QUALITY
    width_pixels: int = DEFAULT_WIDTH_PIXELS
    height_pixels: int = DEFAULT_HEIGHT_PIXELS
    status: str = "ENABLED"


def _validate(params: ImageGenerationParams) -> None:
    if not params.stream_name:
        raise InvalidImageGenerationConfigError("stream_name is required")
    if not params.bucket_uri:
        raise InvalidImageGenerationConfigError("bucket_uri is required")
    if not params.bucket_uri.startswith("s3://"):
        raise InvalidImageGenerationConfigError(
            f"bucket_uri must be an s3:// URI, got {params.bucket_uri!r}"
        )
    if not params.destination_region:
        raise InvalidImageGenerationConfigError("destination_region is required")

    # --- the 200ms floor: rejected before any AWS call is made. -----
    if params.sampling_interval_ms < MIN_SAMPLING_INTERVAL_MS:
        raise InvalidImageGenerationConfigError(
            "sampling_interval_ms must be >= "
            f"{MIN_SAMPLING_INTERVAL_MS} (got {params.sampling_interval_ms}); "
            f"{MIN_SAMPLING_INTERVAL_MS}ms is enforced as a floor by this "
            "module regardless of what the service itself would accept."
        )

    if params.image_selector_type != "PRODUCER_TIMESTAMP":
        raise InvalidImageGenerationConfigError(
            "image_selector_type must be 'PRODUCER_TIMESTAMP' "
            f"(got {params.image_selector_type!r})"
        )
    if params.image_format != "JPEG":
        raise InvalidImageGenerationConfigError(
            f"image_format must be 'JPEG' (got {params.image_format!r})"
        )
    if params.status != "ENABLED" and params.status != "DISABLED":
        raise InvalidImageGenerationConfigError(
            f"status must be 'ENABLED' or 'DISABLED' (got {params.status!r})"
        )
    if params.width_pixels <= 0 or params.height_pixels <= 0:
        raise InvalidImageGenerationConfigError(
            "width_pixels and height_pixels must be positive"
        )


def build_image_generation_payload(
    stream_name: str,
    bucket_uri: str,
    destination_region: str,
    *,
    sampling_interval_ms: int = DEFAULT_SAMPLING_INTERVAL_MS,
    image_selector_type: str = DEFAULT_IMAGE_SELECTOR_TYPE,
    image_format: str = DEFAULT_FORMAT,
    jpeg_quality: str = DEFAULT_JPEG_QUALITY,
    width_pixels: int = DEFAULT_WIDTH_PIXELS,
    height_pixels: int = DEFAULT_HEIGHT_PIXELS,
    status: str = "ENABLED",
) -> Dict[str, Any]:
    """Build the JSON-serializable payload for
    ``update_image_generation_configuration`` / the CLI's
    ``update-image-generation-input.json``, matching the documented
    schema field-for-field (p.245-246).

    Raises ``InvalidImageGenerationConfigError`` -- before any AWS API
    call is made -- if ``sampling_interval_ms`` is below the enforced
    200ms floor, or any other parameter fails validation.
    """
    params = ImageGenerationParams(
        stream_name=stream_name,
        bucket_uri=bucket_uri,
        destination_region=destination_region,
        sampling_interval_ms=sampling_interval_ms,
        image_selector_type=image_selector_type,
        image_format=image_format,
        jpeg_quality=jpeg_quality,
        width_pixels=width_pixels,
        height_pixels=height_pixels,
        status=status,
    )
    _validate(params)

    return {
        "StreamName": params.stream_name,
        "ImageGenerationConfiguration": {
            "Status": params.status,
            "DestinationConfig": {
                "DestinationRegion": params.destination_region,
                "Uri": params.bucket_uri,
            },
            "SamplingInterval": params.sampling_interval_ms,
            "ImageSelectorType": params.image_selector_type,
            "Format": params.image_format,
            "FormatConfig": {"JPEGQuality": params.jpeg_quality},
            "WidthPixels": params.width_pixels,
            "HeightPixels": params.height_pixels,
        },
    }


def apply_image_generation_configuration(
    stream_name: str,
    bucket_uri: str,
    destination_region: str,
    *,
    sampling_interval_ms: int = DEFAULT_SAMPLING_INTERVAL_MS,
    image_selector_type: str = DEFAULT_IMAGE_SELECTOR_TYPE,
    image_format: str = DEFAULT_FORMAT,
    jpeg_quality: str = DEFAULT_JPEG_QUALITY,
    width_pixels: int = DEFAULT_WIDTH_PIXELS,
    height_pixels: int = DEFAULT_HEIGHT_PIXELS,
    status: str = "ENABLED",
    client: Optional[Any] = None,
) -> Dict[str, Any]:
    """Build the payload (validating the 200ms floor and every other
    field first) and submit it via
    ``kinesisvideo.update_image_generation_configuration``.

    ``client`` is an optional pre-built boto3 kinesisvideo client
    (mainly for tests / dependency injection); if omitted one is
    created lazily so importing this module has no AWS/network side
    effect.

    Returns the (empty, per the API contract -- p.246: "Upon success,
    an empty response is returned") response dict from boto3.

    IMPORTANT: per the KVS Developer Guide (p.246), it takes >= 60
    seconds for KVS to initiate the image-generation workflow after
    this call succeeds. Callers MUST wait at least
    ``MIN_WAIT_SECONDS_BEFORE_PUTMEDIA`` seconds before pushing any
    video/PutMedia traffic to the stream. This function does not sleep
    on the caller's behalf; see the module docstring.
    """
    payload = build_image_generation_payload(
        stream_name,
        bucket_uri,
        destination_region,
        sampling_interval_ms=sampling_interval_ms,
        image_selector_type=image_selector_type,
        image_format=image_format,
        jpeg_quality=jpeg_quality,
        width_pixels=width_pixels,
        height_pixels=height_pixels,
        status=status,
    )

    if client is None:
        import boto3

        client = boto3.client("kinesisvideo", region_name=destination_region)

    return client.update_image_generation_configuration(
        StreamName=payload["StreamName"],
        ImageGenerationConfiguration=payload["ImageGenerationConfiguration"],
    )


def _main(argv: Optional[list] = None) -> int:
    """CLI entry point:

        python -m reference_producer.image_gen_config \\
            --stream-name demo-stream \\
            --bucket-uri s3://my-bucket-name \\
            --region us-east-1

    Applies the configuration and prints the mandatory post-conditions
    (fragment tag requirement + 60s wait) to stdout so an operator
    running this interactively is told, not left to find it in a
    comment.
    """
    import argparse
    import json
    import sys
    import time

    parser = argparse.ArgumentParser(
        description=(
            "Apply a KVS real-time image generation configuration to a "
            "stream (see .docs/kinesisvideo_dg.txt p.245-246)."
        )
    )
    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--bucket-uri", required=True, help="e.g. s3://my-bucket-name")
    parser.add_argument("--region", required=True, dest="destination_region")
    parser.add_argument(
        "--sampling-interval-ms", type=int, default=DEFAULT_SAMPLING_INTERVAL_MS
    )
    parser.add_argument("--jpeg-quality", default=DEFAULT_JPEG_QUALITY)
    parser.add_argument("--width-pixels", type=int, default=DEFAULT_WIDTH_PIXELS)
    parser.add_argument("--height-pixels", type=int, default=DEFAULT_HEIGHT_PIXELS)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the payload that would be submitted and exit without calling AWS.",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help=(
            f"Block for {MIN_WAIT_SECONDS_BEFORE_PUTMEDIA}s after a successful "
            "call, honouring the documented minimum wait before any producer "
            "pushes video."
        ),
    )
    args = parser.parse_args(argv)

    try:
        payload = build_image_generation_payload(
            args.stream_name,
            args.bucket_uri,
            args.destination_region,
            sampling_interval_ms=args.sampling_interval_ms,
            jpeg_quality=args.jpeg_quality,
            width_pixels=args.width_pixels,
            height_pixels=args.height_pixels,
        )
    except InvalidImageGenerationConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return 0

    apply_image_generation_configuration(
        args.stream_name,
        args.bucket_uri,
        args.destination_region,
        sampling_interval_ms=args.sampling_interval_ms,
        jpeg_quality=args.jpeg_quality,
        width_pixels=args.width_pixels,
        height_pixels=args.height_pixels,
    )
    print(json.dumps(payload, indent=2))
    print(
        "\nImage generation configuration applied. REQUIRED before pushing "
        "video:\n"
        f"  1. Every fragment pushed by the producer MUST carry the MKV "
        f"simple tag '{IMAGE_GENERATION_FRAGMENT_TAG}' (empty value) via "
        "putKinesisVideoEventMetadata, or KVS silently skips image "
        "generation for that fragment (no error raised).\n"
        f"  2. Wait at least {MIN_WAIT_SECONDS_BEFORE_PUTMEDIA} seconds "
        "before uploading any video to this stream -- KVS needs that long "
        "to initiate the image-generation workflow."
    )
    if args.wait:
        print(f"Waiting {MIN_WAIT_SECONDS_BEFORE_PUTMEDIA}s before returning (--wait)...")
        time.sleep(MIN_WAIT_SECONDS_BEFORE_PUTMEDIA)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(_main())
