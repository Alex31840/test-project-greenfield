"""
handler.py

S3-event Lambda: on an object-created event in the image-landing
bucket, fetch the new image, validate it, invoke the configured
SageMaker endpoint, threshold the extracted confidence score, and --
only when the score clears the threshold -- compose and publish a
multi-protocol fire alert to the SNS 'fire-alerts' topic.

Design notes (see module docstrings in config.py / image_validation.py
/ alert_schema.py for the specifics of each concern):

    * boto3 clients and the SSM-backed config are initialized/cached
      at module scope, not inside `handler`, so a warm container pays
      no cold-start cost on subsequent invocations.
    * Invalid images are rejected (size / format / dimensions) before
      any SageMaker call is made; the handler logs and returns
      normally, never raising.
    * A SageMaker invoke_endpoint failure is retried exactly once; if
      the retry also fails, the handler logs a distinct
      "inference_failed" outcome and returns normally without
      publishing.
    * A below-threshold score logs a distinct "discarded" outcome and
      does not publish.
    * Idempotency: S3 can redeliver the same ObjectCreated event, so
      the handler keeps a process-local record of object keys it has
      already alerted on and skips a second publish for the same key.
      This is deliberately in-memory only (no external store) per the
      "no persistent in-pipeline state beyond S3 object and SNS
      message" constraint -- it protects against redelivery within the
      same warm container, which is the common redelivery window.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional
from urllib.parse import unquote_plus

import os

import boto3

# AWS_REGION / AWS_DEFAULT_REGION is always set by the real Lambda
# execution environment; this fallback only matters for local
# import-time safety (e.g. test collection) where boto3 would
# otherwise raise NoRegionError before any client is ever used.
os.environ.setdefault("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))

try:
    # Lambda packages this directory as the deployment root, so at
    # runtime these are plain top-level modules (no package prefix).
    import alert_schema
    import config
    from image_validation import validate_image
except ImportError:  # pragma: no cover - fallback for in-repo package imports (tests)
    from . import alert_schema, config
    from .image_validation import validate_image

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CONFIDENCE_SCORE_KEYS = ("confidence", "score", "probability")

# Clients are created once per execution environment (module scope),
# not per-invocation, to keep warm-start latency low.
_s3_client = boto3.client("s3")
_sagemaker_client = boto3.client("sagemaker-runtime")
_sns_client = boto3.client("sns")

# Process-local idempotency guard: object keys already alerted on by
# this warm container. Bounded so it cannot grow without limit across
# a very long-lived container.
_ALERTED_KEYS_MAX = 10_000
_alerted_keys: set[str] = set()


def _already_alerted(dedupe_key: str) -> bool:
    return dedupe_key in _alerted_keys


def _mark_alerted(dedupe_key: str) -> None:
    if len(_alerted_keys) >= _ALERTED_KEYS_MAX:
        _alerted_keys.clear()
    _alerted_keys.add(dedupe_key)


def _log(outcome: str, **fields: Any) -> None:
    """Structured log line. `outcome` is the distinguishing field used
    to tell discards, inference failures, validation rejections,
    publishes, etc. apart in log search/alarms.
    """
    record = {"outcome": outcome}
    record.update(fields)
    logger.info(json.dumps(record, default=str))


def _extract_s3_records(event: Dict[str, Any]):
    return event.get("Records", []) or []


def _extract_confidence(response_payload: Any) -> Optional[float]:
    """Extract a confidence score (0-1 float) from the SageMaker
    response body. Accepts a few common shapes:
        {"confidence": 0.92}
        {"score": 0.92}
        {"predictions": [{"confidence": 0.92}]}
        [{"confidence": 0.92}, ...]
    Returns None if no numeric confidence can be found.
    """
    if isinstance(response_payload, dict):
        for key in CONFIDENCE_SCORE_KEYS:
            if key in response_payload:
                try:
                    return float(response_payload[key])
                except (TypeError, ValueError):
                    return None
        predictions = response_payload.get("predictions")
        if isinstance(predictions, list) and predictions:
            return _extract_confidence(predictions[0])
        return None
    if isinstance(response_payload, list) and response_payload:
        return _extract_confidence(response_payload[0])
    return None


def _invoke_sagemaker_once(endpoint_name: str, image_bytes: bytes) -> Dict[str, Any]:
    response = _sagemaker_client.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType="image/jpeg",
        Body=image_bytes,
    )
    body = response["Body"].read()
    try:
        return json.loads(body)
    except (TypeError, ValueError, json.JSONDecodeError):
        # Non-JSON model response: wrap the raw text so extraction can
        # still fail gracefully rather than raising.
        return {"raw": body.decode("utf-8", errors="replace")}


def _invoke_sagemaker_with_retry(endpoint_name: str, image_bytes: bytes, *, key: str) -> Optional[Dict[str, Any]]:
    """Invoke the SageMaker endpoint, retrying exactly once on
    failure. Returns the parsed response payload, or None if both the
    initial attempt and the single retry failed (already logged as a
    distinct 'inference_failed' outcome in that case).
    """
    last_error: Optional[Exception] = None
    for attempt in (1, 2):
        try:
            return _invoke_sagemaker_once(endpoint_name, image_bytes)
        except Exception as exc:  # noqa: BLE001 - must never crash the handler
            last_error = exc
            if attempt == 1:
                _log(
                    "inference_retry",
                    key=key,
                    endpoint=endpoint_name,
                    attempt=attempt,
                    error=str(exc),
                )
            continue

    _log(
        "inference_failed",
        key=key,
        endpoint=endpoint_name,
        attempts=2,
        error=str(last_error) if last_error else "unknown error",
    )
    return None


def _derive_drone_id(key: str, metadata: Dict[str, str]) -> str:
    if "drone-id" in metadata:
        return metadata["drone-id"]
    if "drone_id" in metadata:
        return metadata["drone_id"]
    # Fall back to the first path segment of the object key, which is
    # how the KVS image-generation pipeline namespaces frames per
    # stream/drone (e.g. "<stream-name>/<drone_id>/...jpg").
    return key.split("/")[0] if key else "unknown"


def _derive_float(metadata: Dict[str, str], *keys: str) -> float:
    for k in keys:
        if k in metadata:
            try:
                return float(metadata[k])
            except (TypeError, ValueError):
                break
    return 0.0


def _derive_timestamp(metadata: Dict[str, str], last_modified: Optional[Any]) -> str:
    for k in ("timestamp", "capture-time", "capture_time"):
        if k in metadata:
            return metadata[k]
    if last_modified is not None:
        try:
            return last_modified.isoformat()
        except AttributeError:
            return str(last_modified)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _process_record(record: Dict[str, Any]) -> None:
    s3_info = record.get("s3", {})
    bucket = s3_info.get("bucket", {}).get("name")
    raw_key = s3_info.get("object", {}).get("key", "")
    key = unquote_plus(raw_key)

    if not bucket or not key:
        _log("malformed_event", record=record)
        return

    dedupe_key = f"{bucket}/{key}"
    if _already_alerted(dedupe_key):
        _log("duplicate_skipped", bucket=bucket, key=key)
        return

    try:
        head = _s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001
        _log("s3_fetch_failed", bucket=bucket, key=key, error=str(exc))
        return

    metadata = head.get("Metadata", {}) or {}

    try:
        get_response = _s3_client.get_object(Bucket=bucket, Key=key)
        image_bytes = get_response["Body"].read()
    except Exception as exc:  # noqa: BLE001
        _log("s3_fetch_failed", bucket=bucket, key=key, error=str(exc))
        return

    validation = validate_image(image_bytes)
    if not validation.valid:
        _log("invalid_image", bucket=bucket, key=key, reason=validation.reason)
        return

    cfg = config.load_config()

    response_payload = _invoke_sagemaker_with_retry(
        cfg.sagemaker_endpoint_name, image_bytes, key=dedupe_key
    )
    if response_payload is None:
        # Already logged as inference_failed -- no fallback engine.
        return

    confidence = _extract_confidence(response_payload)
    if confidence is None:
        _log(
            "inference_response_unrecognized",
            bucket=bucket,
            key=key,
            response=response_payload,
        )
        return

    if confidence < cfg.confidence_threshold:
        _log(
            "discarded",
            bucket=bucket,
            key=key,
            confidence=confidence,
            threshold=cfg.confidence_threshold,
        )
        return

    drone_id = _derive_drone_id(key, metadata)
    latitude = _derive_float(metadata, "latitude", "lat")
    longitude = _derive_float(metadata, "longitude", "lon", "lng")
    timestamp = _derive_timestamp(metadata, head.get("LastModified"))

    alert = alert_schema.build_alert(
        drone_id=drone_id,
        latitude=latitude,
        longitude=longitude,
        confidence=confidence,
        timestamp=timestamp,
    )
    publish_kwargs = alert_schema.build_sns_publish_kwargs(alert, topic_arn=cfg.sns_topic_arn)

    try:
        _sns_client.publish(**publish_kwargs)
    except Exception as exc:  # noqa: BLE001
        _log("sns_publish_failed", bucket=bucket, key=key, error=str(exc))
        return

    _mark_alerted(dedupe_key)
    _log("alert_published", bucket=bucket, key=key, confidence=confidence, drone_id=drone_id)


def handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """Lambda entry point. Never raises: every failure mode (bad
    image, SageMaker failure, SNS failure, malformed event) is caught,
    logged, and the handler returns normally so S3 does not treat the
    invocation as needing further redelivery handling beyond Lambda's
    own retry policy.
    """
    for record in _extract_s3_records(event):
        try:
            _process_record(record)
        except Exception as exc:  # noqa: BLE001 - absolute last resort
            _log("unhandled_error", error=str(exc), record=record)

    return {"statusCode": 200, "body": "ok"}
