"""
config.py

Fetches detection-lambda configuration (SageMaker endpoint name,
confidence threshold, SNS topic ARN) from SSM Parameter Store on cold
start, and caches it for the lifetime of the execution environment
(module-level cache -- Lambda containers are reused across
invocations, so this avoids an SSM round trip on every event).

The cache additionally honours a TTL so a long-lived warm container
eventually picks up configuration changes without needing a redeploy;
set DETECTION_CONFIG_TTL_SECONDS=0 (the default) to cache forever for
the container's lifetime, as the objective specifies.

Parameter names are supplied via Lambda environment variables (set by
the CDK stack): SAGEMAKER_ENDPOINT_PARAM, CONFIDENCE_THRESHOLD_PARAM,
SNS_TOPIC_ARN_PARAM, IMAGE_BUCKET_PARAM -- each holding the *full* SSM
parameter name (e.g. "/fire-detection/dev/sagemaker-endpoint-name").
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Dict, Optional

import boto3

_ssm_client = None


def _get_ssm_client():
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm")
    return _ssm_client


@dataclass(frozen=True)
class DetectionConfig:
    sagemaker_endpoint_name: str
    confidence_threshold: float
    sns_topic_arn: str
    image_bucket_name: Optional[str] = None


class ConfigError(Exception):
    """Raised when required configuration cannot be resolved."""


# Module-level cache -- persists across invocations within the same
# warm Lambda execution environment, reset only on a cold start.
_cached_config: Optional[DetectionConfig] = None
_cached_at: float = 0.0

#: 0 means "cache forever for the container's lifetime" (the
#: objective's default behaviour). Set DETECTION_CONFIG_TTL_SECONDS to
#: a positive number of seconds to force periodic refresh on an
#: otherwise long-lived warm container.
_DEFAULT_TTL_SECONDS = 0


def _ttl_seconds() -> float:
    try:
        return float(os.environ.get("DETECTION_CONFIG_TTL_SECONDS", _DEFAULT_TTL_SECONDS))
    except (TypeError, ValueError):
        return _DEFAULT_TTL_SECONDS


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"required environment variable {name} is not set")
    return value


def _fetch_parameters(names: Dict[str, str]) -> Dict[str, str]:
    """Batch-fetch the given {logical_name: ssm_param_name} map via
    GetParameters (decrypting SecureString values), returning
    {logical_name: value}.
    """
    client = _get_ssm_client()
    param_names = list(set(names.values()))
    response = client.get_parameters(Names=param_names, WithDecryption=True)

    invalid = response.get("InvalidParameters") or []
    if invalid:
        raise ConfigError(f"SSM parameters not found: {invalid}")

    values_by_name = {p["Name"]: p["Value"] for p in response.get("Parameters", [])}

    result = {}
    for logical_name, ssm_name in names.items():
        if ssm_name not in values_by_name:
            raise ConfigError(f"SSM parameter '{ssm_name}' missing from response")
        result[logical_name] = values_by_name[ssm_name]
    return result


def load_config(*, force_refresh: bool = False) -> DetectionConfig:
    """Return the cached DetectionConfig, fetching (and caching) from
    SSM Parameter Store if there is no cached value yet or the TTL has
    expired.
    """
    global _cached_config, _cached_at

    ttl = _ttl_seconds()
    now = time.monotonic()
    is_stale = ttl > 0 and (now - _cached_at) >= ttl

    if _cached_config is not None and not force_refresh and not is_stale:
        return _cached_config

    param_map = {
        "sagemaker_endpoint_name": _env("SAGEMAKER_ENDPOINT_PARAM"),
        "confidence_threshold": _env("CONFIDENCE_THRESHOLD_PARAM"),
        "sns_topic_arn": _env("SNS_TOPIC_ARN_PARAM"),
    }
    image_bucket_param = os.environ.get("IMAGE_BUCKET_PARAM")
    if image_bucket_param:
        param_map["image_bucket_name"] = image_bucket_param

    values = _fetch_parameters(param_map)

    config = DetectionConfig(
        sagemaker_endpoint_name=values["sagemaker_endpoint_name"],
        confidence_threshold=float(values["confidence_threshold"]),
        sns_topic_arn=values["sns_topic_arn"],
        image_bucket_name=values.get("image_bucket_name"),
    )

    _cached_config = config
    _cached_at = now
    return config


def reset_cache() -> None:
    """Clear the module-level cache. Exposed for tests; a real cold
    start naturally resets the module globals.
    """
    global _cached_config, _cached_at
    _cached_config = None
    _cached_at = 0.0
