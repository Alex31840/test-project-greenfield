#!/usr/bin/env python3
"""
app.py -- CDK app entry point.

Instantiates a single CDK App and one FireDetectionStack per
deployable environment. Every piece of account/region/VPC/threshold/
recipient configuration comes from CDK context (settable via
`-c key=value`, `cdk.json`, or `cdk.context.json`) so the same app
deploys to any account/region/VPC purely by changing configuration --
nothing is hardcoded here or in the stack.
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks.fire_detection_stack import FireDetectionStack


def _context(app: cdk.App, key: str, default=None):
    value = app.node.try_get_context(key)
    return value if value is not None else default


def main() -> None:
    app = cdk.App()

    env_name = _context(app, "env_name", "dev")

    # Account/region: context first, then standard CDK_DEFAULT_*
    # environment variables (populated by the CDK CLI from the current
    # AWS credentials/profile), then None (environment-agnostic
    # synthesis). Never hardcoded.
    account = _context(app, "account") or os.environ.get("CDK_DEFAULT_ACCOUNT")
    region = _context(app, "region") or os.environ.get("CDK_DEFAULT_REGION")

    vpc_id = _context(app, "vpc_id") or None
    raw_subnet_ids = _context(app, "subnet_ids") or []
    if isinstance(raw_subnet_ids, str):
        # Tolerate a comma-separated string (e.g. passed via a single
        # `-c subnet_ids=subnet-a,subnet-b` CLI flag) in addition to a
        # native JSON array from cdk.json/cdk.context.json.
        subnet_ids = [s.strip() for s in raw_subnet_ids.split(",") if s.strip()]
    else:
        subnet_ids = list(raw_subnet_ids)

    stream_name = _context(app, "stream_name", "drone-fire-detection-stream")
    bucket_name = _context(app, "bucket_name") or None
    topic_name = _context(app, "topic_name", "fire-alerts")
    sagemaker_endpoint_name = _context(
        app, "sagemaker_endpoint_name", "fire-detection-endpoint"
    )
    confidence_threshold = _context(app, "confidence_threshold", "0.75")
    recipients = _context(app, "recipients", {"sms": [], "email": []})
    if isinstance(recipients, str):
        # Tolerate a JSON-encoded string (e.g. `-c recipients='{"sms":[...]}'`)
        import json as _json

        recipients = _json.loads(recipients) if recipients.strip() else {
            "sms": [],
            "email": [],
        }
    data_retention_hours = int(_context(app, "data_retention_hours", 24))
    image_sampling_interval_ms = int(
        _context(app, "image_sampling_interval_ms", 200)
    )

    env = None
    if account or region:
        env = cdk.Environment(account=account, region=region)

    FireDetectionStack(
        app,
        f"FireDetectionStack-{env_name}",
        env=env,
        env_name=env_name,
        vpc_id=vpc_id,
        subnet_ids=subnet_ids,
        stream_name=stream_name,
        bucket_name=bucket_name,
        topic_name=topic_name,
        sagemaker_endpoint_name=sagemaker_endpoint_name,
        confidence_threshold=confidence_threshold,
        recipients=recipients,
        data_retention_hours=data_retention_hours,
        image_sampling_interval_ms=image_sampling_interval_ms,
    )

    app.synth()


if __name__ == "__main__":
    main()
