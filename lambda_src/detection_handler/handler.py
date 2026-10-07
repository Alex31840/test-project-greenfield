"""Placeholder S3-event Lambda handler.

The full implementation (image validation, SageMaker invocation,
threshold check, SNS publish, structured logging) is delivered by the
'lambda-detection-handler' module/objective. This stub exists only so
that `stacks/fire_detection_stack.py` has a valid, synthesizable asset
to package -- it is intentionally minimal and MUST be replaced by the
real handler module.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info("Received event: %s", json.dumps(event))
    return {"statusCode": 200, "body": "placeholder handler"}
