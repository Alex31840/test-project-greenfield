# KVS real-time image generation wiring

This covers `reference_producer/image_gen_config.py` and the
`ImageGenerationConfig` custom resource it feeds in
`stacks/fire_detection_stack.py`.

## What gets configured

`FireDetectionStack` applies an `ImageGenerationConfiguration` to the
stack's KVS stream, matching the schema documented in the Kinesis
Video Streams Developer Guide ("Automated real-time image
generation", `update-image-generation-input.json`, p.245-246) exactly:

```json
{
  "StreamName": "<stack's stream name>",
  "ImageGenerationConfiguration": {
    "Status": "ENABLED",
    "DestinationConfig": {
      "DestinationRegion": "<stack's region>",
      "Uri": "s3://<stack's image bucket>"
    },
    "SamplingInterval": 200,
    "ImageSelectorType": "PRODUCER_TIMESTAMP",
    "Format": "JPEG",
    "FormatConfig": { "JPEGQuality": "80" },
    "WidthPixels": 320,
    "HeightPixels": 240
  }
}
```

There is no native CloudFormation resource type for this setting (as
of this writing), so it is applied via a CDK `AwsCustomResource` that
calls `kinesisvideo:UpdateImageGenerationConfiguration` directly,
built from the exact same `build_image_generation_payload()` helper
used by this module's CLI entry point -- both paths produce the
identical payload shape, and the 200ms `SamplingInterval` floor is
enforced in exactly one place regardless of which path a deployer
uses.

## MANDATORY: the `AWS_KINESISVIDEO_IMAGE_GENERATION` fragment tag

Applying the configuration above is **necessary but not sufficient**.
Kinesis Video Streams only generates and delivers images for
fragments that carry the special MKV "simple tag" named exactly

```
AWS_KINESISVIDEO_IMAGE_GENERATION
```

with no value (KVS Developer Guide, "Adding image generation tags to
fragments", p.247). A fragment missing this tag is silently skipped by
the image-generation pipeline -- there is no error, no CloudWatch
metric, nothing: images simply never show up in S3 for that fragment.

On the producer side, the tag is attached per-fragment (before/at each
keyframe) via the Producer SDK's `putKinesisVideoEventMetadata` /
`putFragmentMetadata` call, e.g.:

```java
kinesisVideoProducerStream.putFragmentMetadata(
    "AWS_KINESISVIDEO_IMAGE_GENERATION", "");
```

`reference_producer/producer.py` (built under a separate objective) is
responsible for calling this on every fragment it pushes, so the
tagging mechanism is proven working end-to-end during the build rather
than left as a guess from the docs.

## MANDATORY: wait at least 60 seconds before pushing video

Per the KVS Developer Guide (p.246):

> It takes at least 1 minute to initiate the image generation workflow
> after updating the image generation configuration. Wait at least 1
> minute before uploading video to your stream.

Anything that calls `apply_image_generation_configuration` --
including a deployment script run after `cdk deploy`, or this module's
own CLI -- **must** wait at least
`MIN_WAIT_SECONDS_BEFORE_PUTMEDIA` (60) seconds after the call returns
before any producer (the reference producer included) begins pushing
frames to the stream. `image_gen_config.py` does not sleep on the
caller's behalf by default (a script applying config to several
streams shouldn't be forced to block sequentially); pass `--wait` on
the CLI to have it block for you, or call `time.sleep(60)` yourself
before your first `putFrame`.

## The 200ms `SamplingInterval` floor

`build_image_generation_payload()` / `apply_image_generation_configuration()`
reject any `sampling_interval_ms < 200` with
`InvalidImageGenerationConfigError` **before** any AWS API call is
attempted -- this is enforced by this module itself, independent of
whatever the service would otherwise accept.

## Usage

As a library (used by the CDK stack):

```python
from reference_producer.image_gen_config import build_image_generation_payload

payload = build_image_generation_payload(
    stream_name, f"s3://{bucket_name}", region,
)
```

As a standalone CLI (e.g. to re-apply config to an already-deployed
stream without a full `cdk deploy`):

```bash
python -m reference_producer.image_gen_config \
    --stream-name drone-fire-detection-stream \
    --bucket-uri s3://my-bucket-name \
    --region us-east-1 \
    --wait   # blocks 60s after a successful call
```

Pass `--dry-run` to print the payload without calling AWS.
