"""
image_validation.py

Format / size / dimension checks for images landing in the S3 bucket,
applied before any SageMaker invocation is attempted.

Documented limits:
    * size      <= 5 MB
    * format    PNG or JPEG only
    * dimension <= 10000 x 10000 px

Validation is done by reading the file headers only (PNG signature /
JPEG SOF markers), not by fully decoding the image -- no imaging
library dependency is required.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Optional

MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB
MAX_DIMENSION_PX = 10_000

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_JPEG_SOI = b"\xff\xd8"

# JPEG start-of-frame markers that carry height/width (baseline,
# progressive, and their variants). 0xC4 (DHT), 0xC8 (JPG), 0xCC (DAC)
# are explicitly excluded -- they are not SOF markers.
_JPEG_SOF_MARKERS = {
    0xC0, 0xC1, 0xC2, 0xC3,
    0xC5, 0xC6, 0xC7,
    0xC9, 0xCA, 0xCB,
    0xCD, 0xCE, 0xCF,
}


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    reason: Optional[str] = None
    image_format: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None


class ImageValidationError(Exception):
    """Raised internally to short-circuit validation with a reason."""


def _detect_format(data: bytes) -> str:
    if data.startswith(_PNG_SIGNATURE):
        return "PNG"
    if data.startswith(_JPEG_SOI):
        return "JPEG"
    raise ImageValidationError("unsupported image format (not PNG or JPEG)")


def _png_dimensions(data: bytes) -> tuple[int, int]:
    # PNG: the IHDR chunk is always the first chunk, immediately after
    # the 8-byte signature: 4-byte length, 4-byte type "IHDR", then
    # 4-byte width, 4-byte height (big-endian).
    if len(data) < 24:
        raise ImageValidationError("truncated PNG header")
    width, height = struct.unpack(">II", data[16:24])
    return width, height


def _jpeg_dimensions(data: bytes) -> tuple[int, int]:
    # Walk the JFIF marker segments looking for a start-of-frame
    # marker, which encodes height/width.
    size = len(data)
    offset = 2  # skip SOI
    while offset < size - 1:
        if data[offset] != 0xFF:
            offset += 1
            continue
        marker = data[offset + 1]
        offset += 2
        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
            # markers with no payload
            continue
        if marker == 0xD9:  # EOI
            break
        if offset + 2 > size:
            break
        seg_len = struct.unpack(">H", data[offset : offset + 2])[0]
        if marker in _JPEG_SOF_MARKERS:
            if offset + 7 > size:
                raise ImageValidationError("truncated JPEG SOF segment")
            height, width = struct.unpack(">HH", data[offset + 3 : offset + 7])
            return width, height
        offset += seg_len
    raise ImageValidationError("could not locate JPEG SOF marker (dimensions unknown)")


def validate_image(data: bytes, *, max_bytes: int = MAX_IMAGE_BYTES, max_dimension: int = MAX_DIMENSION_PX) -> ValidationResult:
    """Validate raw image bytes against size, format and dimension
    limits. Never raises -- any failure is reported in the returned
    ValidationResult.
    """
    if data is None or len(data) == 0:
        return ValidationResult(valid=False, reason="empty image payload")

    if len(data) > max_bytes:
        return ValidationResult(valid=False, reason=f"image exceeds max size of {max_bytes} bytes")

    try:
        image_format = _detect_format(data)
        if image_format == "PNG":
            width, height = _png_dimensions(data)
        else:
            width, height = _jpeg_dimensions(data)
    except ImageValidationError as exc:
        return ValidationResult(valid=False, reason=str(exc))

    if width > max_dimension or height > max_dimension:
        return ValidationResult(
            valid=False,
            reason=f"image dimensions {width}x{height} exceed max of {max_dimension}x{max_dimension}",
            image_format=image_format,
            width=width,
            height=height,
        )

    return ValidationResult(valid=True, image_format=image_format, width=width, height=height)
