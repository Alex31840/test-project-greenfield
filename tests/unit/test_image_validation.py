from __future__ import annotations

import struct
import zlib

import pytest

from lambda_src.detection_handler.image_validation import (
    MAX_DIMENSION_PX,
    MAX_IMAGE_BYTES,
    validate_image,
)


def _make_png(width: int, height: int, *, extra_bytes: int = 0) -> bytes:
    signature = b"\x89PNG\r\n\x1a\n"

    def chunk(chunk_type: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + chunk_type
            + data
            + struct.pack(">I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    body = chunk(b"IHDR", ihdr) + chunk(b"IDAT", b"\x00" * max(extra_bytes, 1)) + chunk(b"IEND", b"")
    return signature + body


def _make_jpeg(width: int, height: int, *, extra_bytes: int = 0) -> bytes:
    soi = b"\xff\xd8"
    # SOF0 segment: marker(2) + length(2) + precision(1) + height(2) + width(2) + ncomp(1) + comp(3)
    sof_payload = struct.pack(">BHHB", 8, height, width, 1) + b"\x01\x11\x00"
    sof_len = len(sof_payload) + 2
    sof = b"\xff\xc0" + struct.pack(">H", sof_len) + sof_payload
    eoi = b"\xff\xd9"
    padding = b"\x00" * extra_bytes
    return soi + sof + padding + eoi


def test_valid_jpeg_passes():
    data = _make_jpeg(800, 600)
    result = validate_image(data)
    assert result.valid is True
    assert result.image_format == "JPEG"
    assert result.width == 800
    assert result.height == 600


def test_valid_png_passes():
    data = _make_png(640, 480)
    result = validate_image(data)
    assert result.valid is True
    assert result.image_format == "PNG"
    assert result.width == 640
    assert result.height == 480


def test_oversized_file_rejected():
    data = _make_jpeg(100, 100, extra_bytes=MAX_IMAGE_BYTES + 1024)
    result = validate_image(data)
    assert result.valid is False
    assert "size" in result.reason


def test_non_png_jpeg_format_rejected():
    data = b"GIF89a" + b"\x00" * 100
    result = validate_image(data)
    assert result.valid is False
    assert "format" in result.reason


def test_dimensions_over_limit_rejected():
    data = _make_jpeg(MAX_DIMENSION_PX + 1, 100)
    result = validate_image(data)
    assert result.valid is False
    assert "exceed" in result.reason


def test_png_dimensions_over_limit_rejected():
    data = _make_png(100, MAX_DIMENSION_PX + 500)
    result = validate_image(data)
    assert result.valid is False
    assert "exceed" in result.reason


def test_empty_payload_rejected():
    result = validate_image(b"")
    assert result.valid is False


def test_validate_image_never_raises_on_garbage():
    # malformed JPEG-looking header with truncated SOF
    garbage = b"\xff\xd8\xff\xc0\x00\x05\x08"
    result = validate_image(garbage)
    assert result.valid is False
