"""Regenerate the self-contained ATIF fixture using only the Python standard library."""

import json
import math
from pathlib import Path
import struct
import wave
import zlib

DIRECTORY = Path(__file__).resolve().parent
WIDTH, HEIGHT = 1024, 768


def chunk(kind, data):
    return (
        struct.pack(">I", len(data))
        + kind
        + data
        + struct.pack(">I", zlib.crc32(kind + data))
    )


def media(kind, mime, path, **extra):
    return {"type": kind, "source": {"media_type": mime, "path": path, **extra}}


def main():
    # An RGB test pattern, with a PNG filter byte at the start of each scanline.
    rows = b"".join(
        b"\x00" + bytes(channel for x in range(WIDTH) for channel in (x % 256, y % 256, (x + y) % 256))
        for y in range(HEIGHT)
    )
    # No DEFLATE compression: valid image data naturally exceeds the default
    # 2 MiB inline-blob threshold, without padding or trailing junk.
    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", WIDTH, HEIGHT, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows, level=0))
        + chunk(b"IEND", b"")
    )
    (DIRECTORY / "atif-image.png").write_bytes(png)
    with wave.open(str(DIRECTORY / "atif-audio.wav"), "wb") as audio:
        audio.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
        audio.writeframes(b"".join(
            struct.pack("<h", round(12000 * math.sin(2 * math.pi * 440 * i / 8000)))
            for i in range(800)
        ))
    image = media("image", "image/png", "atif-image.png")
    audio = media("audio", "audio/wav", "atif-audio.wav", duration_sec=0.1)
    document = {
        "schema_version": "ATIF-v1.8",
        "trajectory_id": "media",
        "agent": {"name": "test", "version": "1"},
        "notes": "Generated conformance fixture with decodable PNG/WAV; not a production agent capture.",
        "steps": [
            {"step_id": 1, "source": "user", "message": [
                {"type": "text", "text": "hello 你好🙂"}, image, audio,
            ]},
            {"step_id": 2, "source": "agent", "message": "Inspecting the supplied media.",
             "tool_calls": [{"tool_call_id": "inspect-1", "function_name": "inspect_media", "arguments": {}}],
             "observation": {"results": [{"source_call_id": "inspect-1", "content": [
                 {"type": "text", "text": "RGB pattern and a 440 Hz tone."}, image, audio,
             ]}]}},
            {"step_id": 3, "source": "agent", "message": "The image is an RGB pattern; the audio is a short tone."},
        ],
    }
    (DIRECTORY / "trajectory_media.json").write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
