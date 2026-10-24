"""Regenerate the PNG/WAV referenced by trajectory_media.json (standard library only)."""

import math
from pathlib import Path
import struct
import wave
import zlib

DIRECTORY = Path(__file__).resolve().parent
WIDTH, HEIGHT = 1024, 768


def chunk(kind, data):
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))


def main():
    # PNG Sub filtering stores each channel's difference from the previous pixel.
    rows = b"".join(
        b"\x01" + bytes((0, y % 256, y % 256)) + b"\x01\x00\x01" * (WIDTH - 1)
        for y in range(HEIGHT)
    )
    (DIRECTORY / "atif-image.png").write_bytes(
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", WIDTH, HEIGHT, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )
    with wave.open(str(DIRECTORY / "atif-audio.wav"), "wb") as audio:
        audio.setparams((1, 2, 8000, 0, "NONE", "not compressed"))
        audio.writeframes(b"".join(
            struct.pack("<h", round(12000 * math.sin(2 * math.pi * 440 * i / 8000)))
            for i in range(800)
        ))


if __name__ == "__main__":
    main()
