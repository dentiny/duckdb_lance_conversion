# ATIF media fixture

`trajectory_media.json` is a generated ATIF v1.8 conformance fixture, not a
production Harbor export. It contains Unicode text, image/audio user messages,
an agent tool call with image/audio observations, and a final agent response.
Media references are relative to this JSON file.

- `atif-image.png`: a valid 1024 × 768, 8-bit RGB test pattern. Uncompressed
  DEFLATE keeps the actual image above 2 MiB to exercise default out-of-line
  Lance Blob storage; there is no padding or trailing junk.
- `atif-audio.wav`: valid mono 16-bit PCM, 8 kHz, 800 samples (0.1 s), 440 Hz tone.

Regenerate all three files with the Python standard library:

```sh
python3 test/data/generate_atif_media.py
```

`test/sql/atif_conversion.test` compares message and observation payloads with
these binary files and exercises `COPY ... FORMAT LANCE`.
`rust/tests/atif_media.rs::real_atif_media_survives_conversion_and_source_deletion`
writes the fixture with default options, deletes the source JSON and media,
opens the Lance dataset again, compares the exact bytes and metadata, and
fully decodes the returned PNG pixels and WAV samples. PNG/WAV decoders are
only test dependencies; the extension does not transcode media.
