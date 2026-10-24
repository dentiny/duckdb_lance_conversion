# ATIF media fixture

`trajectory_media.json` is a generated ATIF v1.8 conformance fixture, not a
production Harbor export. It contains Unicode text, image/audio user messages,
a tool call with image/audio observations, and a final response.

`atif-image.png` is a compressed 1024 × 768 RGB pattern; `atif-audio.wav` is a
0.1-second, 440 Hz, mono 16-bit PCM tone at 8 kHz. Regenerate them with:

```sh
python3 test/data/generate_atif_media.py
```

The SQL test compares message/observation bytes with these files and writes Lance.
The Rust round-trip test re-encodes the same pixels without compression to exceed
2 MiB, exercises default out-of-line Blob storage, deletes the input files,
then compares and decodes the bytes read back from Lance. PNG/WAV decoders are
test-only dependencies; conversion does not transcode media.
