# ATIF media fixture

`trajectory_media.json` is a generated ATIF v1.8 conformance fixture, not a
production Harbor export. It contains Unicode text, image/audio user messages,
a tool call with image/audio observations, and a final response.

`atif-image.png` is a compressed 1024 × 768 RGB pattern; `atif-audio.wav` is a
0.1-second, 440 Hz, mono 16-bit PCM tone at 8 kHz. Both files are checked in.

The SQL test compares message/observation bytes with these files and writes Lance.
The Rust round-trip test re-encodes the same pixels without compression to exceed
2 MiB, exercises default out-of-line Blob storage, deletes the input files,
then compares and decodes the bytes read back from Lance. PNG/WAV decoders are
test-only dependencies; conversion does not transcode media.

# XML fixture

`noaa_stations_al.xml` is real data from NOAA's National Weather Service
observation station index (<https://forecast.weather.gov/xml/current_obs/index.xml>,
fetched 2026-11-10), a US government work in the public domain. It keeps the
original header and the 46 Alabama `<station>` elements verbatim, including an
escaped `&amp;` in KJFX's name. `noaa_stations_al.xml.gz` is the same file
compressed with `gzip -9 -n`.

`usgs_significant_month.atom` is the USGS "Significant Earthquakes, Past Month"
Atom feed (<https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.atom>,
fetched 2026-11-10), a US government work in the public domain, kept verbatim.
It has a default Atom namespace, `georss:` elements, attributes, and repeated
`<category>` elements.
