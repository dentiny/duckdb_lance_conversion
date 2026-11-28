"""Check local ATIF media against scoped DuckDB S3 secrets, without a Lance reader."""
import argparse
import functools
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import tempfile
from threading import Thread
from urllib.parse import unquote, urlsplit


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--extension", required=True)
    parser.add_argument("--extension-directory")
    parser.add_argument("--unsigned", action="store_true", help="Allow a locally built extension")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    fixtures = root / "test/data"
    objects = {
        "/image-bucket/scoped/atif-image.png": ("image-key", (fixtures / "atif-image.png").read_bytes()),
        "/audio-bucket/atif-audio.wav": ("audio-key", (fixtures / "atif-audio.wav").read_bytes()),
    }
    requests = set()

    class Handler(BaseHTTPRequestHandler):
        def serve(self, head):
            path = unquote(urlsplit(self.path).path)
            key, body = objects[path]
            if f"Credential={key}/" not in self.headers.get("Authorization", ""):
                self.send_error(403)
                return
            requests.add(path)
            first, last = 0, len(body) - 1
            byte_range = self.headers.get("Range")
            if byte_range:
                first, last = byte_range.removeprefix("bytes=").split("-")
                first, last = int(first), int(last) if last else len(body) - 1
            self.send_response(206 if byte_range else 200)
            self.send_header("Content-Length", str(last - first + 1))
            if byte_range:
                self.send_header("Content-Range", f"bytes {first}-{last}/{len(body)}")
            self.end_headers()
            if not head:
                self.wfile.write(body[first:last + 1])

        do_HEAD = functools.partialmethod(serve, True)
        do_GET = functools.partialmethod(serve, False)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            document = (fixtures / "trajectory_media.json").read_text()
            document = document.replace("atif-image.png", "s3://image-bucket/scoped/atif-image.png")
            document = document.replace("atif-audio.wav", "s3://audio-bucket/atif-audio.wav")
            trajectory = directory / "trajectory.json"
            trajectory.write_text(document)
            sql = []
            if args.extension_directory:
                sql.append(f"SET extension_directory={literal(args.extension_directory)};")
            sql += ["LOAD cache_httpfs;", f"LOAD {literal(Path(args.extension).resolve())};"]
            # A less-specific secret must not override either media object's own secret.
            sql.append("CREATE SECRET fallback (TYPE S3, ENDPOINT '127.0.0.1:1', USE_SSL false, SCOPE 's3://');")
            for name, scope, key in [("image", "s3://image-bucket/scoped/", "image-key"),
                                     ("audio", "s3://audio-bucket/", "audio-key")]:
                sql.append(f"CREATE SECRET {name} (TYPE S3, KEY_ID '{key}', SECRET 'test-secret', "
                           f"REGION 'test', ENDPOINT '127.0.0.1:{server.server_port}', "
                           f"USE_SSL false, URL_STYLE 'path', SCOPE '{scope}');")
            sql.append(f"CREATE TABLE converted AS SELECT * FROM read_atif({literal(trajectory)});")
            sql.append(f"""SELECT CASE WHEN raw_json = {literal(document)}
                AND source_uri = {literal(trajectory)}
                AND parts[2].source.path = (SELECT content FROM read_blob({literal(fixtures / 'atif-image.png')}))
                AND parts[3].source.path = (SELECT content FROM read_blob({literal(fixtures / 'atif-audio.wav')}))
                THEN true ELSE error('ATIF media or provenance changed') END
                FROM (SELECT raw_json, source_uri,
                      unnest([steps[1].message, steps[2].observation.results[1].content]) AS parts
                      FROM converted);""")
            destination = directory / "trajectory.lance"
            sql.append(f"COPY converted TO {literal(destination)} (FORMAT LANCE);")
            command = [args.duckdb, "-bail"]
            if args.unsigned:
                command.append("-unsigned")
            subprocess.run(command, input="\n".join(sql), text=True, check=True, cwd=root)
            assert destination.is_dir()
            assert requests == set(objects)
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


if __name__ == "__main__":
    main()
