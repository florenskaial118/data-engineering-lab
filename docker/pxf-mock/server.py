import json
import os
import sys
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").rstrip("/")
DEFAULT_SOURCE = os.getenv(
    "PXF_DEFAULT_SOURCE",
    "datalake/raw/orders/dt=2026-06-23/orders.csv",
).lstrip("/")
PORT = int(os.getenv("PXF_MOCK_PORT", "5888"))


def log(message):
    print(message, file=sys.stdout, flush=True)


def normalize_source(source):
    if not source:
        return DEFAULT_SOURCE

    source = urllib.parse.unquote(source).strip().lstrip("/")
    if source.startswith("pxf://"):
        parsed = urllib.parse.urlparse(source)
        source = f"{parsed.netloc}{parsed.path}"
    return source.lstrip("/") or DEFAULT_SOURCE


def source_from_fragment_header(header_value):
    if not header_value:
        return DEFAULT_SOURCE

    try:
        fragment = json.loads(header_value)
    except json.JSONDecodeError:
        return normalize_source(header_value)

    if isinstance(fragment, dict):
        return normalize_source(fragment.get("sourceName") or fragment.get("source"))
    return DEFAULT_SOURCE


def source_from_headers(headers):
    fragment_source = source_from_fragment_header(headers.get("X-GP-DATA-FRAGMENT"))
    if fragment_source != DEFAULT_SOURCE:
        return fragment_source

    uri_source = normalize_source(headers.get("X-GP-URI"))
    if uri_source != DEFAULT_SOURCE:
        return uri_source

    data_dir_source = normalize_source(headers.get("X-GP-DATA-DIR"))
    if data_dir_source and data_dir_source != "v15":
        return data_dir_source

    return DEFAULT_SOURCE


class PxfMockHandler(BaseHTTPRequestHandler):
    server_version = "PXFMock/0.1"

    def log_message(self, fmt, *args):
        log(f"{self.client_address[0]} - {fmt % args}")

    def do_GET(self):
        self.handle_request()

    def do_POST(self):
        self.handle_request()

    def handle_request(self):
        parsed = urllib.parse.urlparse(self.path)
        log(f"{self.command} {self.path}")
        for key, value in self.headers.items():
            if key.lower().startswith("x-gp"):
                log(f"  {key}: {value}")

        if parsed.path.endswith("/Fragmenter/getFragments"):
            self.handle_fragments(parsed)
            return

        if "/Bridge/" in parsed.path or parsed.path.endswith("/Bridge"):
            self.handle_bridge()
            return

        self.send_json({"status": "ok", "service": "pxf-mock"})

    def handle_fragments(self, parsed):
        query = urllib.parse.parse_qs(parsed.query)
        source = query.get("path", [None])[0]

        if not source:
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) >= 2:
                # pxf.so calls /<profile>/<path>/Fragmenter/getFragments.
                source = "/".join(parts[1:-2])

        source_name = normalize_source(source)
        fragment = {
            "sourceName": source_name,
            "metadata": "",
            "userData": "",
            "replicas": ["pxf-mock"],
        }
        self.send_json({"PXFFragments": [fragment]})

    def handle_bridge(self):
        source = source_from_headers(self.headers)
        url = f"{MINIO_ENDPOINT}/{source}"
        log(f"  fetch: {url}")

        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                payload = response.read()
        except Exception as exc:  # noqa: BLE001 - return readable HTTP error to Greenplum.
            body = f"PXF mock failed to read {url}: {exc}\n".encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def send_json(self, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    server = HTTPServer(("0.0.0.0", PORT), PxfMockHandler)
    log(f"PXF mock listening on 0.0.0.0:{PORT}, MinIO endpoint: {MINIO_ENDPOINT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
