from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import Dict, Optional
from urllib.parse import parse_qs, urlparse

import threading
import json


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class RequestHandler(BaseHTTPRequestHandler):
    @dataclass
    class Response:
        json: Optional[Dict]
        text: Optional[str]
        status_code: Optional[int]
        headers: Optional[Dict[str, str]]

    routes = {}

    def parse_data(self):
        content_type = self.headers.get("Content-Type")
        if content_type is None:
            self.send_error(400, "Missing Content-Type header")
            return False

        content_length = int(self.headers["Content-Length"])
        if "application/json" in content_type:
            data = self.rfile.read(content_length)
            try:
                self.json = json.loads(data)
            except json.decoder.JSONDecodeError:
                self.send_error(400, "Json deserialization failed")
                return False
        elif "application/x-www-form-urlencoded" in content_type:
            data = self.rfile.read(content_length)
            self.form_data = parse_qs(data.decode())
        else:
            self.send_error(400, "Unsupported Content-Type")
            return False

        return True

    def do_GET(self):
        self.handle_request("GET")

    def do_POST(self):
        if self.parse_data():
            self.handle_request("POST")

    def do_PUT(self):
        if self.parse_data():
            self.handle_request("PUT")

    def do_DELETE(self):
        self.handle_request("DELETE")

    def do_PATCH(self):
        if self.parse_data():
            self.handle_request("PATCH")

    def _handle_request(self, handler, kwargs):
        response = handler(self, **kwargs)
        if response is None:
            self.send_response(200)
            self.end_headers()
        elif isinstance(response, dict):
            json_text = json.dumps(response)
            encoded = json_text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        elif isinstance(response, str):
            self.send_response(200)
            encoded = response.encode("utf-8")
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        elif isinstance(response, RequestHandler.Response):
            self.send_response(response.status_code or 200)
            if response.headers:
                for key, value in response.headers.items():
                    self.send_header(key, value)
            if response.json:
                json_text = json.dumps(response.json)
                encoded = json_text.encode("utf-8")
                if not response.headers or "Content-Type" not in response.headers:
                    self.send_header("Content-Type", "application/json")
                if not response.headers or "Content-Length" not in response.headers:
                    self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
            elif response.text:
                encoded = response.text.encode("utf-8")
                if not response.headers or "Content-Type" not in response.headers:
                    self.send_header("Content-Type", "text/plain")
                if not response.headers or "Content-Length" not in response.headers:
                    self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
            else:
                self.end_headers()
        else:
            raise RuntimeError(
                "Unknown response type, please use mockserver.make_response"
            )

    def handle_request(self, method):
        parsed_path = urlparse(self.path)
        base_path = parsed_path.path

        if base_path in self.routes and method in self.routes[base_path]:
            handler, kwargs = self.routes[base_path][method]
            self._handle_request(handler, kwargs)
        else:
            self.send_error(404)


class MockServer:
    def __init__(self, port):
        self.host = "0.0.0.0"
        self.port = port
        self.server = ThreadingHTTPServer((self.host, self.port), RequestHandler)
        self.thread = None

    def handler(self, path, method="GET", **kwargs):
        def decorator(f):
            def f_with_attributes(*args, **kwargs):
                f_with_attributes.times_called += 1
                return f(*args, **kwargs)

            f_with_attributes.times_called = 0
            f_with_attributes.host_port = self.host_port
            f_with_attributes.url = f"http://{self.host_port}{path}"

            if path not in RequestHandler.routes:
                RequestHandler.routes[path] = {}
            RequestHandler.routes[path][method] = (f_with_attributes, kwargs)
            return f_with_attributes

        return decorator

    def up(self):
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def down(self):
        if not self.thread:
            return
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    @property
    def host_port(self):
        return f"{self.host}:{self.port}"

    @property
    def base_url(self):
        return f"http://{self.host_port}"

    def url_for(self, path):
        return f"http://{self.host}:{self.port}{path}"

    def cleanup(self):
        RequestHandler.routes.clear()

    def make_response(
        self, *, json=None, text=None, status=200, headers=None
    ) -> RequestHandler.Response:
        return RequestHandler.Response(
            json=json, text=text, status_code=status, headers=headers
        )
