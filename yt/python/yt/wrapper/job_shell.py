from __future__ import print_function

from . import yson
from .common import generate_uuid, get_user_agent, YtError, get_binary_std_stream
from .errors import YtResponseError
from .config import get_backend_type
from .driver import get_api_version
from .http_helpers import get_proxy_address_url, get_token, make_request_with_retries

# yt.packages is imported here just to set sys.path for further loading of local tornado module
from yt.packages import PackagesImporter
try:
    with PackagesImporter():  # noqa
        from tornado.httpclient import HTTPClient, AsyncHTTPClient, HTTPRequest, HTTPError, HTTPResponse
        from tornado.httputil import HTTPHeaders
        from tornado.ioloop import IOLoop
        # It is necessary to prevent local imports during runtime.
        import tornado.simple_httpclient # noqa
    tornado_was_imported = True
except ImportError:
    tornado_was_imported = False

from binascii import hexlify
from io import FileIO
import sys
import os
import json
import random
import struct
import signal
import time

try:
    # These modules are usually missing on Windows
    import fcntl
    import tty
    import termios
    job_shell_supported = True
except ImportError:
    job_shell_supported = False


class JobShell(object):
    def __init__(self, job_id, shell_name=None, interactive=True, timeout=None, client=None):
        if not job_shell_supported:
            raise YtError("Job shell is not supported on your platform")
        if not tornado_was_imported:
            raise YtError("Job shell requires tornado module that is missing")
        if get_backend_type(client) != "http":
            raise YtError("Command run-job-shell requires http backend.")

        self.job_id = job_id
        self.shell_name = shell_name
        self.inactivity_timeout = timeout
        self.interactive = interactive
        self.yt_client = client
        self.shell_id = None
        self.terminating = False
        self._save_termios()
        if self.interactive:
            self.width, self.height = self._terminal_size()
        self.key_buffer = b""
        self.input_offset = 0
        self.ioloop = IOLoop.current() if hasattr(IOLoop, "current") else IOLoop.instance()
        if self.interactive:
            self.client = AsyncHTTPClient()
        else:
            self.client = HTTPClient()
        self.terminal_mode = True
        self.output = FileIO(sys.stdout.fileno(), mode='w', closefd=False)

        self.proxy_url = get_proxy_address_url(client=self.yt_client)
        self.api_version = get_api_version(client=client)

        self.environment = [b"YT_PROXY=" + bytes(self.proxy_url, "utf-8")]
        self.token = get_token(client=client)
        if self.token is not None:
            self.environment.append(b"YT_TOKEN=" + bytes(self.token, "utf-8"))

        self.current_proxy = None

    def _save_termios(self):
        if self.interactive:
            self.saved_tc = termios.tcgetattr(sys.stdin.fileno())

    def _restore_termios(self):
        if self.interactive:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.saved_tc)

    def _prepare_request(self, operation, keys=None, input_offset=None, term=None,
                         height=None, width=None, command=None):
        if self.current_proxy is None:
            control_proxies = make_request_with_retries(
                "get",
                "{0}/hosts?role=control".format(self.proxy_url),
                client=self.yt_client).json()
            if control_proxies:
                self.current_proxy = get_proxy_address_url(client=self.yt_client, replace_host=random.choice(control_proxies))
            else:
                self.current_proxy = self.proxy_url

        headers = HTTPHeaders()
        if self.token:
            headers["Authorization"] = "OAuth " + self.token
        headers["User-Agent"] = get_user_agent()
        headers["X-YT-Header-Format"] = "<format=text>yson"
        headers["X-YT-Output-Format"] = "yson"

        req = HTTPRequest(
            "{0}/api/{1}/poll_job_shell".format(self.current_proxy, self.api_version),
            method="POST",
            headers=headers,
            body="",
            request_timeout=60)

        if self.interactive and (not height or not width):
            width, height = self._terminal_size()
        parameters = {
            b"operation": bytes(operation, "utf-8"),
        }
        if operation == "spawn":
            if self.inactivity_timeout is not None:
                parameters[b"inactivity_timeout"] = self.inactivity_timeout
            if command:
                parameters[b"command"] = bytes(command, "utf-8")
            parameters[b"environment"] = self.environment
        if height is not None:
            parameters[b"height"] = height
        if width is not None:
            parameters[b"width"] = width
        if keys is not None:
            parameters[b"keys"] = hexlify(keys)
        if input_offset is not None:
            parameters[b"input_offset"] = input_offset
        if term is not None:
            parameters[b"term"] = bytes(term, "utf-8")
        if self.shell_id:
            parameters[b"shell_id"] = self.shell_id
        request = {
            b"job_id": bytes(self.job_id, "utf-8"),
            b"parameters": parameters
        }
        if self.shell_name is not None:
            request[b"shell_name"] = bytes(self.shell_name, "utf-8")
        req.headers["X-YT-Parameters"] = yson.dumps(request, yson_format="text", encoding=None)
        req.headers["X-YT-Correlation-Id"] = generate_uuid()
        return req

    def make_request(self, operation, callback=None, keys=None, input_offset=None, term=None,
                     height=None, width=None, command=None):
        if self.terminating:
            raise YtError("Shell has already terminated")
        if operation == "spawn" or self.shell_id is not None or not self.interactive:
            req = self._prepare_request(operation, keys=keys, input_offset=input_offset, term=term,
                                        height=height, width=width, command=command)
            if self.interactive:
                if tornado.version_info[0] < 6:
                    self.client.fetch(req, callback=callback)
                else:
                    def _tornado6_callback(f):
                        try:
                            res = f.result()
                        except HTTPError as err:
                            res = getattr(err, 'response', None)
                            if not res:
                                print("Unhandled tornado6 http exception: ", err)
                        except Exception as ex:
                            print("Unhandled tornado6 general exception: ", ex)
                            res = None

                        if res is None:
                            res = HTTPResponse(HTTPRequest(""), None, error=True)

                        callback(res)

                    future = self.client.fetch(req)
                    future.add_done_callback(_tornado6_callback)
            else:
                try:
                    rsp = yson.loads(self.client.fetch(req).body, encoding=None)
                    if get_api_version(self.yt_client) == "v4":
                        rsp = rsp[b"result"]
                    if rsp and b"shell_id" in rsp:
                        self.shell_id = rsp[b"shell_id"]
                    if operation == "terminate":
                        self.terminating = True
                    return rsp
                except HTTPError as err:
                    self._on_http_error(err)

    def _terminal_size(self):
        height, width, pixelHeight, pixelWidth = struct.unpack(
            "HHHH", fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack("HHHH", 0, 0, 0, 0)))
        return width, height

    def _on_http_error(self, err):
        self._restore_termios()
        self.current_proxy = None
        if type(err) is HTTPError and hasattr(err, "response") and err.response:
            if "X-Yt-Error" in err.response.headers:
                if sys.version_info.minor <= 5:
                    error = json.loads(err.response.headers["X-Yt-Error"], encoding=None)
                else:
                    error = json.loads(err.response.headers["X-Yt-Error"])
                yt_error = YtResponseError(error)
                if self.interactive:
                    if yt_error.is_shell_exited():
                        if self.terminal_mode:
                            print("Shell exited")
                    else:
                        print(yt_error)
                else:
                    raise yt_error
        elif self.interactive:
            print("Error:", err)
        else:
            raise err

        if self.interactive:
            self.ioloop.stop()

    def _on_spawn_response(self, rsp):
        if rsp.error:
            if not self.terminating:
                self._on_http_error(rsp.error)
            return
        rsp = yson.loads(rsp.body, encoding=None)
        if get_api_version(self.yt_client) == "v4":
            rsp = rsp[b"result"]
        if rsp and b"shell_id" in rsp:
            self.shell_id = rsp[b"shell_id"]
        self._poll_shell()

    def _on_update_response(self, rsp):
        if rsp.error:
            if not self.terminating:
                self._on_http_error(rsp.error)
            return
        rsp = yson.loads(rsp.body, encoding=None)
        if get_api_version(self.yt_client) == "v4":
            rsp = rsp[b"result"]

        consumed_offset = rsp.get(b"consumed_offset")
        if isinstance(consumed_offset, yson.YsonEntity):
            consumed_offset = None
        if consumed_offset is not None and self.input_offset < consumed_offset <= self.input_offset + len(self.key_buffer):
            self.key_buffer = self.key_buffer[consumed_offset - self.input_offset:]
            self.input_offset = consumed_offset

    def _on_poll_response(self, rsp):
        if rsp.error:
            if not self.terminating:
                self._on_http_error(rsp.error)
                self._poll_shell()
            return
        rsp = yson.loads(rsp.body, encoding=None)
        if get_api_version(self.yt_client) == "v4":
            rsp = rsp[b"result"]
        written = 0
        while written < len(rsp[b"output"]):
            chars = self.output.write(rsp[b"output"][written:])
            if chars is not None:
                written += chars
            else:
                # None means EAGAIN error on write.
                time.sleep(0.05)
        self._poll_shell()

    def _on_terminate_response(self, rsp):
        pass

    def _spawn_shell(self, command=None):
        if command:
            self.terminal_mode = False
        self.make_request("spawn", callback=self._on_spawn_response, term=os.environ["TERM"], command=command)

    def _poll_shell(self):
        self.make_request("poll", callback=self._on_poll_response)

    def _update_shell(self, keys=None, input_offset=None):
        self.make_request("update", callback=self._on_update_response, keys=keys, input_offset=input_offset)

    def _terminate_shell(self):
        self.make_request("terminate", callback=self._on_terminate_response)

    def _resize_window(self):
        width, height = self._terminal_size()
        if self.width != width or self.height != height:
            self.width = width
            self.height = height
            self._update_shell()

    def _on_timer(self):
        self.ioloop.add_callback(self._resize_window)
        self.ioloop.add_timeout(time.time() + 5, self._on_timer)

    def _terminate(self, reason=None):
        self._terminate_shell()
        self.terminating = True
        self.ioloop.stop()

        self._restore_termios()
        print("")
        if reason:
            print(reason)
        sys.stdout.flush()

    def _on_signal(self, sig, frame):
        if sig == signal.SIGWINCH:
            self.ioloop.add_callback_from_signal(self._resize_window)
        if sig == signal.SIGHUP:
            self.ioloop.add_callback_from_signal(self._terminate, "Connection lost")
        if sig == signal.SIGTERM:
            self.ioloop.add_callback_from_signal(self._terminate, "Terminated by signal")

    def _on_keyboard_input(self, fd, events):
        try:
            keys = get_binary_std_stream(sys.stdin).read()
        except (IOError):
            pass
        if b"\6" in keys:
            self._terminate("Terminated by user request")
        elif len(keys) > 0:
            self.key_buffer += keys
            self._update_shell(self.key_buffer, self.input_offset)

    def run(self, command=None):
        if not self.interactive:
            raise YtError("Run requires interactive shell")

        if self.terminal_mode:
            print("Use ^F to terminate shell.")
            sys.stdout.flush()
        try:
            tty.setraw(sys.stdin)
            stdin = sys.stdin.fileno()
            fcntl.fcntl(stdin, fcntl.F_SETFL, fcntl.fcntl(stdin, fcntl.F_GETFL) | os.O_NONBLOCK)

            self.ioloop.add_handler(stdin, self._on_keyboard_input, IOLoop.READ)
            self.ioloop.add_timeout(time.time() + 1, self._on_timer)
            signal.signal(signal.SIGWINCH, self._on_signal)
            signal.signal(signal.SIGHUP, self._on_signal)
            signal.signal(signal.SIGTERM, self._on_signal)

            self._spawn_shell(command=command)
            self.ioloop.start()
        finally:
            self._restore_termios()
            stdin = sys.stdin.fileno()
            fcntl.fcntl(stdin, fcntl.F_SETFL, fcntl.fcntl(stdin, fcntl.F_GETFL) & ~os.O_NONBLOCK)
            self.ioloop.stop()
