from __future__ import print_function

from . import yson
from .common import generate_uuid, get_version, YtError, get_binary_std_stream
from .errors import YtResponseError
from .config import get_backend_type
from .http_helpers import get_proxy_url, get_api_version, get_token

from yt.common import to_native_str
# yt.packages is imported here just to set sys.path for further loading of local tornado module
from yt.packages import PackagesImporter
with PackagesImporter():
    from tornado.httpclient import HTTPClient, AsyncHTTPClient, HTTPRequest, HTTPError
    from tornado.httputil import HTTPHeaders
    from tornado.ioloop import IOLoop
    # It is necessary to prevent local imports during runtime.
    import tornado.simple_httpclient

from copy import deepcopy
from binascii import hexlify
import sys
import os
import json
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
    def __init__(self, job_id, interactive=True, timeout=None, client=None):
        if not job_shell_supported:
            raise YtError("Job shell is not supported on your platform")
        if get_backend_type(client) != "http" or get_api_version(client) != "v3":
            raise YtError("Command run-job-shell requires http v3 backend.")

        self.job_id = job_id
        self.inactivity_timeout = timeout
        self.interactive = interactive
        self.client = client
        self.shell_id = None
        self.terminating = False
        self._save_termios()
        if self.interactive:
            self.width, self.height = self._terminal_size()
        self.key_buffer = b""
        self.input_offset = 0
        self.ioloop = IOLoop.current() if hasattr(IOLoop, "current") else IOLoop.instance()
        self.sync = HTTPClient()
        self.async = AsyncHTTPClient()
        self.terminal_mode = True

        proxy_url = get_proxy_url(client=client)
        proxy = "http://{0}/api/{1}"\
            .format(proxy_url, get_api_version(client=client))
        self.environment = ["YT_PROXY=" + proxy_url]
        token = get_token(client=client)
        if token is not None:
            self.environment.append("YT_TOKEN=" + token)

        headers = HTTPHeaders()
        if token:
            headers["Authorization"] = "OAuth " + token
        headers["User-Agent"] = "Python wrapper " + get_version()
        headers["X-YT-Header-Format"] = "<format=text>yson"
        headers["X-YT-Output-Format"] = "yson"
        self.req = HTTPRequest(proxy + "/poll_job_shell", method="POST", headers=headers, body="", request_timeout=60)

    def _save_termios(self):
        if self.interactive:
            self.saved_tc = termios.tcgetattr(sys.stdin.fileno())

    def _restore_termios(self):
        if self.interactive:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.saved_tc)

    def _prepare_request(self, operation, keys=None, input_offset=None, term=None,
                         height=None, width=None, command=None):
        req = deepcopy(self.req)
        if self.interactive and (not height or not width):
            width, height = self._terminal_size()
        parameters = {
            "operation": operation,
        }
        if operation == "spawn":
            if self.inactivity_timeout is not None:
                parameters["inactivity_timeout"] = self.inactivity_timeout
            if command:
                parameters["command"] = command
            parameters["environment"] = self.environment
        if height is not None:
            parameters["height"] = height
        if width is not None:
            parameters["width"] = width
        if keys is not None:
            parameters["keys"] = to_native_str(hexlify(keys))
        if input_offset is not None:
            parameters["input_offset"] = input_offset
        if term is not None:
            parameters["term"] = term
        if self.shell_id:
            parameters["shell_id"] = self.shell_id
        request = {
            "job_id": self.job_id,
            "parameters": parameters
        }
        req.headers["X-YT-Parameters"] = yson.dumps(request, yson_format="text")
        req.headers["X-YT-Correlation-Id"] = generate_uuid()
        return req

    def make_request(self, operation, callback=None, keys=None, input_offset=None, term=None,
                     height=None, width=None, command=None):
        req = self._prepare_request(operation, keys=keys, input_offset=input_offset, term=term,
                                    height=height, width=width, command=command)
        if callback:
            self.async.fetch(req, callback=callback)
        else:
            try:
                rsp = yson.loads(self.sync.fetch(req).body)
                if rsp and "shell_id" in rsp:
                    self.shell_id = rsp["shell_id"]
                return rsp
            except HTTPError as err:
                self._on_http_error(err)
        return None

    def _terminal_size(self):
        height, width, pixelHeight, pixelWidth = struct.unpack(
            "HHHH", fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack("HHHH", 0, 0, 0, 0)))
        return width, height

    def _on_http_error(self, err):
        self._restore_termios()
        if type(err) is HTTPError and hasattr(err, "response") and err.response:
            if "X-Yt-Error" in err.response.headers:
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

    def _on_update_response(self, rsp):
        if rsp.error:
            if not self.terminating:
                self._on_http_error(rsp.error)
            return
        rsp = yson.loads(rsp.body)
        if "consumed_offset" in rsp:
            consumed_offset = rsp["consumed_offset"]
            if consumed_offset > self.input_offset and consumed_offset <= self.input_offset + len(self.key_buffer):
                self.key_buffer = self.key_buffer[consumed_offset-self.input_offset:]
                self.input_offset = consumed_offset

    def _on_poll_response(self, rsp):
        if rsp.error:
            if not self.terminating:
                self._on_http_error(rsp.error)
                self._poll_shell()
            return
        rsp = yson.loads(rsp.body)
        sys.stdout.write(rsp["output"])
        sys.stdout.flush()
        self._poll_shell()

    def _spawn_shell(self, command=None):
        if command:
            self.terminal_mode = False
        rsp = self.make_request("spawn", term=os.environ["TERM"], command=command)
        if rsp and "shell_id" in rsp:
            self.shell_id = rsp["shell_id"]

    def _poll_shell(self):
        self.make_request("poll", callback=self._on_poll_response)

    def _update_shell(self, keys=None, input_offset=None):
        self.make_request("update", keys=keys, input_offset=input_offset, callback=self._on_update_response)

    def _terminate_shell(self):
        self.make_request("terminate")

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
        self.terminating = True
        self.ioloop.stop()

        self._restore_termios()
        print("")
        if reason:
            print(reason)
        sys.stdout.flush()
        self._terminate_shell()

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

        self._spawn_shell(command=command)
        if not self.shell_id:
            return

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
            self._poll_shell()

            self.ioloop.start()
        finally:
            self._restore_termios()
            self.ioloop.stop()
