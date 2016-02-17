import yson
from common import generate_uuid, get_version
from http import get_proxy_url, get_api_version, get_token

import yt.packages
from tornado.httpclient import HTTPClient, AsyncHTTPClient, HTTPRequest, HTTPError
from tornado.httputil import HTTPHeaders
from tornado.ioloop import IOLoop

from copy import deepcopy
import sys
import os
import tty
import termios
import fcntl
import struct
import signal
import datetime

class JobShell(object):
    def __init__(self, job_id, client=None):
        self.job_id = job_id
        self.shell_id = None
        self.terminating = False
        self.saved_tc = termios.tcgetattr(sys.stdin.fileno())
        self.width, self.height = self._terminal_size()
        self.sync = HTTPClient()
        self.async = AsyncHTTPClient()

        proxy = "http://{0}/api/{1}"\
            .format(get_proxy_url(client=client), get_api_version(client=client))
        token = get_token(client=client).strip()

        headers = HTTPHeaders()
        headers["Authorization"] = "OAuth " + token
        headers["User-Agent"] = "Python wrapper " + get_version()
        headers["X-YT-Header-Format"] = "<format=text>yson"
        headers["X-YT-Output-Format"] = "yson"
        self.req = HTTPRequest(proxy + "/run_job_shell", method="POST", headers=headers, body="", request_timeout=60)

    def _prepare_request(self, operation, keys=None, term=None):
        req = deepcopy(self.req)
        width, height = self._terminal_size()
        command = {
            "job_id": self.job_id,
            "operation": operation,
            "height": height,
            "width": width,
        }
        if keys:
            command["keys"] = keys.encode("hex")
        if term:
            command["term"] = term
        if self.shell_id:
            command["shell_id"] = self.shell_id
        req.headers["X-YT-Parameters"] = yson.dumps(command)
        req.headers["X-YT-Correlation-Id"] = generate_uuid()
        req.headers["X-YT-Date"] = datetime.datetime.now().ctime()
        return req

    def _do_request(self, command, callback=None, keys=None, term=None):
        req = self._prepare_request(command, keys, term)
        if callback:
            self.async.fetch(req, callback=callback)
        else:
            try:
                rsp = yson.loads(self.sync.fetch(req).body)
                return rsp
            except HTTPError as e:
                self._on_http_error(e)
        return None

    def _terminal_size(self):
        h, w, hp, wp = struct.unpack("HHHH", fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack("HHHH", 0, 0, 0, 0)))
        return w, h

    def _on_http_error(self, e):
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.saved_tc)
        print "Error:", e
        if e.response:
            if "X-YT-Parameters" in e.response.request.headers:
                print "Request:", e.response.request.headers["X-YT-Parameters"]
            else:
                print "Request:", e.response.request
            if "X-Yt-Response-Message" in e.response.headers:
                print "Response:", e.response.headers["X-Yt-Response-Message"]
            else:
                print "Response:", e.response
        IOLoop.current().stop()

    def _on_update_response(self, rsp):
        if rsp.error:
            if not self.terminating:
                self._on_http_error(rsp.error)

    def _on_poll_response(self, rsp):
        if rsp.error:
            if not self.terminating:
                self._on_http_error(rsp.error)
            return
        rsp = yson.loads(rsp.body)
        sys.stdout.write(rsp["output"])
        sys.stdout.flush()
        self._poll_shell()

    def _spawn_shell(self):
        rsp = self._do_request("spawn", term=os.environ["TERM"])
        if rsp and "shell_id" in rsp:
            self.shell_id = rsp["shell_id"]

    def _poll_shell(self):
        self._do_request("poll", callback=self._on_poll_response)

    def _update_shell(self, keys=None):
        self._do_request("update", keys=keys, callback=self._on_update_response)

    def _terminate_shell(self):
        self._do_request("terminate")

    def _resize_window(self):
        width, height = self._terminal_size()
        if self.width != width or self.height != height:
            self.width = width
            self.height = height
            self._update_shell()

    def _on_timer(self):
        ioloop = IOLoop.current()
        ioloop.add_callback(self._resize_window)
        ioloop.add_timeout(datetime.timedelta(seconds=5), self._on_timer)

    def _terminate(self, reason=None):
        self.terminating = True
        IOLoop.current().stop()

        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.saved_tc)
        print("")
        if reason:
            print(reason)
        sys.stdout.flush()
        self._terminate_shell()

    def _on_signal(self, sig, frame):
        ioloop = IOLoop.current()
        if sig == signal.SIGWINCH:
            ioloop.add_callback_from_signal(self._resize_window)
        if sig == signal.SIGHUP:
            ioloop.add_callback_from_signal(self._terminate, "Connection lost")
        if sig == signal.SIGTERM:
            ioloop.add_callback_from_signal(self._terminate, "Terminated by signal")

    def _on_keyboard_input(self, fd, events):
        try:
            keys = sys.stdin.read()
        except (IOError):
            pass
        if "\6" in keys:
            self._terminate("Terminated by user request")
        else:
            self._update_shell(keys)

    def run(self):
        self._spawn_shell()
        if not self.shell_id:
            return

        print "Use ^F to terminate shell."
        sys.stdout.flush()
        try:
            tty.setraw(sys.stdin)
            stdin = sys.stdin.fileno()
            fcntl.fcntl(stdin, fcntl.F_SETFL, fcntl.fcntl(stdin, fcntl.F_GETFL) | os.O_NONBLOCK)

            ioloop = IOLoop.current()
            ioloop.add_handler(stdin, self._on_keyboard_input, IOLoop.READ)
            ioloop.add_timeout(datetime.timedelta(seconds=1), self._on_timer)
            signal.signal(signal.SIGWINCH, self._on_signal)
            signal.signal(signal.SIGHUP, self._on_signal)
            signal.signal(signal.SIGTERM, self._on_signal)
            self._poll_shell()

            IOLoop.current().start()
        finally:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.saved_tc)
            IOLoop.current().stop()
