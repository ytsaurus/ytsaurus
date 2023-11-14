from yt_odin.logging import TaskLoggerAdapter
from yt_odin.storage.async_storage_writer import AsyncStorageWriter

from yt.wrapper.common import MB
try:
    from yt.wrapper.retries import run_with_retries
except ImportError:
    from yt.wrapper.common import run_with_retries

from six import itervalues
from six.moves import socketserver

import logging
import logging.handlers
import pickle
import socket
import struct
import traceback
try:
    from cStringIO import StringIO
except ImportError:  # Python 3
    from io import StringIO
from collections import defaultdict
import os

UNKNOWN_STATE = -1
UNAVAILABLE_STATE = 0
PARTIALLY_AVAILABLE_STATE = 0.5
FULLY_AVAILABLE_STATE = 1
TERMINATED_STATE = 2
FAILED_STATE = 3

logger = logging.getLogger("Odin")


def _assert_or_die(value, message):
    try:
        assert value, message
    except AssertionError:
        logger.exception("Critical assertion failed")
        os._exit(1)


class TaskStatusRecord(object):
    def __init__(self, task_id, sender_pid, state=UNKNOWN_STATE, start_timestamp=None, service=None,
                 duration=None):
        self.task_id = task_id
        self.sender_pid = sender_pid
        self.state = state
        self.start_timestamp = start_timestamp
        self.service = service
        self.duration = duration

    def update(self, other):
        _assert_or_die(self.task_id == other.task_id, "task ids mismatch")
        if other.state != UNKNOWN_STATE:
            _assert_or_die(self.state == UNKNOWN_STATE, "state is not UNKNOWN_STATE")
            self.state = other.state
        if self.start_timestamp is None:
            self.start_timestamp = other.start_timestamp
        if self.service is not None and other.service is not None:
            _assert_or_die(self.service == other.service, "service names mismatch")
        if self.service is None:
            self.service = other.service
        if self.duration is None:
            self.duration = other.duration

    def __str__(self):
        return "<TaskStatusRecord:{}>".format(self.__dict__)

    def __repr__(self):
        return "<TaskStatusRecord:{}>".format(self.__dict__)


# ========== Server side ==============

def is_state_ok(state):
    return state == FULLY_AVAILABLE_STATE


def _terminate_logserver():
    logger.error("Killing logserver (pid: %d)", os.getpid())
    _assert_or_die(False, "_terminate_logserver() called")


class _RecordProcessor(object):
    def __init__(self, storage, max_write_batch_size, messages_max_size=None):
        self.storage_writer = AsyncStorageWriter(storage, max_write_batch_size)
        self.messages_max_size = messages_max_size
        self.formatter = logging.Formatter("%(asctime)s.%(msecs)d - %(levelname)s : %(message)s", "%Z%z %H:%M:%S")

        # Store records by task id.
        self.records = defaultdict(list)
        # Store status by task id.
        self.status = {}

    def add_log_record(self, record):
        self.records[record.odin_task_id].append(record)
        record_count = sum(len(value) for value in itervalues(self.records))
        logger.info("Received log record (task_id: %s, sender_pid: %d, record_count: %d)",
                    record.odin_task_id, record.process, record_count)

    def add_control_record(self, record):
        logger.info("Received control record: %s", record)
        task_logger = TaskLoggerAdapter(logger, record.task_id)

        if record.task_id in self.status:
            task_logger.info("Updating status %s with %s", self.status[record.task_id], record)
            self.status[record.task_id].update(record)
        else:
            self.status[record.task_id] = record

        status = self.status[record.task_id]
        if status.state is not UNKNOWN_STATE and status.start_timestamp is not None and status.service is not None:
            messages = self._serialize_log_records(self.records[status.task_id])
            truncated_string = "...\n(TRUNCATED)\n..."
            if self.messages_max_size is not None and \
                    len(messages) > self.messages_max_size + len(truncated_string):
                # NB: `repr(messages)` is to avoid newlines in the log message.
                task_logger.info("Truncating long check messages (task_id: %s, sender_pid: %d): %s",
                                 status.task_id, status.sender_pid, repr(messages))
                half_size = self.messages_max_size // 2
                last_bit = self.messages_max_size % 2
                messages = messages[:half_size + last_bit] + truncated_string \
                    + messages[len(messages) - half_size:]

            task_logger.info("Assemble check result and write it to the storage (task_id: %s, sender_pid: %d)",
                             status.task_id, status.sender_pid)
            record = dict(
                check_id=status.task_id,
                service=status.service,
                timestamp=status.start_timestamp,
                state=float(status.state),
                duration=status.duration,
                messages=messages)
            task_logger.debug("Check result record: %s", str(record))

            try:
                self.storage_writer.add_record(**record)
            except Exception:
                task_logger.exception("Failed to write record")
                _terminate_logserver()
            task_logger.info("Record assembled (byte_count: %d, task_id: %s, sender_pid: %d)",
                             len(messages), status.task_id, status.sender_pid)
            del self.status[status.task_id]
            del self.records[status.task_id]

    def _serialize_log_records(self, records):
        stream_handler = logging.StreamHandler(StringIO())
        stream_handler.setFormatter(self.formatter)
        for record in sorted(records, key=lambda rec: rec.created):
            stream_handler.handle(record)
        return stream_handler.stream.getvalue()


class _LoggingRequestHandler(socketserver.StreamRequestHandler):
    def __init__(self, *args, **kwargs):
        socketserver.StreamRequestHandler.__init__(self, *args, **kwargs)

    def _recv_n(self, sock, n):
        rest = n
        result = b""
        while rest > 0:
            delta = sock.recv(rest)
            if not delta:
                raise RuntimeError("Failed to read full message")
            result += delta
            rest = n - len(result)
        return result

    def _recv_all(self, sock):
        raw_len = self._recv_n(sock, 4)
        length = struct.unpack(">L", raw_len)[0]
        if not (0 < length < 10 * MB):
            raise RuntimeError("Message length is out of range: {}".format(length))
        return self._recv_n(sock, length)

    def handle(self):
        raw_data = self._recv_all(self.request)

        try:
            record = pickle.loads(raw_data)
        except Exception:
            logger.error("Failed to unpickle message: %s", repr(raw_data))
            raise

        if isinstance(record, TaskStatusRecord):
            self.server.record_processor.add_control_record(record)
        elif isinstance(record, dict):
            log_record = logging.makeLogRecord(record)
            self.server.record_processor.add_log_record(log_record)
        else:
            logger.error("Unknown record type in _LoggingRequestHandler: %s", record.__class__.__name__)

        self.request.shutdown(socket.SHUT_RDWR)
        self.request.close()


class LogrecordSocketReceiver(socketserver.UnixStreamServer):
    request_queue_size = 128

    def __init__(self, socket_path, storage, max_write_batch_size, messages_max_size=None,
                 die_on_error=False):
        socketserver.UnixStreamServer.__init__(self, socket_path, _LoggingRequestHandler)
        self.record_processor = _RecordProcessor(storage, max_write_batch_size, messages_max_size)
        self.die_on_error = die_on_error

    def handle_error(self, request, client_address):
        logger.exception("Exception happened during processing of request from '%s':\n%s",
                         str(client_address),
                         traceback.format_exc())
        if self.die_on_error:
            _terminate_logserver()


def run_logserver(socket_path, storage, max_write_batch_size, messages_max_size=None,
                  die_on_error=False):
    server = LogrecordSocketReceiver(socket_path, storage, max_write_batch_size, messages_max_size,
                                     die_on_error)
    server.allow_reuse_address = True
    server.serve_forever()


# ========== Client side ==============

class _ReliableSocketHandler(logging.handlers.SocketHandler):
    """Implements reliable log handler"""

    _RETRY_COUNT = 7
    _RETRY_BACKOFF = 5.0

    def __init__(self, socket_path):
        self.socket_path = socket_path
        # XXX(asaitgalin): No host and port, will use socket_path later.
        super(_ReliableSocketHandler, self).__init__(None, None)

    def _backoff_action(self, error, iteration, sleep_backoff):
        logger.warning(
            "Socket error occurred \"%s\" (pid: %d). Sleeping for %.2lf seconds before next retry (%d)",
            str(error),
            os.getpid(),
            sleep_backoff,
            iteration + 1,
        )

    def makeSocket(self, timeout=1):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if hasattr(s, "settimeout"):
            s.settimeout(timeout)
        s.connect(self.socket_path)
        return s

    def createSocket(self):
        def do_create():
            self.sock = self.makeSocket()

        run_with_retries(
            do_create,
            retry_count=self._RETRY_COUNT,
            backoff=self._RETRY_BACKOFF,
            exceptions=(socket.error,),
            backoff_action=self._backoff_action)

    def send(self, message):
        if self.sock is None:
            self.createSocket()

        def do_send():
            self.sock.sendall(message)

        run_with_retries(
            do_send,
            retry_count=self._RETRY_COUNT,
            backoff=self._RETRY_BACKOFF,
            exceptions=(socket.error,),
            backoff_action=self._backoff_action)

        super(_ReliableSocketHandler, self).close()


class OdinSocketHandler(_ReliableSocketHandler):
    """
        Implements Odin-specific socket handler for logs and control messages about checks.
        Intended to be used in Odin runner.
        Notice: to make task id appear in log message, you need to use TaskLoggerAdapter.
    """

    def send_control_message(self, **kwargs):
        assert "task_id" in kwargs, "Control message should have 'task_id' set"
        logger.info("Sending control message (%r)", kwargs)
        record = TaskStatusRecord(sender_pid=os.getpid(), **kwargs)
        data = pickle.dumps(record, protocol=2)
        header = struct.pack(">L", len(data))
        self.send(header + data)

    def emit(self, record):
        assert hasattr(record, "odin_task_id"), "LogRecord should have 'odin_task_id' attribute"
        super(OdinSocketHandler, self).emit(record)


class OdinCheckSocketHandler(_ReliableSocketHandler):
    """
        Like OdinSocketHandler, but has odin_task_id built-in at instantiation time
        and does not support control messages. Intended to be used in Odin check subprocesses.
        Notice: task id appears in log message automatically, no need to use TaskLoggerAdapter.
    """

    def __init__(self, socket_path, task_id):
        super(OdinCheckSocketHandler, self).__init__(socket_path)
        self._task_id = task_id

    def emit(self, record):
        assert not hasattr(record, "odin_task_id"), "LogRecord must not have 'odin_task_id' attribute"
        record.odin_task_id = self._task_id
        record.msg = "{}\t{}".format(self._task_id, record.msg)
        super(OdinCheckSocketHandler, self).emit(record)
