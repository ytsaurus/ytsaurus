#!/usr/bin/python

import yt.wrapper as yt

try:
    from yt.fennel.version import VERSION
except ImportError:
    VERSION="unknown"

from tornado import ioloop
from tornado import iostream
from tornado import gen
from tornado import tcpclient
from tornado import options

try:
    from raven.handlers.logging import SentryHandler
except ImportError:
    pass

import requests

import atexit
import socket
import struct
import logging
import json
import datetime
import time
import sys
import gzip
import StringIO
import collections


DEFAULT_TABLE_NAME = "//sys/scheduler/event_log"
DEFAULT_ADVICER_URL = "http://cellar-t.stat.yandex.net/advise"
DEFAULT_CHUNK_SIZE = 4000
DEFAULT_SERVICE_ID = "yt"
DEFAULT_SOURCE_ID = "tramsmm43"

CHUNK_HEADER_FORMAT = "<QQQ"
CHUNK_HEADER_SIZE = struct.calcsize(CHUNK_HEADER_FORMAT)


log = logging.getLogger("Fennel")


def sleep_future(seconds, io_loop=None):
    future = gen.Future()
    io_loop = io_loop or ioloop.IOLoop.instance()
    def callback():
        log.debug("Setting sleep_future future")
        future.set_result(None)

    io_loop.call_later(seconds, callback)
    return future


class ExceptionLoggingContext(object):
    def __init__(self, logger):
        self._logger = logger

    def __enter__(self):
        pass

    def __exit__(self, typ, value, tb):
        if value is not None:
            self._logger.error("Uncaught exception", exc_info=(typ, value, tb))


class EventLog(object):
    log = logging.getLogger("EventLog")

    class NotEnoughDataError(RuntimeError):
        pass

    def __init__(self, yt, table_name=None):
        self.yt = yt
        self._table_name = table_name or "//tmp/event_log"
        self._archive_table_name = self._table_name + ".archive"
        self._number_of_first_row_attr = "{0}/@number_of_first_row".format(self._table_name)
        self._row_to_save_attr = "{0}/@row_to_save".format(self._table_name)
        self._row_count = "{0}/@row_count".format(self._table_name)

    def get_data(self, begin, count):
        with self.yt.Transaction():
            rows_removed = self.yt.get(self._number_of_first_row_attr)
            begin -= rows_removed

            result = []
            if begin < 0:
                self.log.warning("%d < 0", begin)
                archive_row_count = self.yt.get("{0}/@row_count".format(self._archive_table_name))
                archive_begin = archive_row_count + begin
                result.extend([item for item in self.yt.read_table(yt.TablePath(
                    self._archive_table_name,
                    start_index=archive_begin,
                    end_index=archive_begin + count), format="json", raw=False)])

            self.log.debug("Reading %s event log. Begin: %d, count: %d",
                self._table_name,
                begin,
                count)
            result.extend([item for item in self.yt.read_table(yt.TablePath(
                self._table_name,
                start_index=begin,
                end_index=begin + count), format="json", raw=False)])
            self.log.debug("Reading is finished")
        if len(result) != count:
            raise EventLog.NotEnoughDataError("Not enough data. Got only {0} rows".format(len(result)))
        return result

    def truncate(self, count):
        with self.yt.Transaction():
            first_row = self.yt.get(self._number_of_first_row_attr)
            first_row += count
            self.yt.set(self._number_of_first_row_attr, first_row)
            self.yt.run_erase(yt.TablePath(
                self._table_name,
                start_index=0,
                end_index=count))

    def monitor(self, threshold):
        with self.yt.Transaction():
            first_row = self.yt.get(self._number_of_first_row_attr)
            row_to_save = int(self.get_next_row_to_save())
            real_row_to_save = row_to_save - first_row

            row_count = self.yt.get(self._row_count)

            lag = row_count - real_row_to_save
            if lag > threshold:
                sys.stdout.write("2;  Lag equals to: %d\n" % (lag,))
            else:
                sys.stdout.write("0; Lag equals to: %d\n" % (lag,))

    def archive(self, count = None):
        try:
            self.log.debug("Archive table has %d rows", yt.get(self._archive_table_name + "/@row_count"))
        except:
            pass

        self.log.info("%d rows has been requested to archive", count)

        desired_chunk_size = 2 * 1024 ** 3
        ratio = 0.137
        data_size_per_job = max(1, int(desired_chunk_size / ratio))

        count = count or yt.get(self._table_name + "/@row_count")
        max_count = yt.get(self._row_to_save_attr) - yt.get(self._number_of_first_row_attr)

        if count > max_count:
            self.log.info("One of rows which are requested to archive is not pushed to LogBroker yet. Set count to %d", max_count)
            count = max_count

        self.log.info("Archive %s rows from event log", count)

        partition = yt.TablePath(
            self._table_name,
            start_index=0,
            end_index=count)

        tries = 0
        finished = False
        backoff_time = 5
        while not finished:
            try:
                with self.yt.Transaction():
                    self.yt.create_table(
                        self._archive_table_name,
                        attributes={
                            "erasure_codec": "lrc_12_2_2",
                            "compression_codec": "gzip_best_compression"
                        },
                        ignore_existing=True)

                    self.log.info("Run merge...")
                    self.yt.run_merge(
                        source_table=partition,
                        destination_table=yt.TablePath(self._archive_table_name, append=True),
                        mode="ordered",
                        compression_codec="gzip_best_compression",
                        spec={
                            "combine_chunks": "true",
                            "force_transform": "true",
                            "data_size_per_job": data_size_per_job,
                            "job_io": {
                                "table_writer": {
                                    "desired_chunk_size": desired_chunk_size
                                }
                            }
                        }
                    )
                    finished = True
            except yt.common.YtError as e:
                self.log.error("Unhandled exception", exc_info=True)
                self.log.info("Retry again in %d seconds...", backoff_time)
                time.sleep(backoff_time)
                tries += 1
                backoff_time = min(backoff_time * 2, 180)

        self.log.info("Truncate event log...")

        tries = 0
        finished = False
        backoff_time = 5
        while not finished:
            try:
                with self.yt.Transaction():
                    first_row = self.yt.get(self._number_of_first_row_attr)
                    first_row += count
                    self.yt.run_erase(partition)
                    self.yt.set(self._number_of_first_row_attr, first_row)
                finished = True
            except yt.common.YtError as e:
                self.log.error("Unhandled exception", exc_info=True)

                self.log.info("Retry again in %d seconds...", backoff_time)
                time.sleep(backoff_time)
                tries += 1
                backoff_time = min(backoff_time * 2, 180)

        try:
            self.log.debug("Archive table has %d rows", yt.get(self._archive_table_name + "/@row_count"))
        except:
            pass


    def set_next_row_to_save(self, row_number):
        self.yt.set(self._row_to_save_attr, row_number)

    def get_next_row_to_save(self):
        return self.yt.get(self._row_to_save_attr)

    def initialize(self):
        with self.yt.Transaction():
            if not self.yt.exists(self._row_to_save_attr):
                self.yt.set(self._row_to_save_attr, 0)
            if not self.yt.exists(self._number_of_first_row_attr):
                self.yt.set(self._number_of_first_row_attr, 0)


def gzip_compress(text):
    out = StringIO.StringIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(text)
    return out.getvalue()


def gzip_decompress(text):
    infile = StringIO.StringIO()
    infile.write(text)
    with gzip.GzipFile(fileobj=infile, mode="r") as f:
        f.rewind()
        return f.read()


def normilize_timestamp(ts):
    dt = datetime.datetime.strptime(ts.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    return dt.isoformat(' ')


ESCAPE_MAP = {
    "\0": "\\0",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\\": "\\\\",
    "=": "\\="
}

UNESCAPE_MAP = {
    "0": "\0",
    "r": "\r",
    "n": "\n",
    "t": "\t",
    "\\": "\\",
    "=": "="
}

TSKV_KEY_ESCAPE = frozenset(['\0', '\r', '\n', '\t', '\\', '='])
TSKV_VALUE_ESCAPE = frozenset(['\0', '\r', '\n', '\t', '\\'])

def escape_encode(line, escape_chars=TSKV_VALUE_ESCAPE):
    result = ""
    start = 0
    index = 0
    for c in line:
        if c in escape_chars:
            result += line[start:index]
            result += ESCAPE_MAP[c]
            start = index + 1

        index += 1
    result += line[start:index]
    return result

def escape_decode(line):
    result = ""
    previousIsSlash = False
    for c in line:
        if previousIsSlash:
            result += UNESCAPE_MAP[c]
            previousIsSlash = False
        else:
            if c == '\\':
                previousIsSlash = True
            else:
                result += c
    return result


def convert_to(row):
    result = "tskv"

    for key, value in row.iteritems():
        if isinstance(value, basestring):
            pass
        else:
            value = json.dumps(value)

        result += "\t" + escape_encode(key, escape_chars=TSKV_KEY_ESCAPE) \
          + "=" + escape_encode(value, escape_chars=TSKV_VALUE_ESCAPE);

    return result

def convert_from(converted_row):
    result = dict()
    for kv in converted_row.split('\t')[1:]:
        key, value = kv.split('=', 1)
        try:
            value = json.loads(value)
        except ValueError:
            pass
        result[key] = value
    return result


def serialize_chunk(chunk_id, seqno, lines, data):
    serialized_data = struct.pack(CHUNK_HEADER_FORMAT, chunk_id, seqno, lines)
    serialized_data += gzip_compress("\n".join([convert_to(row) for row in data]))
    return serialized_data


def parse_chunk(serialized_data):
    serialized_data = serialized_data.strip()

    index = serialized_data.find("\r\n")
    assert index != -1
    index += len("\r\n")

    chunk_id, seqno, lines = struct.unpack(CHUNK_HEADER_FORMAT, serialized_data[index:index + CHUNK_HEADER_SIZE])
    index += CHUNK_HEADER_SIZE

    decompressed_data = gzip_decompress(serialized_data[index:])

    data = []
    for line in decompressed_data.split("\n"):
        data.append(convert_from(line))

    return data

def get_endpoint(self, advicer_url):
    log.info("Getting adviced logbroker endpoint...")
    response = requests.get(advicer_url, headers={"ClientHost": socket.getfqdn()})
    if not response.ok:
        log.error("Unable to get adviced logbroker endpoint")
        return None
    host = response.text.strip()

    log.info("Adviced endpoint: %s", host)
    return (host, 80)

def _pre_process(data):
    return [_transform_record(record) for record in data]

def _transform_record(record):
    try:
        record["timestamp"] = normilize_timestamp(record["timestamp"])
        record["cluster_name"] = "CLUSTER_NAME"
        record["tskv_format"] = "LOG_NAME"
        record["timezone"] = "+0000"
    except:
        log.error("Unable to transform record: %r", record)
        raise
    return record


class LogBroker(object):
    log = logging.getLogger("LogBroker")

    def __init__(self, service_id, source_id, io_loop=None, connection_factory=None):
        self._service_id = service_id
        self._source_id = source_id

        self._session = None
        self._push = None

        self._save_chunk_futures = dict()
        self._last_acked_seqno = None
        self._stopped = False

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection_factory = connection_factory or tcpclient.TCPClient(io_loop=self._io_loop)

    @gen.coroutine
    def connect(self, hostname, timeout=None):
        assert self._session is None
        assert self._push is None

        self._session = SessionStream(service_id=self._service_id, source_id=self._source_id, io_loop=self._io_loop, connection_factory=self._connection_factory)
        self.log.info("Connect session stream")
        session_id = yield self._session.connect((hostname, 80))
        self._update_last_acked_seqno(int(self._session.get_attribute("seqno")))

        self._push = PushStream(io_loop=self._io_loop, connection_factory=self._connection_factory)
        self.log.info("Connect push stream")
        yield self._push.connect((hostname, 9000), session_id=session_id)

        self._io_loop.add_callback(self.read_session)
        raise gen.Return(self._last_acked_seqno)

    def save_chunk(self, seqno, data, timeout=None):
        assert not seqno in self._save_chunk_futures

        serialized_data = serialize_chunk(0, seqno, 0, data)
        self._push.write_chunk(serialized_data)
        f = gen.Future()
        self._save_chunk_futures[seqno] = f
        return f

    @gen.coroutine
    def read_session(self):
        with ExceptionLoggingContext(self.log):
            while not self._stopped:
                try:
                    message = yield self._session.read_message()
                except (RuntimeError, IOError) as e:
                    self._abort(e)
                else:
                    self.log.debug("Get %r message", message)
                    if message.type == "ping":
                        pass
                    elif message.type == "skip":
                        skip_seqno = message.attributes["seqno"]
                        f = self._save_chunk_futures.pop(skip_seqno, None)
                        if f:
                            if skip_seqno > self._last_acked_seqno:
                                self._update_last_acked_seqno(skip_seqno)
                            f.set_result(self._last_acked_seqno)
                    elif message.type == "ack":
                        assert self._last_acked_seqno <= message.attributes["seqno"]

                        self._update_last_acked_seqno(message.attributes["seqno"])
                        self._set_futures(self._last_acked_seqno)

    def _abort(self, e):
        self.log.info("Abort LogBroker client", exc_info=e)
        if not self._stopped:
            self._set_futures(e)
            self.stop()

        assert len(self._save_chunk_futures) == 0

    def _set_futures(self, future_value):
        with ExceptionLoggingContext(self.log):
            is_exception = issubclass(type(future_value), Exception)
            if is_exception:
                self.log.debug("Set all futures to exception %r", future_value)
                for key, value in self._save_chunk_futures.iteritems():
                    value.set_exception(future_value)

                self._save_chunk_futures.clear()
            else:
                seqnos = self._save_chunk_futures.keys()
                for seqno in seqnos:
                    if seqno <= future_value:
                        f = self._save_chunk_futures.pop(seqno)
                        f.set_result(future_value)

    def _update_last_acked_seqno(self, value):
        self.log.debug("Update last acked seqno. Old: %s. New: %s", self._last_acked_seqno, value)
        self._last_acked_seqno = value

    def stop(self):
        assert len(self._save_chunk_futures) == 0
        self.log.info("Stop push stream...")
        self._push.stop()
        self._push = None
        self.log.info("Stop session stream...")
        self._session.stop()
        self._session = None
        self._stopped = True


class SessionEnd(RuntimeError):
    pass

class BadProtocol(RuntimeError):
    pass


SessionMessage = collections.namedtuple("SessionMessage", ["type", "attributes"])


class SessionStream(object):
    log = logging.getLogger("SessionStream")

    def __init__(self, service_id=None, source_id=None, logtype=None, io_loop=None, connection_factory=None):
        self._id = None
        self._attributes = None
        self._iostream = None

        self._logtype = logtype
        self._service_id = service_id or DEFAULT_SERVICE_ID
        self._source_id = source_id or DEFAULT_SOURCE_ID

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection_factory = connection_factory or tcpclient.TCPClient(io_loop=self._io_loop)

    @gen.coroutine
    def connect(self, endpoint, timeout=None):
        while True:
            try:
                self.log.info("Create a session. Endpoint: %s", endpoint)

                self._iostream = yield self._connection_factory.connect(endpoint[0], endpoint[1])

                self._iostream.write(
                    "GET /rt/session?"
                    "ident={ident}&"
                    "sourceid={source_id}&"
                    "logtype={logtype} "
                    "HTTP/1.1\r\n"
                    "Host: {host}\r\n"
                    "Accept: */*\r\n\r\n".format(
                        ident=self._service_id,
                        source_id=self._source_id,
                        logtype=self._logtype,
                        host=endpoint[0])
                    )

                self.log.info("The session stream has been created")
                metadata_raw = yield self._iostream.read_until("\r\n\r\n", max_bytes=1024*1024)

                self.log.debug("Parse response %s", metadata_raw)
                self.parse_metadata(metadata_raw[:-4])

                if not "seqno" in self._attributes:
                    self.log.error("There is no seqno header in session response")
                    raise BadProtocol("There is no seqno header in session response")
                if not "session" in self._attributes:
                    self.log.error("There is no seqno header in session response")
                    raise BadProtocol("There is no seqno header in session response")

                self._id = self._attributes["session"]

                raise gen.Return(self._id)
            except (IOError, BadProtocol) as e:
                self.log.error("Error occured. Try reconnect...", exc_info=True)
                yield sleep_future(1.0, self._io_loop)
            except gen.Return:
                raise
            except:
                self.log.error("Unhandled exception", exc_info=True)
                self.stop()
                raise

    def stop(self):
        if self._iostream is not None:
            self._iostream.close()
            self._iostream = None

    def parse_metadata(self, data):
        attributes = {}
        for index, line in enumerate(data.split("\n")):
            if index > 0:
                key, value = line.split(":", 1)
                attributes[key.strip().lower()] = value.strip()
        self._attributes = attributes

    def get_attribute(self, name):
        return self._attributes[name]

    @gen.coroutine
    def read_message(self, timeout=None):
        try:
            headers_raw = yield self._iostream.read_until("\r\n", max_bytes=4*1024)
            try:
                body_size = int(headers_raw, 16)
            except ValueError:
                self.log.error("[%s] Bad HTTP chunk header format", self._id)
                raise BadProtocol()
            if body_size == 0:
                self.log.error("[%s] HTTP response is finished", self._id)
                data = yield self._iostream.read_until_close()
                self.log.debug("[%s] Session trailers: %s", self._id, data)
                raise SessionEnd()
            else:
                data = yield self._iostream.read_bytes(body_size + 2)
                self.log.debug("[%s] Process status: %s", self._id, data.strip())
                raise gen.Return(self._parse(data.strip()))
        except gen.Return:
            raise
        except:
            self.stop()
            raise

    def _parse(self, line):
        if line.startswith("ping"):
            return SessionMessage("ping", {})
        elif line.startswith("eof"):
            return SessionMessage("eof", {})

        attributes = {}
        records = line.split()

        if records[0] == "skip":
            type_ = records[0]
            records = records[1:]
        else:
            type_ = "ack"

        for record in records[1:]:
            try:
                key, value = record.split("=", 1)
                value = int(value)
            except ValueError:
                raise BadProtocol()
            else:
                attributes[key] = value
        return SessionMessage(type_, attributes)


class PushStream(object):
    log = logging.getLogger("PushStream")

    def __init__(self, io_loop=None, connection_factory=None):
        self._iostream = None
        self._session_id = None

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection_factory = connection_factory or tcpclient.TCPClient(io_loop=self._io_loop)

    @gen.coroutine
    def connect(self, endpoint, session_id, timeout=None):
        self._session_id = session_id

        self.log.info("Create a push channel for session %s. Endpoint: %s", self._session_id, endpoint)

        self._iostream = yield self._connection_factory.connect(endpoint[0], endpoint[1])
        self._iostream.write(
            "PUT /rt/store HTTP/1.1\r\n"
            "Host: {host}\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Encoding: gzip\r\n"
            "Transfer-Encoding: chunked\r\n"
            "RTSTreamFormat: v2le\r\n"
            "Session: {session_id}\r\n"
            "\r\n".format(
                host=endpoint[0],
                session_id=self._session_id)
            )
        self.log.info("The push stream for session %s has been created", self._session_id)

        self._iostream.read_until_close(callback=self._post_close, streaming_callback=self._dump_output)
        raise gen.Return()

    def stop(self):
        if self._iostream is not None:
            self._iostream.close()
            self._iostream = None

    def write_chunk(self, serialized_data):
        return self.write("{size:X}\r\n{data}\r\n".format(size=len(serialized_data), data=serialized_data))

    def write(self, data):
        self.log.debug("Write to push stream for session %s %d bytes", self._session_id, len(data))
        return self._iostream.write(data)

    def _dump_output(self, data):
        with ExceptionLoggingContext(self.log):
            self.log.debug("Received data from push stream for session %s: %s", self._session_id, data)

    def _post_close(self, data):
        with ExceptionLoggingContext(self.log):
            if data:
                self.log.debug("Received data from push stream for session %s: %s", self._session_id, data)
            self.log.debug("The push stream for session %s was closed", self._session_id)


class Application(object):
    log = logging.getLogger("Application")

    def __init__(self, proxy_path, logbroker_url, table_name, service_id, source_id):
        yt.config.set_proxy(proxy_path)

        self._last_acked_seqno = None
        self._chunk_size = 4000
        self._logbroker_url = logbroker_url

        self._service_id = service_id
        self._source_id = source_id

        self._io_loop = ioloop.IOLoop.instance()
        self._event_log = EventLog(yt, table_name=table_name)
        self._log_broker = None

    def start(self):
        self._io_loop.add_callback(self._start)
        self._io_loop.start()

    @gen.coroutine
    def _start(self):
        while True:
            try:
                self._log_broker = LogBroker(self._service_id, self._source_id, io_loop=self._io_loop)

                hostname = self._get_hostname()
                self._last_acked_seqno = yield self._log_broker.connect(hostname)

                while True:
                    data = self._event_log.get_data(self._last_acked_seqno, self._chunk_size)
                    data = _pre_process(data)
                    self._last_acked_seqno = yield self._log_broker.save_chunk(self._last_acked_seqno + self._chunk_size, data)
            except Exception:
                self.log.error("Unhandled exception. Try to reconnect...", exc_info=True)
                self._log_broker.stop()
                self._log_broker = None
                yield sleep_future(1, self._io_loop)

    def _get_hostname(self):
        self.log.info("Getting adviced logbroker endpoint hostname...")
        response = requests.get("http://{0}/advice".format(self._logbroker_url), headers={"ClientHost": socket.getfqdn()})
        if not response.ok:
            raise RuntimeError("Unable to get adviced logbroker endpoint hostname")
        host = response.text.strip()

        self.log.info("Adviced endpoint hostname: %s", host)
        return host


def main(proxy_path, table_name, service_id, source_id, **kwargs):
    app = Application(proxy_path, "kafka01ft.stat.yandex.net", table_name, service_id, source_id)
    app.start()


def init(table_name, proxy_path, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    event_log.initialize()


def truncate(table_name, proxy_path, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    count = int(kwargs.get("count", 10**6))
    event_log.truncate(count)


def monitor(table_name, proxy_path, threshold, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    event_log.monitor(threshold)


def archive(table_name, proxy_path, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    count = kwargs.get("count", None)
    if count is not None:
        count = int(count)
    event_log.archive(count)


def run():
    options.define("table_name",
        metavar="PATH",
        default=DEFAULT_TABLE_NAME,
        help="[yt] path to scheduler event log")
    options.define("proxy_path", metavar="URL", help="[yt] url to proxy")
    options.define("chunk_size", default=DEFAULT_CHUNK_SIZE, help="size of chunk in rows")

    options.define("cluster_name", default="", help="[logbroker] name of source cluster")
    options.define("logtype", default="", help="[logbroker] log type")
    options.define("advicer_url", default=DEFAULT_ADVICER_URL, help="[logbroker] url to get adviced kafka endpoint")
    options.define("service_id", default=DEFAULT_SERVICE_ID, help="[logbroker] service id")
    options.define("source_id", default=DEFAULT_SOURCE_ID, help="[logbroker] source id")

    options.define("count", default=None, help="number of rows to truncate")

    options.define("threshold", default=10**6, help="threshold of lag size to generate error")

    options.define("init", default=False, help="init and exit")
    options.define("truncate", default=False, help="truncate and exit")
    options.define("monitor", default=False, help="output status and exit")
    options.define("version", default=False, help="output version and exit")
    options.define("archive", default=False, help="archive and exit")

    options.define("log_dir", metavar="PATH", default="/var/log/fennel", help="log directory")

    options.define("sentry_endpoint", default="", help="sentry endpoint")

    options.parse_command_line()

    if options.options["version"]:
        sys.stdout.write("Version: {0}\n".format(VERSION))
        return

    sentry_endpoint = options.options["sentry_endpoint"]
    if sentry_endpoint:
        root_logger = logging.getLogger("")
        sentry_handler = SentryHandler(sentry_endpoint)
        sentry_handler.setLevel(logging.ERROR)
        root_logger.addHandler(sentry_handler)

    logging.debug("Started. Version: %s", VERSION)

    @atexit.register
    def log_exit():
        logging.debug("Exited")

    if options.options.truncate:
        func = truncate
    elif options.options.init:
        func = init
    elif options.options.monitor:
        func = monitor
    elif options.options.archive:
        func = archive
    else:
        func = main

    try:
        func(**options.options.as_dict())
    except Exception:
        logging.error("Unhandled exception: ", exc_info=True)


if __name__ == "__main__":
    run()
