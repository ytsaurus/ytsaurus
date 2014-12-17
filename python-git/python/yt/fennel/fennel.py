#!/usr/bin/python

from yt.wrapper import errors
from yt.wrapper import client
from yt.wrapper import format
from yt.wrapper import table
from yt.wrapper.config import set_proxy

try:
    from yt.fennel.version import VERSION
except ImportError:
    VERSION="unknown"

import tornado
assert tornado.version_info > (4,)


from tornado import ioloop
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
DEFAULT_CHUNK_SIZE = 4000
DEFAULT_SERVICE_ID = "yt"
DEFAULT_LOG_NAME = "yt-scheduler-log"

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

    def __init__(self, yt, table_name):
        self.yt = yt
        self._table_name = table_name
        self._archive_table_name = self._table_name + ".archive"
        self._number_of_first_row_attr = "{0}/@number_of_first_row".format(self._table_name)
        self._row_count_attr = "{0}/@row_count".format(self._table_name)
        self._archive_row_count_attr = "{0}/@row_count".format(self._archive_table_name)

    def get_row_count(self):
        with self.yt.Transaction():
            first_row = self.yt.get(self._number_of_first_row_attr)
            row_count = self.yt.get(self._row_count_attr)
            return row_count + first_row

    def get_data(self, begin, count):
        with self.yt.Transaction():
            rows_removed = self.yt.get(self._number_of_first_row_attr)
            begin -= rows_removed

            result = []
            if begin < 0:
                self.log.error("Table index is less then 0: %d. Use archive table", begin)
                archive_row_count = self.yt.get(self._archive_row_count_attr)
                archive_begin = archive_row_count + begin
                result.extend(self.yt.read_table(table.TablePath(
                    self._archive_table_name,
                    start_index=archive_begin,
                    end_index=archive_begin + count), format="json", raw=False))

            self.log.debug("Reading %s event log. Begin: %d, count: %d",
                self._table_name,
                begin,
                count)
            result.extend(self.yt.read_table(table.TablePath(
                self._table_name,
                start_index=begin,
                end_index=begin + count), format="json", raw=False))
            self.log.debug("Reading is finished")
        if len(result) != count:
            raise EventLog.NotEnoughDataError("Not enough data. Got only {0} rows".format(len(result)))
        return result

    def archive(self, count=None):
        try:
            self.log.debug("Archive table has %d rows", self.yt.get(self._archive_row_count_attr))
        except Exception:
            pass

        self.log.info("%s rows has been requested to archive", count)

        desired_chunk_size = 2 * 1024 ** 3
        approximate_gzip_compression_ratio = 0.137
        data_size_per_job = max(1, int(desired_chunk_size / approximate_gzip_compression_ratio))

        count = count or self.yt.get(self._row_count_attr)
        self.log.info("Archive %s rows from event log", count)

        partition = table.TablePath(
            self._table_name,
            start_index=0,
            end_index=count)

        self.log.info("Ensuring %s table exists", self._archive_table_name)
        self.yt.create_table(
            self._archive_table_name,
            attributes={
                "erasure_codec": "lrc_12_2_2",
                "compression_codec": "gzip_best_compression"
            },
            ignore_existing=True)

        tries = 0
        finished = False
        backoff_time = 5
        while not finished:
            try:
                with self.yt.Transaction():
                    self.log.info("Run merge...")
                    self.yt.run_merge(
                        source_table=partition,
                        destination_table=table.TablePath(self._archive_table_name, append=True),
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
            except errors.YtError:
                self.log.error("Unhandled exception", exc_info=True)

                if tries > 20:
                    self.log.error("Too many retries. Reraise")
                    raise

                self.log.info("Retry again in %d seconds...", backoff_time)
                time.sleep(backoff_time)
                tries += 1
                backoff_time = min(backoff_time * 2, 600)

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
            except errors.YtError:
                self.log.error("Unhandled exception", exc_info=True)

                if tries > 20:
                    self.log.error("Too many retries. Reraise")
                    raise

                self.log.info("Retry again in %d seconds...", backoff_time)
                time.sleep(backoff_time)
                tries += 1
                backoff_time = min(backoff_time * 2, 600)

        try:
            self.log.debug("Archive table has %d rows", self.yt.get(self._archive_row_count_attr))
        except Exception:
            pass

    def initialize(self):
        with self.yt.Transaction():
            if not self.yt.exists(self._number_of_first_row_attr):
                self.yt.set(self._number_of_first_row_attr, 0)


#==========================================

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

#==========================================

EVENT_LOG_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
LOGBROKER_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

def normalize_timestamp(ts):
    dt = datetime.datetime.strptime(ts, EVENT_LOG_TIMESTAMP_FORMAT)
    microseconds = dt.microsecond
    dt -= datetime.timedelta(microseconds=microseconds)
    return dt.isoformat(' '), microseconds

def revert_timestamp(normalized_ts, microseconds):
    dt = datetime.datetime.strptime(normalized_ts, LOGBROKER_TIMESTAMP_FORMAT)
    dt += datetime.timedelta(microseconds=microseconds)
    return dt.strftime(EVENT_LOG_TIMESTAMP_FORMAT)

#==========================================

def convert_to_tskved_json(row):
    result = {}
    for key, value in row.iteritems():
        if isinstance(value, basestring):
            pass
        else:
            value = json.dumps(value)
        result[key] = value
    return result

def convert_from_tskved_json(converted_row):
    result = dict()
    for key, value in converted_row.iteritems():
        new_value = value
        try:
            if isinstance(new_value, basestring):
                new_value = json.loads(new_value)
        except ValueError:
            pass

        result[key] = new_value
    return result

#==========================================

LOGBROKER_TSKV_PREFIX = "tskv\t"

def convert_to_logbroker_format(row):
    stream = StringIO.StringIO()
    stream.write(LOGBROKER_TSKV_PREFIX)
    row = convert_to_tskved_json(row)
    format.DsvFormat(enable_escaping=True).dump_row(row, stream)
    return stream.getvalue()

def convert_from_logbroker_format(converted_row):
    stream = StringIO.StringIO(converted_row)
    stream.seek(len(LOGBROKER_TSKV_PREFIX))
    return convert_from_tskved_json(format.DsvFormat(enable_escaping=True).load_row(stream))

#==========================================

def serialize_chunk(chunk_id, seqno, lines, data):
    serialized_data = struct.pack(CHUNK_HEADER_FORMAT, chunk_id, seqno, lines)
    serialized_data += gzip_compress("".join([convert_to_logbroker_format(row) for row in data]))
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
        data.append(convert_from_logbroker_format(line))

    return data

def _preprocess(data, **args):
    return [_transform_record(record, **args) for record in data]

def _transform_record(record, cluster_name, log_name):
    try:
        normalized_ts, microseconds = normalize_timestamp(record["timestamp"])
        record.update({
            "timestamp": normalized_ts,
            "microseconds": microseconds,
            "cluster_name": cluster_name,
            "tskv_format": log_name,
            "timezone": "+0000"
        })
    except:
        log.error("Unable to transform record: %r", record)
        raise
    return record

def _untransform_record(record):
    record.pop("cluster_name", None)
    record.pop("tskv_format", None)
    record.pop("timezone", None)
    microseconds = record.pop("microseconds", 0)
    timestamp = record["timestamp"]
    record["timestamp"] = revert_timestamp(timestamp, microseconds)
    return record


class ChunkTooBigError(Exception):
    pass


class LogBroker(object):
    log = logging.getLogger("LogBroker")

    MAX_CHUNK_SIZE = 10 * 1024 * 1024

    def __init__(self, service_id, source_id, io_loop=None, connection_factory=None):
        self._service_id = service_id
        self._source_id = source_id

        self._chunk_id = 0

        self._session = None
        self._push = None

        self._save_chunk_futures = dict()
        self._last_acked_seqno = None
        self._stopped = False
        self._last_message_ts = None

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

        result = gen.Future()

        ts = self._get_timestamp_for(data)

        serialized_data = serialize_chunk(self._chunk_id, seqno, 0, data)
        self._chunk_id += 1
        if len(serialized_data) > self.MAX_CHUNK_SIZE:
            self.log.debug("Unable to save chunk %d with seqno %d. Its size equals to %d > %d",
                self._chunk_id - 1, seqno, len(serialized_data), self.MAX_CHUNK_SIZE)
            result.set_exception(ChunkTooBigError())
            return result

        self.log.debug("Save chunk %d with seqno %d. Timestamp: %s. Its size equals to %d", self._chunk_id - 1, seqno, ts, len(serialized_data))
        self._push.write_chunk(serialized_data)
        self._save_chunk_futures[seqno] = result
        return result

    @gen.coroutine
    def read_session(self):
        with ExceptionLoggingContext(self.log):
            while not self._stopped:
                if (self._save_chunk_futures and
                   self._last_message_ts is not None and
                   time.time() - self._last_message_ts > 30*60):
                    self._abort(RuntimeError("There are no not ping messages for more than 30 minutes"))

                try:
                    message = yield self._session.read_message()
                except GeneratorExit as e:
                    self.log.error("Got GeneratorExit")
                    assert self._stopped
                except (RuntimeError, IOError) as e:
                    self._abort(e)
                else:
                    self.log.debug("Get %r message", message)
                    if message.type == "ping":
                        pass
                    elif message.type == "skip":
                        self._last_message_ts = time.time()
                        skip_seqno = message.attributes["seqno"]
                        f = self._save_chunk_futures.pop(skip_seqno, None)
                        if f:
                            if skip_seqno > self._last_acked_seqno:
                                self._update_last_acked_seqno(skip_seqno)
                            f.set_result(self._last_acked_seqno)
                        else:
                            self.log.error("Get skip message for unknown seqno: %s", skip_seqno)
                    elif message.type == "ack":
                        self._last_message_ts = time.time()
                        assert self._last_acked_seqno <= message.attributes["seqno"]

                        self._update_last_acked_seqno(message.attributes["seqno"])
                        self._set_futures(self._last_acked_seqno)

    def _get_timestamp_for(self, data):
        if data:
            return data[0].get("timestamp")
        else:
            return None

    def _abort(self, e):
        self.log.info("Abort LogBroker client", exc_info=e)
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
        if not self._stopped:
            try:
                assert len(self._save_chunk_futures) == 0
                self.log.info("Stop push stream...")
                if self._push:
                    self._push.stop()
                self.log.info("Stop session stream...")
                if self._session:
                    self._session.stop()
            except:
                self.log.error("Unhandled exception while stopping LogBroker", exc_info=True)
                raise
            finally:
                self._push = None
                self._session = None
                self._stopped = True


class SessionEndError(RuntimeError):
    pass

class BadProtocolError(RuntimeError):
    pass


SessionMessage = collections.namedtuple("SessionMessage", ["type", "attributes"])


class SessionStream(object):
    log = logging.getLogger("SessionStream")

    SESSION_TIMEOUT = datetime.timedelta(minutes=5)

    def __init__(self, service_id, source_id, logtype=None, io_loop=None, connection_factory=None):
        self._id = None
        self._attributes = None
        self._iostream = None

        self._logtype = logtype
        self._service_id = service_id
        self._source_id = source_id

        self._pending_messages = []

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection_factory = connection_factory or tcpclient.TCPClient(io_loop=self._io_loop)

    @gen.coroutine
    def connect(self, endpoint, timeout=None):
        if timeout is None:
            timeout = self.SESSION_TIMEOUT

        while True:
            try:
                self.log.info("Create a session. Endpoint: %s. Service id: %s. Source id: %s", endpoint, self._service_id, self._source_id)

                self._iostream = yield gen.with_timeout(
                    timeout,
                    self._connection_factory.connect(endpoint[0], endpoint[1]),
                    self._io_loop
                    )

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
                metadata_raw = yield gen.with_timeout(
                    timeout,
                    self._iostream.read_until("\r\n\r\n", max_bytes=1024*1024),
                    self._io_loop
                    )

                self.log.debug("Parse response %s", metadata_raw)
                self.parse_metadata(metadata_raw[:-4])

                if not "seqno" in self._attributes:
                    self.log.error("There is no seqno header in session response")
                    raise BadProtocolError("There is no seqno header in session response")
                if not "session" in self._attributes:
                    self.log.error("There is no session header in session response")
                    raise BadProtocolError("There is no session header in session response")

                self._id = self._attributes["session"]

                raise gen.Return(self._id)
            except (IOError, BadProtocolError, gen.TimeoutError):
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
        if self._pending_messages:
            current_message = self._pending_messages.pop()
            raise gen.Return(self._parse(current_message))

        if timeout is None:
            timeout = self.SESSION_TIMEOUT

        try:
            headers_raw = yield gen.with_timeout(
                timeout,
                self._iostream.read_until("\r\n", max_bytes=4*1024),
                self._io_loop
                )
            try:
                body_size = int(headers_raw, 16)
            except ValueError:
                self.log.error("[%s] Bad HTTP chunk header format", self._id)
                raise BadProtocolError()
            if body_size == 0:
                self.log.error("[%s] HTTP response is finished", self._id)
                data = yield gen.with_timeout(
                    timeout,
                    self._iostream.read_until_close(),
                    self._io_loop
                    )
                self.log.debug("[%s] Session trailers: %s", self._id, data)
                raise SessionEndError()
            else:
                data = yield gen.with_timeout(
                    timeout,
                    self._iostream.read_bytes(body_size + 2),
                    self._io_loop
                    )
                self.log.debug("[%s] Process status: '%s'", self._id, data.strip().encode("string_escape"))
                messages = data.strip().split("\n")
                current_message = messages[0]
                if len(messages) > 0:
                    self._pending_messages.extend(messages[1:])

                raise gen.Return(self._parse(current_message))
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
                self.log.error("Unable to parse record %s", record, exc_info=True)
                raise BadProtocolError("Unable to parse record")
            else:
                attributes[key] = value
        return SessionMessage(type_, attributes)


class PushStream(object):
    log = logging.getLogger("PushStream")

    PUSH_TIMEOUT = datetime.timedelta(minutes=5)

    def __init__(self, io_loop=None, connection_factory=None):
        self._iostream = None
        self._session_id = None

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection_factory = connection_factory or tcpclient.TCPClient(io_loop=self._io_loop)

    @gen.coroutine
    def connect(self, endpoint, session_id, timeout=None):
        if timeout is None:
            timeout = self.PUSH_TIMEOUT

        self._session_id = session_id

        self.log.info("Create a push channel for session %s. Endpoint: %s", self._session_id, endpoint)

        self._iostream = yield gen.with_timeout(
            timeout,
            self._connection_factory.connect(endpoint[0], endpoint[1]),
            self._io_loop
            )
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

    def __init__(self, proxy_path, logbroker_url, table_name,
                 service_id, source_id,
                 cluster_name, log_name):
        self._last_acked_seqno = None
        self._chunk_size = 4000
        self._logbroker_url = logbroker_url

        self._service_id = service_id
        self._source_id = source_id

        if self._source_id is None:
            raise RuntimeError("Source id is not set")

        self._cluster_name = cluster_name
        self._log_name = log_name

        self._io_loop = ioloop.IOLoop.instance()

        set_proxy(proxy_path)
        self._event_log = EventLog(client.Yt(proxy_path), table_name=table_name)
        self._log_broker = None

    def start(self):
        self._io_loop.add_callback(self._start)
        self._io_loop.start()

    @gen.coroutine
    def _start(self):
        while True:
            try:
                self._log_broker = LogBroker(self._service_id, self._source_id, io_loop=self._io_loop)

                hostname = _get_logbroker_hostname(logbroker_url=self._logbroker_url)
                self._last_acked_seqno = yield self._log_broker.connect(hostname)

                while True:
                    chunk_size = self._chunk_size
                    saved = False
                    while not saved:
                        try:
                            data = self._event_log.get_data(self._last_acked_seqno, chunk_size)
                            data = _preprocess(data, cluster_name=self._cluster_name, log_name=self._log_name)
                            self._last_acked_seqno = yield self._log_broker.save_chunk(self._last_acked_seqno + self._chunk_size, data)
                        except EventLog.NotEnoughDataError:
                            self.log.info("Not enough data in the event log", exc_info=True)
                            yield sleep_future(30.0, self._io_loop)
                        except ChunkTooBigError:
                            new_chunk_size = max(100, chunk_size / 2)
                            self.log.error("%d table rows forms chunk which is too big. Use small chunk size: %d", chunk_size, new_chunk_size)
                            chunk_size = new_chunk_size
                        else:
                            saved = True
            except Exception:
                self.log.error("Unhandled exception. Try to reconnect...", exc_info=True)
                self._log_broker.stop()
                self._log_broker = None
                yield sleep_future(1, self._io_loop)


class LastSeqnoGetter(object):
    log = logging.getLogger("LastSeqnoGetter")

    def __init__(self, logbroker_url, service_id, source_id,
                 io_loop=None, connection_factory=None):
        self._last_seqno = None
        self._last_seqno_ex = None

        self._logbroker_url = logbroker_url
        self._service_id = service_id
        self._source_id = source_id

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection_factory = connection_factory or tcpclient.TCPClient(io_loop=self._io_loop)

    def get(self):
        self._io_loop.add_callback(self._get_seqno)
        self._io_loop.start()
        if self._last_seqno_ex is not None:
            raise self._last_seqno_ex
        return self._last_seqno

    @gen.coroutine
    def _get_seqno(self):
        try:
            with ExceptionLoggingContext(self.log):
                hostname = _get_logbroker_hostname(self._logbroker_url)
                session = SessionStream(service_id=self._service_id, source_id=self._source_id, io_loop=self._io_loop, connection_factory=self._connection_factory)
                self.log.info("Connect session stream")
                yield session.connect((hostname, 80))
                self._last_seqno = int(session.get_attribute("seqno"))
        except Exception as e:
            self._last_seqno_ex = e
        finally:
            self._io_loop.stop()

def _get_logbroker_hostname(logbroker_url):
    log.info("Getting adviced logbroker endpoint hostname for %s...", logbroker_url)
    response = requests.get("http://{0}/advice".format(logbroker_url), headers={"ClientHost": socket.getfqdn()})
    if not response.ok:
        raise RuntimeError("Unable to get adviced logbroker endpoint hostname")
    host = response.text.strip()

    log.info("Adviced endpoint hostname: %s", host)
    return host


def get_last_seqno(**kwargs):
    getter = LastSeqnoGetter(**kwargs)
    return getter.get()


def main(proxy_path, table_name, logbroker_url,
         service_id, source_id,
         cluster_name, log_name, **kwargs):
    app = Application(proxy_path, logbroker_url, table_name,
                      service_id, source_id,
                      cluster_name, log_name)
    app.start()


def print_last_seqno(logbroker_url, service_id, source_id, **kwargs):
    try:
        last_seqno = get_last_seqno(logbroker_url=logbroker_url, service_id=service_id, source_id=source_id)
        sys.stdout.write("Last seqno: {0}\n".format(last_seqno))
    except Exception:
        log.error("Unhandled exception", exc_info=True)
        sys.stderr.write("Internal error\n")


def monitor(proxy_path, table_name, threshold, logbroker_url, service_id, source_id, **kwargs):
    try:
        last_seqno = get_last_seqno(logbroker_url=logbroker_url, service_id=service_id, source_id=source_id)
        set_proxy(proxy_path)
        event_log = EventLog(client.Yt(proxy_path), table_name=table_name)
        row_count = event_log.get_row_count()
    except Exception:
        log.error("Unhandled exception", exc_info=True)
        sys.stdout.write("2; Internal error\n")
    else:
        lag = row_count - last_seqno
        if lag > threshold:
            sys.stdout.write("2; Lag equals to: %d\n" % (lag,))
        else:
            sys.stdout.write("0; Lag equals to: %d\n" % (lag,))


def init(table_name, proxy_path, **kwargs):
    set_proxy(proxy_path)
    event_log = EventLog(client.Yt(proxy_path), table_name=table_name)
    event_log.initialize()


def archive(table_name, proxy_path, **kwargs):
    set_proxy(proxy_path)
    event_log = EventLog(client.Yt(proxy_path), table_name=table_name)
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
    options.define("log_name", default=DEFAULT_LOG_NAME, help="[logbroker] name of source cluster")
    options.define("logtype", default="", help="[logbroker] log type")
    options.define("logbroker_url", default="", help="[logbroker] url to get adviced kafka endpoint")
    options.define("service_id", default=DEFAULT_SERVICE_ID, help="[logbroker] service id")
    options.define("source_id", help="[logbroker] source id")

    options.define("threshold", default=10**6, help="threshold of lag size to generate error")
    options.define("count", default=10**6, help="row count to archive")

    options.define("init", default=False, help="init and exit")
    options.define("monitor", default=False, help="output status and exit")
    options.define("version", default=False, help="output version and exit")
    options.define("archive", default=False, help="archive and exit")
    options.define("print_last_seqno", default=False, help="print last seqno and exit")

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

    if options.options.init:
        func = init
    elif options.options.monitor:
        func = monitor
    elif options.options.archive:
        func = archive
    elif options.options.print_last_seqno:
        func = print_last_seqno
    else:
        func = main

    try:
        func(**options.options.as_dict())
    except Exception:
        logging.error("Unhandled exception: ", exc_info=True)


if __name__ == "__main__":
    run()
