#!/usr/bin/python

import yt.wrapper.config
from yt.wrapper import errors
from yt.wrapper import client
from yt.wrapper import table
from yt.wrapper.config import set_proxy

try:
    from yt.fennel.version import VERSION
except ImportError:
    VERSION="unknown"

from yt.fennel import misc

import tornado
assert tornado.version_info > (4,)


from tornado import ioloop
from tornado import gen
from tornado import tcpclient
from tornado import options
from tornado import httputil
from tornado import http1connection


try:
    from raven.handlers.logging import SentryHandler
except ImportError:
    pass

import requests

import atexit
import socket
import logging
import datetime
import time
import sys
import collections

yt.wrapper.config.RETRY_READ = True

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
        self._last_saved_ts = "{0}/@last_saved_ts".format(self._table_name)

        self._number_of_first_row_attr = "{0}/@number_of_first_row".format(self._table_name)
        self._row_count_attr = "{0}/@row_count".format(self._table_name)

        self._archive_number_of_first_row_attr = "{0}/@number_of_first_row".format(self._archive_table_name)
        self._archive_row_count_attr = "{0}/@row_count".format(self._archive_table_name)

    def get_row_count(self):
        with self.yt.Transaction():
            first_row = self.yt.get(self._number_of_first_row_attr)
            row_count = self.yt.get(self._row_count_attr)
            return row_count + first_row

    def get_archive_row_count(self):
        with self.yt.Transaction():
            first_row = self.yt.get(self._archive_number_of_first_row_attr)
            row_count = self.yt.get(self._archive_row_count_attr)
            return row_count + first_row

    def update_processed_row_count(self, value):
        try:
            attr_name = "//sys/scheduler/event_log/@processed_row_count"
            row_count = self.yt.get(attr_name)
            self.yt.set(attr_name, row_count + value)
        except errors.YtError:
            self.log.error("Failed to update processed row count. Unhandled exception", exc_info=True)

    def get_data(self, begin, count):
        with self.yt.Transaction():

            # NB: attributes processed_row_count added by ignat to fix fennel.
            # This attribute should never be changed manually and event_log should never be rotated.
            row_count = self.yt.get("//sys/scheduler/event_log/@processed_row_count")
            begin = row_count

            #rows_removed = self.yt.get(self._number_of_first_row_attr)
            #begin -= rows_removed

            result = []
            if begin < 0:
                self.log.error("Table index is less then 0: %d. Use archive table", begin)
                archive_row_count = self.get_archive_row_count()
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

    def archive(self, count):
        self._check_invariant()

        self.log.info("Archive %s rows from event log", count)
        if count == 0:
            self.log.warning("Do not archive 0 rows. Exit")
            return

        assert count > 0

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
        if not self.yt.exists(self._archive_number_of_first_row_attr):
            self.yt.set(self._archive_number_of_first_row_attr, 0)

        self._perform_with_retries(lambda: self._do_archive(partition))

        self._truncate(self._table_name, count)
        self._check_invariant()

    def _do_archive(self, partition):
        desired_chunk_size = 2 * 1024 ** 3
        approximate_gzip_compression_ratio = 0.137
        data_size_per_job = max(1, int(desired_chunk_size / approximate_gzip_compression_ratio))

        self.log.info("Run merge; source table: %s", partition.to_yson_type())
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


    def truncate_archive(self, count):
        self._truncate(self._archive_table_name, count)

    def _truncate(self, table_name, count):
        row_count = self.yt.get("{0}/@row_count".format(table_name))
        if row_count < count:
            raise RuntimeError("Unable to truncate %d rows from %s: there is only %d rows" %(count, table_name, row_count))

        self._perform_with_retries(lambda: self._do_truncate(table_name, count))

    def _do_truncate(self, table_name, count):
        partition = table.TablePath(
            table_name,
            start_index=0,
            end_index=count)

        self.log.info("Truncate %d rows from %s...", count, table_name)
        first_row = self.yt.get("{0}/@number_of_first_row".format(table_name))
        first_row += count
        self.yt.run_erase(partition)
        self.yt.set("{0}/@number_of_first_row".format(table_name), first_row)

    def _perform_with_retries(self, func):
        tries = 0
        finished = False
        backoff_time = 5
        while not finished:
            try:
                with self.yt.Transaction():
                    func()
                finished = True
            except errors.YtError:
                self.log.error("Failed to perform %s. Unhandled exception", func.__name__, exc_info=True)

                if tries > 20:
                    self.log.error("Too many retries. Reraise")
                    raise

                self.log.info("Retry again in %d seconds...", backoff_time)
                time.sleep(backoff_time)
                tries += 1
                backoff_time = min(backoff_time * 2, 600)


    def update_last_saved_ts(self, value):
        try:
            self.yt.set(self._last_saved_ts, value)
        except errors.YtError:
            self.log.error("Failed to update last saved ts. Unhandled exception", exc_info=True)

    def get_last_saved_ts(self):
        serialized_ts = self.yt.get(self._last_saved_ts)
        dt = datetime.datetime.strptime(serialized_ts, misc.LOGBROKER_TIMESTAMP_FORMAT)
        return dt

    def _check_invariant(self):
        try:
            archive_row_count = self.get_archive_row_count()
            self.log.debug("Archive table has %d rows", archive_row_count)
        except Exception:
            self.log.error("Unhandled exception while getting archive row count", exc_info=True)
            archive_row_count = 0

        try:
            number_of_first_row = self.yt.get(self._number_of_first_row_attr)
            self.log.debug("Number of first row: %d", number_of_first_row)
        except Exception:
            self.log.error("Unhandled exception while getting number of first row", exc_info=True)

        assert number_of_first_row == archive_row_count, "%d != %d" % (number_of_first_row, archive_row_count)

    def initialize(self):
        with self.yt.Transaction():
            if not self.yt.exists(self._number_of_first_row_attr):
                self.yt.set(self._number_of_first_row_attr, 0)
            if not self.yt.exists(self._archive_number_of_first_row_attr):
                self.yt.set(self._archive_number_of_first_row_attr, 0)


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
        self._last_session_message_ts = None
        self._last_chunk_ts = None

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection_factory = connection_factory or tcpclient.TCPClient(io_loop=self._io_loop)

    @gen.coroutine
    def connect(self, hostname, timeout=None):
        assert self._session is None
        assert self._push is None

        self._session = SessionReader(service_id=self._service_id, source_id=self._source_id, io_loop=self._io_loop, connection_factory=self._connection_factory)
        self.log.info("Connect session stream")
        yield self._session.connect((hostname, 80))
        session_id = yield self._session.get_id()
        seqno = yield self._session.get_seqno()
        self._update_last_acked_seqno(seqno)

        self._push = PushStream(io_loop=self._io_loop, connection_factory=self._connection_factory)
        self.log.info("Connect push stream")
        yield self._push.connect((hostname, 9000), session_id=session_id)

        self._io_loop.add_callback(self.read_session)
        raise gen.Return(self._last_acked_seqno)

    def save_chunk(self, seqno, data, timeout=None):
        assert not seqno in self._save_chunk_futures

        result = gen.Future()
        if self._stopped:
            result.set_exception(RuntimeError("Failed to save chunk: logbroker client is stopped"))
            return result

        self._last_chunk_ts = self._get_timestamp_for(data)

        serialized_data = misc.serialize_chunk(self._chunk_id, seqno, 0, data)
        self._chunk_id += 1
        if len(serialized_data) > self.MAX_CHUNK_SIZE:
            self.log.debug("Unable to save chunk %d with seqno %d. Its size equals to %d > %d",
                self._chunk_id - 1, seqno, len(serialized_data), self.MAX_CHUNK_SIZE)
            result.set_exception(ChunkTooBigError())
            return result

        self.log.debug("Save chunk %d with seqno %d. Timestamp: %s. Its size equals to %d", self._chunk_id - 1, seqno, self._last_chunk_ts, len(serialized_data))
        self._push.write_chunk(serialized_data)
        self._save_chunk_futures[seqno] = result
        return result

    def get_chunk_data_ts(self):
        return self._last_chunk_ts

    @gen.coroutine
    def read_session(self):
        try:
            while not self._stopped:
                if (self._save_chunk_futures and
                    self._last_session_message_ts is not None and
                    time.time() - self._last_session_message_ts > 30*60):
                    self._abort(RuntimeError("There are no not ping messages for more than 30 minutes"))
                    return

                try:
                    message = yield self._session.read_message()
                except GeneratorExit as e:
                    self.log.error("Got GeneratorExit")
                    assert self._stopped
                except (RuntimeError, IOError, gen.TimeoutError) as e:
                    self._abort(e)
                else:
                    self.log.debug("Get %r message", message)
                    if message.type == "ping":
                        pass
                    elif message.type == "skip":
                        self._last_session_message_ts = time.time()
                        skip_seqno = message.attributes["seqno"]
                        f = self._save_chunk_futures.pop(skip_seqno, None)
                        if f:
                            if skip_seqno > self._last_acked_seqno:
                                self._update_last_acked_seqno(skip_seqno)
                            f.set_result(self._last_acked_seqno)
                        else:
                            self.log.error("Get skip message for unknown seqno: %s", skip_seqno)
                    elif message.type == "ack":
                        self._last_session_message_ts = time.time()
                        assert self._last_acked_seqno <= message.attributes["seqno"]

                        self._update_last_acked_seqno(message.attributes["seqno"])
                        self._set_futures(self._last_acked_seqno)
        except Exception as e:
            self.log.error("Failed to read session. Unhandled exception: ", exc_info=True)
            self._abort(e)

    def _get_timestamp_for(self, data):
        if data:
            return data[-1].get("timestamp")
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
                self.log.error("Failed to stop LogBroker. Unhandled exception", exc_info=True)
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


class SessionReader(object):
    log = logging.getLogger("SessionReader")

    SESSION_TIMEOUT = datetime.timedelta(minutes=5)
    HEADERS_TIMEOUT = datetime.timedelta(minutes=1)

    def __init__(self, service_id, source_id, logtype=None, io_loop=None, connection_factory=None):
        self._logtype = logtype
        self._service_id = service_id
        self._source_id = source_id

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._connection_factory = connection_factory or tcpclient.TCPClient(io_loop=self._io_loop)

        self._iostream = None
        self._connection = None
        self._error = None

        self._id = None
        self._seqno = None

        self._ready = None
        self._messages = []

    @gen.coroutine
    def connect(self, endpoint, timeout=None):
        if timeout is None:
            timeout = self.SESSION_TIMEOUT

        self.log.info("Start connection to %s...", endpoint)
        self._iostream = yield gen.with_timeout(
            timeout,
            self._connection_factory.connect(endpoint[0], endpoint[1]),
            self._io_loop
            )
        self.log.info("Connection to %s is established", endpoint)
        self._connection = http1connection.HTTP1Connection(
            self._iostream,
            is_client=True,
            params=http1connection.HTTP1ConnectionParameters(
                no_keep_alive=True,
                header_timeout=min(timeout.total_seconds(), self.HEADERS_TIMEOUT.total_seconds()),
                max_header_size=64*1024
                ))
        future = self._connection.write_headers(
            httputil.RequestStartLine(
                "GET",
                httputil.url_concat(
                    "/rt/session", dict(
                        ident=self._service_id,
                        sourceid=self._source_id,
                        logtype=self._logtype)),
                "HTTP/1.1"),
            httputil.HTTPHeaders({
                "Host" : endpoint[0],
                "Accept" : "*/*"
                })
            )
        self._connection.finish()
        yield gen.with_timeout(
            timeout,
            future,
            self._io_loop)
        self.log.info("Start reading response...")
        read_response_future = self._connection.read_response(self)

        self._io_loop.add_future(
            read_response_future,
            self._finish)

    def _finish(self, future):
        self.log.info("Session reader is finished. Id: %s", self._id)

    def stop(self, e=None):
        if e:
            self.log.error("Stopping...", exc_info=e)
        else:
            self.log.info("Stopping...")
        self._error = e
        self._set_ready()
        if self._iostream:
            self._iostream.close(e)

    def read_message(self):
        return self._return_when_ready(
            lambda: self._messages,
            lambda: self._messages.pop())

    def get_seqno(self):
        return self._return_when_ready(
            lambda: self._seqno is not None,
            lambda: self._seqno)

    def get_id(self):
        return self._return_when_ready(
            lambda: self._id is not None,
            lambda: self._id)

    @gen.coroutine
    def _return_when_ready(self, is_ready, getter):
        while True:
            if self._error:
                raise self._error

            if is_ready():
                raise gen.Return(getter())

            if self._ready is None:
                self._ready = gen.Future()
            yield self._ready

    def headers_received(self, start_line, headers):
        try:
            self.log.debug("Headers received. Start line: %s. Headers: %r", start_line, headers)
            if start_line.code != 200:
                raise RuntimeError("Bad response. Reason: %s", start_line.reason)

            self._seqno = int(self._extract_header(headers, "seqno"))
            self._id = self._extract_header(headers, "session")
            self._set_ready()
        except Exception as e:
            self.stop(e)

    def _extract_header(self, headers, name):
        value = headers.get_list(name)
        if len(value) == 0:
            raise BadProtocolError("There is no %s header in session response", name)
        if len(value) > 1:
            raise BadProtocolError("There is more than one %s header in session response", name)
        return value[0]

    def data_received(self, chunk):
        try:
            self.log.debug("Data received %r", chunk)
            for line in chunk.strip().split("\n"):
                self._messages.append(self._parse(line))
                self._set_ready()
        except Exception as e:
            self.stop(e)

    def _set_ready(self):
        if self._ready:
            self._ready.set_result(True)
            self._ready = None

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

    def finish(self):
        self.stop(SessionEndError())

    def on_connection_close(self):
        self.stop(SessionEndError())


class PushStream(object):
    log = logging.getLogger("PushStream")

    PUSH_TIMEOUT = datetime.timedelta(minutes=5)
    HEADERS_TIMEOUT = datetime.timedelta(minutes=1)

    def __init__(self, io_loop=None, connection_factory=None):
        self._iostream = None
        self._connection = None
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
        self._connection = http1connection.HTTP1Connection(
            self._iostream,
            is_client=True,
            params=http1connection.HTTP1ConnectionParameters(
                no_keep_alive=True,
                header_timeout=min(timeout.total_seconds(), self.HEADERS_TIMEOUT.total_seconds()),
                max_header_size=64*1024
                ))

        future = self._connection.write_headers(
            httputil.RequestStartLine("PUT", "/rt/store", "HTTP/1.1"),
            httputil.HTTPHeaders({
                "Host": endpoint[0],
                "Content-Type": "text/plain",
                "Content-Encoding": "gzip",
                "RTSTreamFormat": "v2le",
                "Session": self._session_id
                })
            )
        yield gen.with_timeout(
            timeout,
            future,
            self._io_loop)

        self.log.info("The push stream for session %s has been created", self._session_id)

        self._iostream.read_until_close(callback=self._post_close, streaming_callback=self._dump_output)
        raise gen.Return()

    def stop(self):
        if self._iostream is not None:
            self._iostream.close()
            self._iostream = None

    def write_chunk(self, serialized_data):
        return self._connection.write(serialized_data)

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
                 cluster_name, log_name,
                 chunk_size):
        self._last_acked_seqno = None
        self._chunk_size = chunk_size
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
        try:
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
                                row_count = len(data)
                                assert row_count == chunk_size
                                data = misc._preprocess(data, cluster_name=self._cluster_name, log_name=self._log_name)
                                self._last_acked_seqno = yield self._log_broker.save_chunk(self._last_acked_seqno + self._chunk_size, data)

                                self._event_log.update_last_saved_ts(self._log_broker.get_chunk_data_ts())
                                self._event_log.update_processed_row_count(row_count)
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
        except:
            self.log.exception("Unhandled exception. Something goes wrong.")
            self._io_loop.stop()


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
                session = SessionReader(service_id=self._service_id, source_id=self._source_id, io_loop=self._io_loop, connection_factory=self._connection_factory)
                self.log.info("Connect session stream")
                yield session.connect((hostname, 80))
                self._last_seqno = yield session.get_seqno()
        except Exception as e:
            self._last_seqno_ex = e
        finally:
            self._io_loop.stop()

def _get_logbroker_hostname(logbroker_url):
    log.info("Getting adviced logbroker endpoint hostname for %s...", logbroker_url)
    response = requests.get("http://{0}/advice".format(logbroker_url), headers={"ClientHost": socket.getfqdn(), "Accept-Encoding": "identity"})
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
         cluster_name, log_name,
         chunk_size, **kwargs):
    app = Application(proxy_path, logbroker_url, table_name,
                      service_id, source_id,
                      cluster_name, log_name,
                      chunk_size)
    app.start()


def print_last_seqno(logbroker_url, service_id, source_id, **kwargs):
    try:
        last_seqno = get_last_seqno(logbroker_url=logbroker_url, service_id=service_id, source_id=source_id)
        sys.stdout.write("Last seqno: {0}\n".format(last_seqno))
    except Exception:
        log.error("Failed to get last seqno. Unhandled exception", exc_info=True)
        sys.stderr.write("Internal error\n")


def monitor(proxy_path, table_name, threshold, **kwargs):
    try:
        set_proxy(proxy_path)
        event_log = EventLog(client.Yt(proxy_path), table_name=table_name)
        lag = datetime.datetime.utcnow() - event_log.get_last_saved_ts()
    except Exception:
        log.error("Failed to run monitoring. Unhandled exception", exc_info=True)
        sys.stdout.write("2; Internal error\n")
    else:
        if lag.total_seconds() > threshold * 3600:
            sys.stdout.write("2; Lag equals to: %s\n" % (lag,))
        else:
            sys.stdout.write("0; Lag equals to: %s\n" % (lag,))


def init(table_name, proxy_path, **kwargs):
    set_proxy(proxy_path)
    event_log = EventLog(client.Yt(proxy_path), table_name=table_name)
    event_log.initialize()


def archive(table_name, proxy_path, logbroker_url, service_id, source_id, **kwargs):
    set_proxy(proxy_path)
    last_seqno = get_last_seqno(logbroker_url=logbroker_url, service_id=service_id, source_id=source_id)
    event_log = EventLog(client.Yt(proxy_path), table_name=table_name)
    archive_row_count = event_log.get_archive_row_count()

    max_count = last_seqno - archive_row_count

    count = int(kwargs.get("count", max_count))
    if count > max_count:
        count = max_count

    event_log.archive(count)


def truncate(table_name, proxy_path, count, **kwargs):
    set_proxy(proxy_path)
    event_log = EventLog(client.Yt(proxy_path), table_name=table_name)
    event_log.truncate_archive(count)


def run():
    options.define("table_name",
        metavar="PATH",
        default="//sys/scheduler/event_log",
        help="[yt] path to scheduler event log")
    options.define("proxy_path", metavar="URL", help="[yt] url to proxy")
    options.define("chunk_size", default=4000, help="size of chunk in rows")

    options.define("cluster_name", default="", help="[logbroker] name of source cluster")
    options.define("log_name", default="yt-scheduler-log", help="[logbroker] name of source cluster")
    options.define("logtype", default="", help="[logbroker] log type")
    options.define("logbroker_url", default="", help="[logbroker] url to get adviced kafka endpoint")
    options.define("service_id", default="yt", help="[logbroker] service id")
    options.define("source_id", help="[logbroker] source id")

    options.define("threshold", default=48, help="threshold of lag size in hours to generate error")
    options.define("count", default=10**6, help="row count to archive")

    options.define("init", default=False, help="init and exit")
    options.define("monitor", default=False, help="output status and exit")
    options.define("version", default=False, help="output version and exit")
    options.define("archive", default=False, help="archive and exit")
    options.define("print_last_seqno", default=False, help="print last seqno and exit")
    options.define("truncate", default=False, help="truncate archive table and exit")

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
    elif options.options.truncate:
        func = truncate
    else:
        func = main

    try:
        func(**options.options.as_dict())
    except Exception:
        logging.error("Unhandled exception: ", exc_info=True)


if __name__ == "__main__":
    run()
